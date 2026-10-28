package io.oxmq.mockserver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.oxmq.mockserver.model.TenantProfile;
import io.oxmq.mockserver.model.TenantTier;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Ultra-Realistic Multi-Tenant Microsoft Office 365 Mock Server.
 * Supports all 5 official Microsoft 365 Paid License Tiers:
 * - tenant-smb-250: Small Business (250 seats, 25 RUs/s burst)
 * - tenant-mid-2500: Mid-Market Growth (2,500 seats, 50 RUs/s burst)
 * - tenant-ent-10k: Enterprise E3/E5 (10,000 seats, 90 RUs/s burst)
 * - tenant-large-35k: Large Enterprise (35,000 seats, 150 RUs/s burst)
 * - tenant-global-100k: Hyperscale Global (100,000 seats, 250 RUs/s burst)
 *
 * Implements:
 * - Real Resource Unit (RU) cost metering per Microsoft Learn guidance
 * - IETF standard RateLimit-* response headers
 * - Strict per-tenant token bucket isolation
 * - Direct Azure Blob SAS download simulation (0 RUs)
 * - Real physical files on disk (clean documents, archives, spreadsheets, PDFs)
 */
@Component
@ConditionalOnProperty(name = "oxdoc.mock-server.enabled", havingValue = "true", matchIfMissing = true)
public class MockOffice365Server {

    private static final Logger log = LoggerFactory.getLogger(MockOffice365Server.class);

    private final int port;
    private final String storageDirPath;
    private final boolean simulate429Burst;
    private final int totalFilesToSeed;
    private final String scaleMode;
    private final int smbFiles;
    private final int midFiles;
    private final int entFiles;
    private final int largeFiles;
    private final int globalFiles;
    private final String webhookUrl;
    private final String webhookClientState;
    private final ObjectMapper objectMapper;

    // Per-Tenant Drive Folders
    public static final Map<String, List<String>> DRIVE_FOLDERS = new LinkedHashMap<>();

    // Multi-tenant Drive Inventory: Drive Metadata
    public record DriveMetadata(
            String driveId,
            String tenantId,
            String name,
            String driveType, // "documentLibrary" or "business"
            String ownerPrincipal,
            String siteOrUserName
    ) {}

    // File Metadata
    public record MockFileInfo(
            String fileId,
            String driveId,
            String tenantId,
            String name,
            String path,
            long size,
            String mimeType,
            String exposureScope, // "anonymous", "users", "organization", "private"
            String diskFileName,
            long lastModified
    ) {}

    public enum ChaosDecision {
        PROCEED,
        THROTTLE_429,
        SERVICE_UNAVAILABLE_503,
        GATEWAY_TIMEOUT_504,
        INTERNAL_ERROR_500
    }

    // Per-Tenant Rate Limiter and Token Bucket State with Cloud Chaos Engine
    public static class TenantRateLimiter {
        private final String tenantId;
        private final AtomicInteger burstLimitRUs;
        private final AtomicInteger currentSecondRUs = new AtomicInteger(0);
        private final AtomicLong currentSecondEpoch = new AtomicLong(System.currentTimeMillis() / 1000L);
        private final AtomicInteger burst429Remaining = new AtomicInteger(0);
        private final AtomicBoolean burst429Active = new AtomicBoolean(false);
        private final AtomicInteger transient503Remaining = new AtomicInteger(0);
        private final AtomicInteger transient504Remaining = new AtomicInteger(0);
        private final AtomicInteger transient500Remaining = new AtomicInteger(0);
        private final AtomicInteger customRetryAfterSeconds = new AtomicInteger(3);
        private volatile double randomChaosProbability = 0.0;

        public TenantRateLimiter(String tenantId, int defaultBurst) {
            this.tenantId = tenantId;
            this.burstLimitRUs = new AtomicInteger(defaultBurst);
        }

        public ChaosDecision evaluate(int ruCost, Map<String, String> headersToSet) {
            int retrySec = customRetryAfterSeconds.get();
            headersToSet.put("x-ms-resource-unit", String.valueOf(ruCost));

            // 1. Injected 503 Service Unavailable
            if (transient503Remaining.get() > 0) {
                transient503Remaining.decrementAndGet();
                headersToSet.put("Retry-After", String.valueOf(Math.max(2, retrySec)));
                headersToSet.put("x-ms-throttle-limit-percentage", "1.00");
                return ChaosDecision.SERVICE_UNAVAILABLE_503;
            }

            // 2. Injected 504 Gateway Timeout
            if (transient504Remaining.get() > 0) {
                transient504Remaining.decrementAndGet();
                return ChaosDecision.GATEWAY_TIMEOUT_504;
            }

            // 3. Injected 500 Internal Server Error
            if (transient500Remaining.get() > 0) {
                transient500Remaining.decrementAndGet();
                return ChaosDecision.INTERNAL_ERROR_500;
            }

            // 4. Random Transient Chaos (if configured)
            if (randomChaosProbability > 0 && java.util.concurrent.ThreadLocalRandom.current().nextDouble() < randomChaosProbability) {
                double roll = java.util.concurrent.ThreadLocalRandom.current().nextDouble();
                if (roll < 0.4) {
                    headersToSet.put("Retry-After", String.valueOf(retrySec));
                    headersToSet.put("x-ms-throttle-limit-percentage", "1.25");
                    return ChaosDecision.THROTTLE_429;
                } else if (roll < 0.7) {
                    headersToSet.put("Retry-After", String.valueOf(retrySec + 2));
                    headersToSet.put("x-ms-throttle-limit-percentage", "1.00");
                    return ChaosDecision.SERVICE_UNAVAILABLE_503;
                } else if (roll < 0.9) {
                    return ChaosDecision.GATEWAY_TIMEOUT_504;
                } else {
                    return ChaosDecision.INTERNAL_ERROR_500;
                }
            }

            // 5. Injected 429 Throttle Burst
            if (burst429Remaining.get() > 0) {
                burst429Remaining.decrementAndGet();
                headersToSet.put("Retry-After", String.valueOf(retrySec));
                headersToSet.put("RateLimit-Limit", String.valueOf(burstLimitRUs.get()));
                headersToSet.put("RateLimit-Remaining", "0");
                headersToSet.put("RateLimit-Reset", String.valueOf(retrySec));
                headersToSet.put("x-ms-throttle-limit-percentage", "1.25");
                return ChaosDecision.THROTTLE_429;
            }
            if (burst429Active.get()) {
                headersToSet.put("Retry-After", String.valueOf(retrySec));
                headersToSet.put("RateLimit-Limit", String.valueOf(burstLimitRUs.get()));
                headersToSet.put("RateLimit-Remaining", "0");
                headersToSet.put("RateLimit-Reset", String.valueOf(retrySec));
                headersToSet.put("x-ms-throttle-limit-percentage", "1.25");
                return ChaosDecision.THROTTLE_429;
            }

            // 6. Token Bucket Rate Limiting
            long nowSec = System.currentTimeMillis() / 1000L;
            long currentSec = currentSecondEpoch.get();
            if (nowSec > currentSec) {
                if (currentSecondEpoch.compareAndSet(currentSec, nowSec)) {
                    currentSecondRUs.set(0);
                }
            }

            int current = currentSecondRUs.addAndGet(ruCost);
            int limit = burstLimitRUs.get();
            int remaining = Math.max(0, limit - current);

            headersToSet.put("RateLimit-Limit", String.valueOf(limit));
            headersToSet.put("RateLimit-Remaining", String.valueOf(remaining));
            headersToSet.put("RateLimit-Reset", "1");

            double percentage = limit > 0 ? (double) current / (double) limit : 0.0;
            if (percentage >= 0.80) {
                headersToSet.put("x-ms-throttle-limit-percentage", String.format(java.util.Locale.ROOT, "%.2f", percentage));
            }

            if (current > limit) {
                headersToSet.put("Retry-After", String.valueOf(retrySec));
                headersToSet.put("RateLimit-Remaining", "0");
                headersToSet.put("x-ms-throttle-limit-percentage", String.format(java.util.Locale.ROOT, "%.2f", Math.max(1.0, percentage)));
                return ChaosDecision.THROTTLE_429;
            }

            return ChaosDecision.PROCEED;
        }

        public void triggerBurst(int count) {
            this.burst429Remaining.set(count);
        }

        public void trigger503(int count) {
            this.transient503Remaining.set(count);
        }

        public void trigger504(int count) {
            this.transient504Remaining.set(count);
        }

        public void trigger500(int count) {
            this.transient500Remaining.set(count);
        }

        public void setCustomRetryAfter(int seconds) {
            this.customRetryAfterSeconds.set(Math.max(1, seconds));
        }

        public void setRandomChaosProbability(double probability) {
            this.randomChaosProbability = Math.max(0.0, Math.min(1.0, probability));
        }

        public void setBurstActive(boolean active) {
            this.burst429Active.set(active);
        }

        public void setLimit(int limit) {
            this.burstLimitRUs.set(limit);
        }

        public int getLimit() {
            return this.burstLimitRUs.get();
        }

        public int getCurrentRUs() {
            return this.currentSecondRUs.get();
        }
    }

    // Per-Drive / Document Library Rate Limiter (~1,200 req/min = 20 req/s baseline per M365 spec)
    public static class DriveRateLimiter {
        private final String driveId;
        private final int limitPerSec;
        private final AtomicInteger currentSecondReqs = new AtomicInteger(0);
        private final AtomicLong currentSecondEpoch = new AtomicLong(System.currentTimeMillis() / 1000L);

        public DriveRateLimiter(String driveId, int limitPerSec) {
            this.driveId = driveId;
            this.limitPerSec = limitPerSec;
        }

        public boolean tryAcquire(Map<String, String> headersToSet) {
            long nowSec = System.currentTimeMillis() / 1000L;
            long currentSec = currentSecondEpoch.get();
            if (nowSec > currentSec) {
                if (currentSecondEpoch.compareAndSet(currentSec, nowSec)) {
                    currentSecondReqs.set(0);
                }
            }
            int current = currentSecondReqs.incrementAndGet();
            if (current > limitPerSec) {
                headersToSet.put("Retry-After", "2");
                headersToSet.put("RateLimit-Remaining", "0");
                headersToSet.put("x-ms-throttle-limit-percentage", "1.15");
                return false;
            }
            return true;
        }

        public void reset() {
            currentSecondReqs.set(0);
        }
    }

    private HttpServer httpServer;
    private java.util.concurrent.ExecutorService httpExecutor;
    private final Map<String, MockFileInfo> filesInventory = new ConcurrentHashMap<>();
    private final Map<String, List<String>> driveFiles = new ConcurrentHashMap<>();
    private final Map<String, DriveMetadata> drivesMetadata = new ConcurrentHashMap<>();
    private final Map<String, TenantRateLimiter> tenantLimiters = new ConcurrentHashMap<>();
    private final Map<String, DriveRateLimiter> driveLimiters = new ConcurrentHashMap<>();
    private final int driveBurstLimit;

    @Autowired
    public MockOffice365Server(
            @Value("${oxdoc.mock-server.port:8081}") int port,
            @Value("${oxdoc.mock-server.storage-dir:./data/m365-storage}") String storageDirPath,
            @Value("${oxdoc.mock-server.simulate-429-burst:false}") boolean simulate429Burst,
            @Value("${oxdoc.mock-server.total-files:231000}") int totalFilesToSeed,
            @Value("${oxdoc.mock-server.scale-mode:ENTERPRISE_TIERED}") String scaleMode,
            @Value("${oxdoc.mock-server.smb-files:1000}") int smbFiles,
            @Value("${oxdoc.mock-server.mid-files:5000}") int midFiles,
            @Value("${oxdoc.mock-server.ent-files:25000}") int entFiles,
            @Value("${oxdoc.mock-server.large-files:100000}") int largeFiles,
            @Value("${oxdoc.mock-server.global-files:100000}") int globalFiles,
            @Value("${oxdoc.mock-server.drive-burst-limit:100}") int driveBurstLimit,
            @Value("${oxdoc.webhook.notification-url:http://localhost:8080/api/v1/webhooks/graph}") String webhookUrl,
            @Value("${oxdoc.webhook.client-state:oxdoc-secret-state-token}") String webhookClientState,
            ObjectMapper objectMapper
    ) {
        this.port = port;
        this.storageDirPath = storageDirPath;
        this.simulate429Burst = simulate429Burst;
        this.totalFilesToSeed = totalFilesToSeed;
        this.scaleMode = scaleMode;
        this.smbFiles = smbFiles;
        this.midFiles = midFiles;
        this.entFiles = entFiles;
        this.largeFiles = largeFiles;
        this.globalFiles = globalFiles;
        this.driveBurstLimit = driveBurstLimit;
        this.webhookUrl = webhookUrl;
        this.webhookClientState = webhookClientState;
        this.objectMapper = objectMapper;

        // Initialize tenant rate limiters matching official license tiers
        for (TenantProfile profile : TenantProfile.ALL_PROFILES) {
            tenantLimiters.put(profile.tenantId(), new TenantRateLimiter(profile.tenantId(), profile.burstLimitRUsPerSec()));
        }
    }

    public MockOffice365Server(
            int port,
            String storageDirPath,
            boolean simulate429Burst,
            int totalFilesToSeed,
            String webhookUrl,
            String webhookClientState,
            ObjectMapper objectMapper
    ) {
        this(port, storageDirPath, simulate429Burst, totalFilesToSeed, "COMPACT", 200, 1000, 2000, 2000, 2000, 100, webhookUrl, webhookClientState, objectMapper);
    }

    public MockOffice365Server(
            int port,
            String storageDirPath,
            boolean simulate429Burst,
            String webhookUrl,
            String webhookClientState,
            ObjectMapper objectMapper
    ) {
        this(port, storageDirPath, simulate429Burst, 1000, "COMPACT", 200, 200, 200, 200, 200, 100, webhookUrl, webhookClientState, objectMapper);
    }

    @PostConstruct
    public void start() throws IOException {
        Path storageDir = Paths.get(storageDirPath);
        Files.createDirectories(storageDir);

        initMultiTenantCatalog();
        seedBenchmarkInitialFiles(storageDir);
        seedPhysicalFilesAcrossTenants(storageDir);

        httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        this.httpExecutor = Executors.newCachedThreadPool();
        httpServer.setExecutor(this.httpExecutor);

        // OAuth2 Token Endpoints (per-tenant and generic)
        httpServer.createContext("/contoso-enterprise-tenant/oauth2/v2.0/token", this::handleOAuthToken);
        httpServer.createContext("/tenant-smb-250/oauth2/v2.0/token", this::handleOAuthToken);
        httpServer.createContext("/tenant-mid-2500/oauth2/v2.0/token", this::handleOAuthToken);
        httpServer.createContext("/tenant-ent-10k/oauth2/v2.0/token", this::handleOAuthToken);
        httpServer.createContext("/tenant-large-35k/oauth2/v2.0/token", this::handleOAuthToken);
        httpServer.createContext("/tenant-global-100k/oauth2/v2.0/token", this::handleOAuthToken);
        httpServer.createContext("/oauth2/v2.0/token", this::handleOAuthToken);

        // Microsoft Graph v1.0 Endpoints
        httpServer.createContext("/v1.0/subscribedSkus", this::handleSubscribedSkus);
        httpServer.createContext("/v1.0/organization", this::handleOrganization);
        httpServer.createContext("/v1.0/sites/getAllSites", this::handleGetSites);
        httpServer.createContext("/v1.0/sites", this::handleSiteDrives);
        httpServer.createContext("/v1.0/users", this::handleUsersAndDrives);
        httpServer.createContext("/v1.0/drives", this::handleDrives);

        // Binary File Download Endpoint (Direct Azure Blob SAS Storage Simulation - 0 RUs)
        httpServer.createContext("/mock-storage/download", this::handleFileDownload);

        // Mock Server Admin APIs
        httpServer.createContext("/mock-admin/upload-forward-file", this::handleAdminUpload);
        httpServer.createContext("/mock-admin/simulate-429", this::handleAdminSimulate429);
        httpServer.createContext("/mock-admin/set-rate-limit", this::handleAdminSetRateLimit);
        httpServer.createContext("/mock-admin/seed", this::handleAdminSeed);
        httpServer.createContext("/mock-admin/stats", this::handleAdminStats);
        httpServer.createContext("/mock-admin/chaos", this::handleAdminChaos);

        httpServer.start();
        log.info("==========================================================================================");
        log.info("  🌐 ULTRA-REALISTIC MULTI-TENANT M365 SERVER STARTED on http://localhost:{}", port);
        log.info("  📁 Real Physical File Storage: {}", storageDir.toAbsolutePath());
        log.info("  🏢 5 Enterprise Tenants Active: Acme (SMB), Contoso (Mid), Fabrikam (Ent), Northwind (Large), MegaCorp (Global)");
        log.info("  📊 Total Cataloged: {} drives, {} real files on disk", drivesMetadata.size(), filesInventory.size());
        log.info("==========================================================================================");
    }

    @PreDestroy
    public void stop() {
        if (httpServer != null) {
            httpServer.stop(0);
            log.info("Mock Office 365 Server stopped.");
        }
        if (httpExecutor != null) {
            httpExecutor.shutdownNow();
        }
    }

    /**
     * Set up 5 drives for each of the 5 official tenants (25 drives total).
     */
    private void initMultiTenantCatalog() {
        // --- 1. Acme TinyCorp (SMB: 250 seats, 25 RUs/s) ---
        registerDrive("drive-smb-sp-finance", "tenant-smb-250", "Acme Finance & Invoices", "documentLibrary", "founder@acme.com", "Acme Finance");
        DRIVE_FOLDERS.put("drive-smb-sp-finance", List.of("/Invoices/2026", "/Budgets/Q1", "/Tax/Filings", "/Ledgers", "/Receipts"));

        registerDrive("drive-smb-sp-operations", "tenant-smb-250", "Acme Operations & Warehouse", "documentLibrary", "ops@acme.com", "Acme Operations");
        DRIVE_FOLDERS.put("drive-smb-sp-operations", List.of("/Inventory/Stock", "/Suppliers", "/Logistics", "/Facility", "/Policies"));

        registerDrive("drive-smb-od-founder", "tenant-smb-250", "Alice Founder's Personal OneDrive", "business", "alice.founder@acme.com", "Alice Founder");
        DRIVE_FOLDERS.put("drive-smb-od-founder", List.of("/Pitch Decks", "/Cap Table", "/Executive Notes", "/Banking", "/Contracts"));

        registerDrive("drive-smb-od-sales", "tenant-smb-250", "Bob Sales's Personal OneDrive", "business", "bob.sales@acme.com", "Bob Sales");
        DRIVE_FOLDERS.put("drive-smb-od-sales", List.of("/Leads/2026", "/Proposals", "/Pricing Sheets", "/Customer Notes", "/NDAs"));

        registerDrive("drive-smb-od-eng", "tenant-smb-250", "Carol Engineer's Personal OneDrive", "business", "carol.eng@acme.com", "Carol Engineer");
        DRIVE_FOLDERS.put("drive-smb-od-eng", List.of("/Architecture", "/Designs", "/Source Archive", "/Credentials", "/Prototypes"));

        // --- 2. Contoso Logistics (Mid-Market: 2,500 seats, 50 RUs/s) ---
        registerDrive("drive-mid-sp-logistics", "tenant-mid-2500", "Fleet & Logistics Vault", "documentLibrary", "admin@contoso.com", "Fleet Logistics");
        DRIVE_FOLDERS.put("drive-mid-sp-logistics", List.of("/Fleet/Dispatch", "/Fuel Management", "/Maintenance/Trucks", "/Safety", "/Vendor SLAs"));

        registerDrive("drive-mid-sp-compliance", "tenant-mid-2500", "Safety & Compliance Library", "documentLibrary", "compliance@contoso.com", "Compliance Portal");
        DRIVE_FOLDERS.put("drive-mid-sp-compliance", List.of("/OSHA/Audits", "/Driver Records", "/Certifications", "/Incidents", "/Training"));

        registerDrive("drive-mid-od-cfo", "tenant-mid-2500", "Sarah Miller (CFO) OneDrive", "business", "cfo@contoso.com", "Sarah Miller");
        DRIVE_FOLDERS.put("drive-mid-od-cfo", List.of("/Financial Models", "/Board Decks", "/Investor Relations", "/M&A Analysis", "/Taxes"));

        registerDrive("drive-mid-od-dispatcher", "tenant-mid-2500", "Dave Dispatcher OneDrive", "business", "dispatcher@contoso.com", "Dave Dispatcher");
        DRIVE_FOLDERS.put("drive-mid-od-dispatcher", List.of("/Route Schedules", "/Daily Logs", "/Customer Delivery", "/Carrier Rates", "/PODs"));

        registerDrive("drive-mid-od-rnd", "tenant-mid-2500", "Alex Chen (R&D Lead) OneDrive", "business", "rnd.lead@contoso.com", "Alex Chen");
        DRIVE_FOLDERS.put("drive-mid-od-rnd", List.of("/Technical Whitepapers", "/System Blueprints", "/AI Research", "/Architecture Docs", "/Prototypes"));

        // Aliases for backward compatibility
        registerDrive("sp-drive-finance", "tenant-mid-2500", "Finance Documents", "documentLibrary", "admin@contoso.com", "Finance");
        registerDrive("sp-drive-hr", "tenant-mid-2500", "Human Resources Portal", "documentLibrary", "admin@contoso.com", "HR");
        registerDrive("sp-drive-legal", "tenant-mid-2500", "Corporate Legal & Compliance", "documentLibrary", "admin@contoso.com", "Legal");
        registerDrive("sp-drive-it", "tenant-mid-2500", "IT & Infrastructure Assets", "documentLibrary", "admin@contoso.com", "IT");
        registerDrive("od-drive-rnd", "tenant-mid-2500", "rnd.lead@contoso.com's OneDrive", "business", "rnd.lead@contoso.com", "Alex Chen");
        registerDrive("od-drive-cfo", "tenant-mid-2500", "Sarah Miller (CFO) OneDrive", "business", "cfo@contoso.com", "Sarah Miller");
        registerDrive("od-drive-lead-architect", "tenant-mid-2500", "Marcus Vance (Architect) OneDrive", "business", "architect@contoso.com", "Marcus Vance");

        // --- 3. Fabrikam Financial (Enterprise E3/E5: 10,000 seats, 90 RUs/s) ---
        registerDrive("drive-ent-sp-wealth", "tenant-ent-10k", "Wealth Management & Assets", "documentLibrary", "wealth@fabrikam.com", "Wealth Management");
        DRIVE_FOLDERS.put("drive-ent-sp-wealth", List.of("/High Net Worth", "/Portfolios/Q1", "/Trusts", "/Equities/Research", "/Securities"));

        registerDrive("drive-ent-sp-audits", "tenant-ent-10k", "SOX & Regulatory Audits", "documentLibrary", "audit@fabrikam.com", "Audit Portal");
        DRIVE_FOLDERS.put("drive-ent-sp-audits", List.of("/SOX 404", "/SEC Filings", "/Risk Assessments", "/Internal Audit", "/FINRA Compliance"));

        registerDrive("drive-ent-od-risk", "tenant-ent-10k", "Elena Rostova (CRO) OneDrive", "business", "cro@fabrikam.com", "Elena Rostova");
        DRIVE_FOLDERS.put("drive-ent-od-risk", List.of("/Stress Testing", "/Credit Risk", "/Liquidity Reports", "/Capital Adequacy", "/Board Risk"));

        registerDrive("drive-ent-od-portfolio", "tenant-ent-10k", "Marcus Vance (Portfolio Mgr) OneDrive", "business", "marcus.pm@fabrikam.com", "Marcus Vance");
        DRIVE_FOLDERS.put("drive-ent-od-portfolio", List.of("/Hedge Strategy", "/Algorithmic Alpha", "/Derivative Models", "/Quarterly Letters", "/Analyst Notes"));

        registerDrive("drive-ent-od-data", "tenant-ent-10k", "David Kim (Data Platform Lead) OneDrive", "business", "data.lead@fabrikam.com", "David Kim");
        DRIVE_FOLDERS.put("drive-ent-od-data", List.of("/Vector Embeddings", "/Knowledge Graph", "/Semantic Taxonomies", "/Data Pipelines", "/Schemas"));

        // --- 4. Northwind Health (Large Enterprise: 35,000 seats, 150 RUs/s) ---
        registerDrive("drive-large-sp-clinical", "tenant-large-35k", "Clinical Research Trials", "documentLibrary", "research@northwindhealth.org", "Clinical Research");
        DRIVE_FOLDERS.put("drive-large-sp-clinical", List.of("/Phase III/Oncology", "/IRB Submissions", "/Genomics Data", "/Clinical Protocols", "/Drug Discovery"));

        registerDrive("drive-large-sp-hipaa", "tenant-large-35k", "Patient Records & HIPAA Vault", "documentLibrary", "privacy@northwindhealth.org", "HIPAA Compliance");
        DRIVE_FOLDERS.put("drive-large-sp-hipaa", List.of("/HIPAA Compliance", "/EMR Integration", "/Audit Trails", "/Business Associates", "/Privacy Policies"));

        registerDrive("drive-large-od-cmo", "tenant-large-35k", "Dr. Aris Thorne (CMO) OneDrive", "business", "cmo@northwindhealth.org", "Dr. Aris Thorne");
        DRIVE_FOLDERS.put("drive-large-od-cmo", List.of("/Medical Advisory", "/Trial Approvals", "/Bioethics", "/Peer Reviews", "/Research Grants"));

        registerDrive("drive-large-od-lab", "tenant-large-35k", "Lab Director OneDrive", "business", "lab@northwindhealth.org", "Lab Director");
        DRIVE_FOLDERS.put("drive-large-od-lab", List.of("/Sequencing Runs", "/Biosafety Level 3", "/Sample Manifests", "/Spectrometry", "/Reagents"));

        registerDrive("drive-large-od-privacy", "tenant-large-35k", "Rachel Adams (CPO) OneDrive", "business", "privacy.officer@northwindhealth.org", "Rachel Adams");
        DRIVE_FOLDERS.put("drive-large-od-privacy", List.of("/Data Subject Requests", "/HHS Reports", "/Breach Notifications", "/Consent Frameworks", "/Audits"));

        // --- 5. MegaCorp Global (Hyperscale Global: 100,000 seats, 250 RUs/s) ---
        registerDrive("drive-global-sp-treasury", "tenant-global-100k", "Global Treasury & M&A Vault", "documentLibrary", "treasury@megacorp.global", "Global Treasury");
        DRIVE_FOLDERS.put("drive-global-sp-treasury", List.of("/M&A/Target Mega", "/Foreign Exchange", "/Syndicated Loans", "/Global Bond Issuance", "/Cross-Border Tax"));

        registerDrive("drive-global-sp-rnd", "tenant-global-100k", "Core R&D & Patents Library", "documentLibrary", "rnd@megacorp.global", "Core R&D");
        DRIVE_FOLDERS.put("drive-global-sp-rnd", List.of("/Quantum Computing", "/AI Foundation Models", "/Hardware Patents", "/Autonomous Robotics", "/Defense Tech"));

        registerDrive("drive-global-od-cio", "tenant-global-100k", "Global CIO OneDrive", "business", "cio@megacorp.global", "Global CIO");
        DRIVE_FOLDERS.put("drive-global-od-cio", List.of("/Global Cloud Modernization", "/Hyperscale Contracts", "/Multi-Region Architecture", "/Supplier Negotiation", "/Executive Briefings"));

        registerDrive("drive-global-od-architect", "tenant-global-100k", "Enterprise Architect OneDrive", "business", "architect@megacorp.global", "Principal Architect");
        DRIVE_FOLDERS.put("drive-global-od-architect", List.of("/Microservices Topologies", "/OxMQ Core Scaling", "/Sub-Millisecond Fabric", "/Cluster Benchmarks", "/Disaster Recovery"));

        registerDrive("drive-global-od-legal", "tenant-global-100k", "General Counsel Global OneDrive", "business", "general.counsel@megacorp.global", "General Counsel");
        DRIVE_FOLDERS.put("drive-global-od-legal", List.of("/Antitrust Inquiries", "/Global Litigation", "/Trade Secrets", "/Cross-Border Data Transfer", "/Whistleblower Logs"));
    }

    private void registerDrive(String driveId, String tenantId, String name, String driveType, String ownerPrincipal, String siteOrUserName) {
        drivesMetadata.put(driveId, new DriveMetadata(driveId, tenantId, name, driveType, ownerPrincipal, siteOrUserName));
    }

    private record TemplateInfo(String fileName, String mimeType, long size) {}

    private Map<String, TemplateInfo> initPhysicalTemplates(Path storageDir) throws IOException {
        Map<String, TemplateInfo> templates = new LinkedHashMap<>();

        // 1. Technical Knowledge Base
        createPhysicalTemplate(storageDir, "template_knowledge_base.txt",
                "Enterprise Knowledge Base Article\nCategory: Architecture & Platform Engineering\nKeywords: OxMQ, Distributed Messaging, Project Loom, High Throughput\nAuthor: Enterprise Architect\nAbstract: Best practices for event-driven decoupled systems and real-time processing.",
                "text/plain", templates);

        // 2. Binary Dataset Archive
        byte[] entropyBytes = new byte[8192];
        new SecureRandom().nextBytes(entropyBytes);
        createPhysicalBinaryTemplate(storageDir, "template_dataset_archive.bin", entropyBytes, "application/octet-stream", templates);

        // 3. Executive Briefing Word Document
        createPhysicalTemplate(storageDir, "template_executive_briefing.docx",
                "Executive Briefing for Leadership: Quarterly Growth & Technology Strategy.\nConfidential.\nKey Initiatives & Milestones for Enterprise Cloud Architecture.",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document", templates);

        // 4. Strategic Plan Word Document
        createPhysicalTemplate(storageDir, "template_strategic_plan.docx",
                "Confidential business directives for Enterprise Operations.\nQuarterly objectives.\nAudited and Approved.",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document", templates);

        // 5. Governance Report PDF
        createPhysicalTemplate(storageDir, "template_governance_report.pdf",
                "%PDF-1.5\n% Corporate Governance Report\nCompliance & Risk Operations.\nConfidential Regulatory Audit.\n%%EOF",
                "application/pdf", templates);

        // 6. Financial Ledger Excel
        createPhysicalTemplate(storageDir, "template_financial_ledger.xlsx",
                "CostCenter,Category,Allocated,Actual,Status\nCC-101,Operations,$500000,$420000,Approved\nCC-102,Research,$750000,$690000,Audited",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", templates);

        // 7. Customer Extract CSV
        createPhysicalTemplate(storageDir, "template_customer_extract.csv",
                "id,name,email,ssn,account_tier\n1001,Client_Alpha,alpha@example.com,123-45-6789,PREMIUM\n1002,Client_Beta,beta@example.com,987-65-4321,ENTERPRISE",
                "text/csv", templates);

        // 8. Clinical Research Trials PDF
        createPhysicalTemplate(storageDir, "template_clinical_study.pdf",
                "%PDF-1.5\n% Clinical Trials Protocol & IRB Approval\nConfidential Medical Research Data.\nPatient Safety & Efficacy.\n%%EOF",
                "application/pdf", templates);

        // 9. Quantum Patents Document
        createPhysicalTemplate(storageDir, "template_quantum_patent.docx",
                "MegaCorp Global Patent Application: Quantum Optimization of High-Throughput Distributed Messaging Fabrics.\nInventors: Chief Scientist, Principal Architect.",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document", templates);

        // 10. HIPAA Compliance PDF
        createPhysicalTemplate(storageDir, "template_hipaa_compliance.pdf",
                "%PDF-1.5\n% HIPAA Safeguards and Protected Health Information Audit\nConfidential Health Records.\nSecurity Rule 164.308 Compliance.\n%%EOF",
                "application/pdf", templates);

        return templates;
    }

    private void createPhysicalTemplate(Path storageDir, String fileName, String content, String mimeType, Map<String, TemplateInfo> map) throws IOException {
        Path filePath = storageDir.resolve(fileName);
        if (!Files.exists(filePath)) {
            Files.writeString(filePath, content, StandardCharsets.UTF_8);
        }
        map.put(fileName, new TemplateInfo(fileName, mimeType, Files.size(filePath)));
    }

    private void createPhysicalBinaryTemplate(Path storageDir, String fileName, byte[] content, String mimeType, Map<String, TemplateInfo> map) throws IOException {
        Path filePath = storageDir.resolve(fileName);
        if (!Files.exists(filePath)) {
            Files.write(filePath, content);
        }
        map.put(fileName, new TemplateInfo(fileName, mimeType, Files.size(filePath)));
    }

    /**
     * Seeds physical files across all 5 tenants and their 25 drives.
     * Supports tiered enterprise distributions:
     * - SMB: 1,000 files (200 / drive)
     * - Mid-Market: 5,000 files (1,000 / drive)
     * - Enterprise: 25,000 files (5,000 / drive)
     * - Large Enterprise: 100,000 files (20,000 / drive)
     * - Global Hyperscale: 100,000 files (20,000 / drive)
     * Total: 231,000 authentic files cataloged with real disk templates.
     */
    private void seedPhysicalFilesAcrossTenants(Path storageDir) throws IOException {
        Map<String, TemplateInfo> templates = initPhysicalTemplates(storageDir);
        String[] scopes = {"private", "organization", "users", "anonymous"};
        long now = System.currentTimeMillis();

        int totalSeeded = 0;
        for (DriveMetadata dm : drivesMetadata.values()) {
            // Skip backward compatibility alias drives during bulk seed
            if (dm.driveId().startsWith("sp-drive-") || dm.driveId().startsWith("od-drive-")) {
                continue;
            }

            int filesForThisDrive;
            if ("COMPACT".equalsIgnoreCase(scaleMode)) {
                filesForThisDrive = Math.max(20, totalFilesToSeed / 5 / 5);
            } else {
                filesForThisDrive = switch (dm.tenantId()) {
                    case "tenant-smb-250" -> smbFiles / 5;
                    case "tenant-mid-2500" -> midFiles / 5;
                    case "tenant-ent-10k" -> entFiles / 5;
                    case "tenant-large-35k" -> largeFiles / 5;
                    case "tenant-global-100k" -> globalFiles / 5;
                    default -> 200;
                };
            }

            List<String> folders = DRIVE_FOLDERS.getOrDefault(dm.driveId(), List.of("/Documents/General"));
            String tenantId = dm.tenantId();
            List<String> driveList = driveFiles.computeIfAbsent(dm.driveId(), k -> Collections.synchronizedList(new ArrayList<>(filesForThisDrive)));

            for (int i = 1; i <= filesForThisDrive; i++) {
                String folder = folders.get(i % folders.size());
                String scope = scopes[i % scopes.length];
                int typeRoll = i % 100;

                String templateKey;
                String ext;
                if (typeRoll == 1) {
                    templateKey = "template_knowledge_base.txt";
                    ext = "txt";
                } else if (typeRoll == 2) {
                    templateKey = "template_dataset_archive.bin";
                    ext = "bin";
                } else if (typeRoll == 3) {
                    templateKey = "template_executive_briefing.docx";
                    ext = "docx";
                } else if (typeRoll < 40) {
                    templateKey = "template_strategic_plan.docx";
                    ext = "docx";
                } else if (typeRoll < 70) {
                    templateKey = "template_governance_report.pdf";
                    ext = "pdf";
                } else if (typeRoll < 90) {
                    templateKey = "template_financial_ledger.xlsx";
                    ext = "xlsx";
                } else {
                    templateKey = "template_customer_extract.csv";
                    ext = "csv";
                }

                TemplateInfo tInfo = templates.get(templateKey);
                String fileName = dm.name().replace(" ", "_") + "_Doc_" + i + "." + ext;
                String fileId = "file-" + dm.driveId() + "-" + i;

                MockFileInfo info = new MockFileInfo(
                        fileId, dm.driveId(), tenantId, fileName, folder,
                        tInfo.size(), tInfo.mimeType(), scope,
                        tInfo.fileName(), now - (i * 1000L)
                );

                filesInventory.put(fileId, info);
                driveList.add(fileId);
                totalSeeded++;
            }
        }
        log.info("Finished seeding {} files across all enterprise tenant drives (Total inventory: {} files)", totalSeeded, filesInventory.size());
    }

    private void seedBenchmarkInitialFiles(Path storageDir) throws IOException {
        createPhysicalFile(storageDir, "Corporate_Finance_2026.docx",
                "CONFIDENTIAL - Contoso Financial Strategy 2026.\nRevenue projections and quarterly targets.\nPrepared by CFO.",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "sp-drive-finance", "tenant-mid-2500", "private", "/Budgets/2026/Q1");

        createPhysicalFile(storageDir, "system_architecture_spec.txt",
                "OxDoc 365 Enterprise Content Intelligence & Document AI Specification.\nArchitecture Blueprint & Indexing Service.\nVersion 1.7.0.",
                "text/plain",
                "od-drive-rnd", "tenant-mid-2500", "private", "/Architecture/2026");

        byte[] binaryData = new byte[16384];
        new SecureRandom().nextBytes(binaryData);
        createPhysicalBinaryFile(storageDir, "vector_embeddings_index.bin",
                binaryData,
                "application/octet-stream",
                "sp-drive-it", "tenant-mid-2500", "private", "/VectorDB/Indices");

        createPhysicalFile(storageDir, "Executive_Strategic_Roadmap.docx",
                "Executive Strategic Roadmap: Cross-Enterprise Document Knowledge Indexing and Distributed Pipeline.\nConfidential.",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "sp-drive-finance", "tenant-mid-2500", "organization", "/Confidential/Strategy");
    }

    private void createPhysicalFile(Path dir, String fileName, String content, String mimeType, String driveId, String tenantId, String exposureScope, String folderPath) throws IOException {
        Path filePath = dir.resolve(fileName);
        if (!Files.exists(filePath)) {
            Files.writeString(filePath, content, StandardCharsets.UTF_8);
        }
        registerFile(fileName, filePath, mimeType, driveId, tenantId, exposureScope, folderPath);
    }

    private void createPhysicalBinaryFile(Path dir, String fileName, byte[] content, String mimeType, String driveId, String tenantId, String exposureScope, String folderPath) throws IOException {
        Path filePath = dir.resolve(fileName);
        if (!Files.exists(filePath)) {
            Files.write(filePath, content);
        }
        registerFile(fileName, filePath, mimeType, driveId, tenantId, exposureScope, folderPath);
    }

    private void registerFile(String fileName, Path filePath, String mimeType, String driveId, String tenantId, String exposureScope, String folderPath) {
        String fileId = "file-" + UUID.nameUUIDFromBytes((driveId + ":" + fileName).getBytes(StandardCharsets.UTF_8)).toString().replace("-", "").substring(0, 12);
        long size = filePath.toFile().length();
        MockFileInfo info = new MockFileInfo(
                fileId, driveId, tenantId, fileName, folderPath, size, mimeType, exposureScope,
                filePath.getFileName().toString(), System.currentTimeMillis()
        );
        filesInventory.put(fileId, info);
        List<String> list = driveFiles.computeIfAbsent(driveId, k -> Collections.synchronizedList(new ArrayList<>()));
        list.add(fileId);
    }

    public synchronized void seedAdditionalFiles(int targetTotal) {
        try {
            Path storageDir = Paths.get(storageDirPath);
            Files.createDirectories(storageDir);
            seedPhysicalFilesAcrossTenants(storageDir);
            log.info("Mock Office 365 Server scaled inventory to {} real files on disk", filesInventory.size());
        } catch (IOException e) {
            log.error("Failed seeding additional files: {}", e.getMessage(), e);
        }
    }

    public int getTotalFiles() {
        return filesInventory.size();
    }

    public void setRateLimit(String tenantId, int limit) {
        TenantRateLimiter limiter = tenantLimiters.get(tenantId);
        if (limiter != null) {
            limiter.setLimit(limit);
            log.info("Mock M365 Server rate limit for tenant [{}] set to {} req/s", tenantId, limit);
        }
    }

    public void setRateLimit(int limit) {
        for (TenantRateLimiter limiter : tenantLimiters.values()) {
            limiter.setLimit(limit);
        }
        log.info("Mock M365 Server global rate limit set to {} req/s across all tenants", limit);
    }

    public int getRateLimit(String tenantId) {
        TenantRateLimiter limiter = tenantLimiters.get(tenantId);
        return limiter != null ? limiter.getLimit() : 50;
    }

    public int getRateLimit() {
        return getRateLimit("tenant-mid-2500");
    }

    public void trigger429Burst(String tenantId, int count) {
        TenantRateLimiter limiter = tenantLimiters.get(tenantId);
        if (limiter != null) {
            limiter.triggerBurst(count);
            log.warn("Mock M365 Server triggering 429 burst on tenant [{}] for next {} requests", tenantId, count);
        }
    }

    public void trigger429Burst(int count) {
        trigger429Burst("tenant-smb-250", count);
    }

    public void setSimulate429Burst(String tenantId, boolean enabled) {
        TenantRateLimiter limiter = tenantLimiters.get(tenantId);
        if (limiter != null) {
            limiter.setBurstActive(enabled);
            log.warn("Mock M365 Server simulate 429 burst mode for [{}]: {}", tenantId, enabled);
        }
    }

    public void setSimulate429Burst(boolean enabled) {
        setSimulate429Burst("tenant-smb-250", enabled);
    }

    /**
     * Resolves the tenant ID from request headers, Bearer token, query parameter, or driveId.
     */
    public String resolveTenantId(HttpExchange exchange, String driveId) {
        // 1. Try X-Tenant-Id header
        String header = exchange.getRequestHeaders().getFirst("X-Tenant-Id");
        if (header != null && !header.isBlank()) {
            return header.trim();
        }

        // 2. Try Authorization Bearer token: e.g. "Bearer mock-azure-ad-jwt-token-tenant-smb-250-..."
        String auth = exchange.getRequestHeaders().getFirst("Authorization");
        if (auth != null && auth.startsWith("Bearer ")) {
            String token = auth.substring(7);
            if (token.contains("contoso-enterprise-tenant")) {
                return "contoso-enterprise-tenant";
            }
            for (TenantProfile p : TenantProfile.ALL_PROFILES) {
                if (token.contains(p.tenantId())) {
                    return p.tenantId();
                }
            }
        }

        // 3. Try query param tenantId
        String query = exchange.getRequestURI().getQuery();
        if (query != null && query.contains("tenantId=")) {
            for (String pair : query.split("&")) {
                if (pair.startsWith("tenantId=")) {
                    return pair.substring(9);
                }
            }
        }

        // 4. Try resolving from driveId if provided
        if (driveId != null) {
            DriveMetadata dm = drivesMetadata.get(driveId);
            if (dm != null) {
                return dm.tenantId();
            }
            if (driveId.startsWith("drive-smb-")) return TenantProfile.SMB_250.tenantId();
            if (driveId.startsWith("drive-mid-")) return TenantProfile.MID_2500.tenantId();
            if (driveId.startsWith("drive-ent-")) return TenantProfile.ENT_10K.tenantId();
            if (driveId.startsWith("drive-large-")) return TenantProfile.LARGE_35K.tenantId();
            if (driveId.startsWith("drive-global-")) return TenantProfile.GLOBAL_100K.tenantId();
        }

        return TenantProfile.MID_2500.tenantId();
    }


    /**
     * Checks rate limits and applies RU cost per Microsoft Learn specification.
     * Endpoints:
     * - Delta sync: 1 RU
     * - Permissions: 1 RU
     * - Search: 3 RUs
     * - Download: 0 RUs
     */
    private boolean checkRateLimitAndSetHeaders(HttpExchange exchange, String tenantId, int ruCost) throws IOException {
        return checkRateLimitAndSetHeaders(exchange, tenantId, null, ruCost);
    }

    private boolean checkRateLimitAndSetHeaders(HttpExchange exchange, String tenantId, String driveId, int ruCost) throws IOException {
        String rateLimitTenant = "contoso-enterprise-tenant".equalsIgnoreCase(tenantId)
                ? TenantProfile.MID_2500.tenantId()
                : tenantId;
        TenantRateLimiter limiter = tenantLimiters.computeIfAbsent(
                rateLimitTenant,
                id -> new TenantRateLimiter(id, TenantProfile.getByIdOrDefault(id).burstLimitRUsPerSec())
        );

        Map<String, String> headers = new LinkedHashMap<>();
        ChaosDecision decision = limiter.evaluate(ruCost, headers);

        // Check per-drive rate limit if driveId is present and tenant limit passed
        if (decision == ChaosDecision.PROCEED && driveId != null && !driveId.isBlank()) {
            DriveRateLimiter driveLimiter = driveLimiters.computeIfAbsent(
                    driveId,
                    id -> new DriveRateLimiter(id, driveBurstLimit)
            );
            if (!driveLimiter.tryAcquire(headers)) {
                decision = ChaosDecision.THROTTLE_429;
            }
        }

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            exchange.getResponseHeaders().set(entry.getKey(), entry.getValue());
        }

        switch (decision) {
            case THROTTLE_429 -> {
                String retry = headers.getOrDefault("Retry-After", "3");
                String reason = (driveId != null && !driveId.isBlank() && "1.15".equals(headers.get("x-ms-throttle-limit-percentage")))
                        ? "document library [" + driveId + "] rate limit was exceeded."
                        : "tenant [" + tenantId + "] application quota was exceeded.";
                sendResponse(exchange, 429, "{\"error\": {\"code\": \"activityLimitReached\", \"message\": \"The request has been throttled because " + reason + " Retry after " + retry + " seconds.\"}}");
                return false;
            }
            case SERVICE_UNAVAILABLE_503 -> {
                String retry = headers.getOrDefault("Retry-After", "5");
                sendResponse(exchange, 503, "{\"error\": {\"code\": \"serviceUnavailable\", \"message\": \"The Microsoft 365 SharePoint Online service is temporarily unavailable for tenant [" + tenantId + "]. Retry after " + retry + " seconds.\"}}");
                return false;
            }
            case GATEWAY_TIMEOUT_504 -> {
                sendResponse(exchange, 504, "{\"error\": {\"code\": \"gatewayTimeout\", \"message\": \"Microsoft Graph gateway timed out waiting for backend storage for tenant [" + tenantId + "].\"}}");
                return false;
            }
            case INTERNAL_ERROR_500 -> {
                sendResponse(exchange, 500, "{\"error\": {\"code\": \"generalException\", \"message\": \"An unexpected internal server error occurred while servicing tenant [" + tenantId + "].\"}}");
                return false;
            }
            case PROCEED -> {
                return true;
            }
        }
        return true;
    }

    // --- HTTP HANDLERS ---

    private void handleOAuthToken(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "Method Not Allowed");
            return;
        }

        String path = exchange.getRequestURI().getPath();
        String tenantId = "tenant-mid-2500";
        for (TenantProfile p : TenantProfile.ALL_PROFILES) {
            if (path.contains("/" + p.tenantId() + "/")) {
                tenantId = p.tenantId();
                break;
            }
        }

        ObjectNode tokenNode = objectMapper.createObjectNode();
        tokenNode.put("token_type", "Bearer");
        tokenNode.put("expires_in", 3600);
        tokenNode.put("access_token", "mock-azure-ad-jwt-token-" + tenantId + "-" + System.currentTimeMillis());
        sendJsonResponse(exchange, 200, tokenNode);
    }

    private void handleSubscribedSkus(HttpExchange exchange) throws IOException {
        String tenantId = resolveTenantId(exchange, null);
        if (!checkRateLimitAndSetHeaders(exchange, tenantId, 1)) {
            return;
        }

        TenantProfile profile = TenantProfile.getByIdOrDefault(tenantId);
        ObjectNode root = objectMapper.createObjectNode();
        root.put("@odata.context", "https://graph.microsoft.com/v1.0/$metadata#subscribedSkus");
        ArrayNode arr = root.putArray("value");
        ObjectNode skuNode = arr.addObject();
        skuNode.put("id", "sku-" + tenantId + "-primary");
        skuNode.put("skuId", UUID.nameUUIDFromBytes(("sku:" + tenantId).getBytes(StandardCharsets.UTF_8)).toString());
        skuNode.put("skuPartNumber", profile.skuPartNumber());
        skuNode.put("capabilityStatus", "Enabled");
        skuNode.put("consumedUnits", (int) (profile.seatCount() * 0.95));
        ObjectNode prepaid = skuNode.putObject("prepaidUnits");
        prepaid.put("enabled", profile.seatCount());
        prepaid.put("suspended", 0);
        prepaid.put("warning", 0);

        ArrayNode plans = skuNode.putArray("servicePlans");
        ObjectNode sp1 = plans.addObject();
        sp1.put("servicePlanId", UUID.nameUUIDFromBytes(("sp:" + tenantId + ":sp").getBytes(StandardCharsets.UTF_8)).toString());
        sp1.put("servicePlanName", "SHAREPOINTENTERPRISE");
        sp1.put("provisioningStatus", "Success");
        sp1.put("appliesTo", "User");

        ObjectNode sp2 = plans.addObject();
        sp2.put("servicePlanId", UUID.nameUUIDFromBytes(("sp:" + tenantId + ":ex").getBytes(StandardCharsets.UTF_8)).toString());
        sp2.put("servicePlanName", "EXCHANGE_S_ENTERPRISE");
        sp2.put("provisioningStatus", "Success");
        sp2.put("appliesTo", "User");

        sendJsonResponse(exchange, 200, root);
    }

    private void handleOrganization(HttpExchange exchange) throws IOException {
        String tenantId = resolveTenantId(exchange, null);
        if (!checkRateLimitAndSetHeaders(exchange, tenantId, 1)) {
            return;
        }

        TenantProfile profile = TenantProfile.getByIdOrDefault(tenantId);
        ObjectNode root = objectMapper.createObjectNode();
        root.put("@odata.context", "https://graph.microsoft.com/v1.0/$metadata#organization");
        ArrayNode arr = root.putArray("value");
        ObjectNode org = arr.addObject();
        org.put("id", tenantId);
        org.put("displayName", profile.organizationName());
        ArrayNode domains = org.putArray("verifiedDomains");
        ObjectNode dom = domains.addObject();
        dom.put("name", profile.domainName());
        dom.put("isDefault", true);

        sendJsonResponse(exchange, 200, root);
    }

    private void handleGetSites(HttpExchange exchange) throws IOException {
        String tenantId = resolveTenantId(exchange, null);
        if (!checkRateLimitAndSetHeaders(exchange, tenantId, 1)) {
            return;
        }

        ObjectNode root = objectMapper.createObjectNode();
        root.put("@odata.context", "https://graph.microsoft.com/v1.0/$metadata#sites");
        ArrayNode value = root.putArray("value");

        boolean isLegacyContoso = "contoso-enterprise-tenant".equalsIgnoreCase(tenantId);

        for (DriveMetadata dm : drivesMetadata.values()) {
            boolean isLegacyDrive = dm.driveId().startsWith("sp-drive-") || dm.driveId().startsWith("od-drive-");
            if (isLegacyContoso) {
                if (!isLegacyDrive) continue;
            } else {
                if (isLegacyDrive) continue;
            }
            if ((dm.tenantId().equals(tenantId) || (isLegacyContoso && "tenant-mid-2500".equals(dm.tenantId())))
                    && "documentLibrary".equals(dm.driveType())) {
                ObjectNode site = value.addObject();
                site.put("id", "site-" + dm.driveId());
                site.put("name", dm.name());
                site.put("webUrl", "https://" + tenantId + ".sharepoint.com/sites/" + dm.siteOrUserName());
            }
        }

        sendJsonResponse(exchange, 200, root);
    }

    private void handleSiteDrives(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        // Path format: /v1.0/sites/{siteId}/drives
        String siteId = "";
        if (path.contains("/sites/") && path.contains("/drives")) {
            siteId = path.substring(path.indexOf("/sites/") + 7, path.indexOf("/drives"));
        }

        String tenantId = resolveTenantId(exchange, siteId);
        if (!checkRateLimitAndSetHeaders(exchange, tenantId, 1)) {
            return;
        }

        ObjectNode root = objectMapper.createObjectNode();
        ArrayNode value = root.putArray("value");
        boolean isLegacyContoso = "contoso-enterprise-tenant".equalsIgnoreCase(tenantId);

        for (DriveMetadata dm : drivesMetadata.values()) {
            boolean isLegacyDrive = dm.driveId().startsWith("sp-drive-") || dm.driveId().startsWith("od-drive-");
            if (isLegacyContoso) {
                if (!isLegacyDrive) continue;
            } else {
                if (isLegacyDrive) continue;
            }
            if ((dm.tenantId().equals(tenantId) || (isLegacyContoso && "tenant-mid-2500".equals(dm.tenantId())))
                    && "documentLibrary".equals(dm.driveType())) {
                if (siteId.isEmpty() || siteId.contains(dm.driveId()) || ("site-" + dm.driveId()).equals(siteId)) {
                    ObjectNode drive = value.addObject();
                    drive.put("id", dm.driveId());
                    drive.put("name", dm.name());
                    drive.put("driveType", "documentLibrary");
                }
            }
        }

        sendJsonResponse(exchange, 200, root);
    }

    private void handleUsersAndDrives(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String tenantId = resolveTenantId(exchange, null);
        boolean isLegacyContoso = "contoso-enterprise-tenant".equalsIgnoreCase(tenantId);

        if (path.endsWith("/drive")) {
            // Path: /v1.0/users/{userId}/drive
            String userId = path.replace("/v1.0/users/", "").replace("/drive", "");
            if (!checkRateLimitAndSetHeaders(exchange, tenantId, 1)) {
                return;
            }

            for (DriveMetadata dm : drivesMetadata.values()) {
                boolean isLegacyDrive = dm.driveId().startsWith("sp-drive-") || dm.driveId().startsWith("od-drive-");
                if (isLegacyContoso) {
                    if (!isLegacyDrive) continue;
                } else {
                    if (isLegacyDrive) continue;
                }
                if ((dm.tenantId().equals(tenantId) || (isLegacyContoso && "tenant-mid-2500".equals(dm.tenantId())))
                        && "business".equals(dm.driveType())
                        && (dm.driveId().contains(userId) || dm.ownerPrincipal().contains(userId))) {
                    ObjectNode drive = objectMapper.createObjectNode();
                    drive.put("id", dm.driveId());
                    drive.put("name", dm.name());
                    drive.put("driveType", "business");
                    sendJsonResponse(exchange, 200, drive);
                    return;
                }
            }

            // Fallback generic drive
            ObjectNode drive = objectMapper.createObjectNode();
            drive.put("id", "drive-" + tenantId + "-od-" + userId);
            drive.put("name", userId + " OneDrive");
            drive.put("driveType", "business");
            sendJsonResponse(exchange, 200, drive);
            return;
        }

        // Path: /v1.0/users
        if (!checkRateLimitAndSetHeaders(exchange, tenantId, 1)) {
            return;
        }

        ObjectNode root = objectMapper.createObjectNode();
        ArrayNode users = root.putArray("value");
        java.util.Set<String> seenUsers = new java.util.HashSet<>();

        for (DriveMetadata dm : drivesMetadata.values()) {
            boolean isLegacyDrive = dm.driveId().startsWith("sp-drive-") || dm.driveId().startsWith("od-drive-");
            if (isLegacyContoso) {
                if (!isLegacyDrive) continue;
            } else {
                if (isLegacyDrive) continue;
            }
            if ((dm.tenantId().equals(tenantId) || (isLegacyContoso && "tenant-mid-2500".equals(dm.tenantId())))
                    && "business".equals(dm.driveType())) {
                String userId = dm.ownerPrincipal().split("@")[0];
                if (seenUsers.add(userId)) {
                    ObjectNode u = users.addObject();
                    u.put("id", userId);
                    u.put("userPrincipalName", dm.ownerPrincipal());
                }
            }
        }

        sendJsonResponse(exchange, 200, root);
    }

    private void appendFileJson(ArrayNode value, MockFileInfo info, String tenantId, String driveId) {
        ObjectNode item = value.addObject();
        item.put("id", info.fileId());
        item.put("name", info.name());
        item.put("size", info.size());
        item.put("webUrl", "https://" + tenantId + ".sharepoint.com/documents/" + info.name());
        item.put("@microsoft.graph.downloadUrl", "http://localhost:" + port + "/mock-storage/download/" + info.fileId());
        item.put("eTag", "etag-" + info.lastModified());
        item.put("cTag", "ctag-" + info.lastModified());

        ObjectNode fileObj = item.putObject("file");
        fileObj.put("mimeType", info.mimeType());

        ObjectNode parentRef = item.putObject("parentReference");
        parentRef.put("path", "/drives/" + driveId + "/root:" + info.path());
    }

    private void handleDrives(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();

        // 1. Delta query: /v1.0/drives/{driveId}/root/delta (1 RU)
        if (path.contains("/root/delta")) {
            String[] parts = path.split("/");
            String driveId = parts[3];
            String tenantId = resolveTenantId(exchange, driveId);

            if (!checkRateLimitAndSetHeaders(exchange, tenantId, driveId, 1)) {
                return;
            }

            ObjectNode root = objectMapper.createObjectNode();
            ArrayNode value = root.putArray("value");

            List<String> fileIds = driveFiles.getOrDefault(driveId, Collections.emptyList());
            String query = exchange.getRequestURI().getQuery();

            int offset = 0;
            boolean isIncrementalToken = false;
            long tokenTimestamp = 0;

            if (query != null) {
                for (String param : query.split("&")) {
                    if (param.startsWith("$skiptoken=")) {
                        try {
                            offset = Integer.parseInt(param.substring(11));
                        } catch (NumberFormatException ignored) {}
                    } else if (param.startsWith("token=")) {
                        isIncrementalToken = true;
                        try {
                            tokenTimestamp = Long.parseLong(param.substring(6));
                        } catch (NumberFormatException ignored) {}
                    }
                }
            }

            if (isIncrementalToken) {
                for (String fid : fileIds) {
                    MockFileInfo info = filesInventory.get(fid);
                    if (info != null && info.lastModified() > tokenTimestamp) {
                        appendFileJson(value, info, tenantId, driveId);
                    }
                }
                root.put("@odata.deltaLink", "http://localhost:" + port + "/v1.0/drives/" + driveId + "/root/delta?token=" + System.currentTimeMillis());
            } else {
                int pageSize = 500;
                int totalCount = fileIds.size();
                int start = Math.min(offset, totalCount);
                int end = Math.min(start + pageSize, totalCount);

                for (int i = start; i < end; i++) {
                    String fid = fileIds.get(i);
                    MockFileInfo info = filesInventory.get(fid);
                    if (info != null) {
                        appendFileJson(value, info, tenantId, driveId);
                    }
                }

                if (end < totalCount) {
                    root.put("@odata.nextLink", "http://localhost:" + port + "/v1.0/drives/" + driveId + "/root/delta?$skiptoken=" + end);
                } else {
                    root.put("@odata.deltaLink", "http://localhost:" + port + "/v1.0/drives/" + driveId + "/root/delta?token=" + System.currentTimeMillis());
                }
            }

            sendJsonResponse(exchange, 200, root);
            return;
        }

        // 2. Permissions query: /v1.0/drives/{driveId}/items/{itemId}/permissions (1 RU)
        if (path.contains("/permissions")) {
            String[] parts = path.split("/");
            String driveId = parts[3];
            String itemId = parts[5];
            String tenantId = resolveTenantId(exchange, driveId);

            if (!checkRateLimitAndSetHeaders(exchange, tenantId, driveId, 1)) {
                return;
            }

            MockFileInfo info = filesInventory.get(itemId);
            ObjectNode root = objectMapper.createObjectNode();
            ArrayNode value = root.putArray("value");

            if (info != null) {
                ObjectNode perm = value.addObject();
                perm.put("id", "perm-" + info.fileId());
                ArrayNode roles = perm.putArray("roles");
                roles.add("read");

                ObjectNode link = perm.putObject("link");
                link.put("type", "view");
                link.put("scope", info.exposureScope()); // "anonymous", "users", "organization"

                if ("private".equalsIgnoreCase(info.exposureScope())) {
                    perm.remove("link");
                    ObjectNode grantedTo = perm.putObject("grantedToV2");
                    ObjectNode user = grantedTo.putObject("user");
                    user.put("displayName", "Owner");
                    user.put("email", "owner@" + tenantId + ".com");
                }
            }

            sendJsonResponse(exchange, 200, root);
            return;
        }

        sendResponse(exchange, 404, "Not Found");
    }

    /**
     * Binary File Download via direct Azure Blob SAS URL simulation (0 RUs).
     */
    private void handleFileDownload(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String fileId = path.substring(path.lastIndexOf('/') + 1);

        MockFileInfo info = filesInventory.get(fileId);
        if (info == null) {
            sendResponse(exchange, 404, "File Not Found in Mock M365 Storage");
            return;
        }

        File physicalFile = Paths.get(storageDirPath, info.diskFileName()).toFile();
        if (!physicalFile.exists()) {
            sendResponse(exchange, 404, "Physical File Missing on Disk: " + physicalFile.getAbsolutePath());
            return;
        }

        exchange.getResponseHeaders().set("Content-Type", info.mimeType());
        exchange.getResponseHeaders().set("Content-Length", String.valueOf(physicalFile.length()));
        exchange.sendResponseHeaders(200, physicalFile.length());

        try (InputStream in = new FileInputStream(physicalFile);
             OutputStream out = exchange.getResponseBody()) {
            in.transferTo(out);
        }
    }

    public void saveForwardFile(String driveId, String fileName, String exposureScope, byte[] content, String mimeType) {
        try {
            Path storageDir = Paths.get(storageDirPath);
            Files.createDirectories(storageDir);
            Path targetFile = storageDir.resolve(fileName);
            Files.write(targetFile, content);
            List<String> folderList = DRIVE_FOLDERS.getOrDefault(driveId, List.of("/Uploads/RealTime"));
            String folder = folderList.get(0);
            DriveMetadata dm = drivesMetadata.get(driveId);
            String tenantId = dm != null ? dm.tenantId() : "tenant-mid-2500";
            registerFile(fileName, targetFile, mimeType, driveId, tenantId, exposureScope, folder);
            log.info("Physically wrote forward file to disk: [{}] ({} bytes) for tenant [{}]", targetFile.toAbsolutePath(), content.length, tenantId);
        } catch (IOException e) {
            log.error("Failed saving forward file to disk: {}", e.getMessage(), e);
        }
    }

    private void handleAdminUpload(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "Method Not Allowed");
            return;
        }

        try {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            var json = objectMapper.readTree(body);

            String fileName = json.has("fileName") ? json.get("fileName").asText() : "Realtime_Uploaded_File_" + System.currentTimeMillis() + ".docx";
            String content = json.has("content") ? json.get("content").asText() : "Confidential Document Body Created at " + System.currentTimeMillis();
            String driveId = json.has("driveId") ? json.get("driveId").asText() : "drive-mid-sp-logistics";
            String exposure = json.has("exposure") ? json.get("exposure").asText() : "anonymous";
            String mimeType = json.has("mimeType") ? json.get("mimeType").asText() : "application/vnd.openxmlformats-officedocument.wordprocessingml.document";

            Path storageDir = Paths.get(storageDirPath);
            Path targetFile = storageDir.resolve(fileName);
            if (json.has("contentBase64")) {
                byte[] raw = Base64.getDecoder().decode(json.get("contentBase64").asText());
                Files.write(targetFile, raw);
            } else {
                Files.writeString(targetFile, content, StandardCharsets.UTF_8);
            }

            DriveMetadata dm = drivesMetadata.get(driveId);
            String tenantId = dm != null ? dm.tenantId() : "tenant-mid-2500";
            registerFile(fileName, targetFile, mimeType, driveId, tenantId, exposure, "/Uploads/RealTime");

            triggerWebhook(driveId);

            ObjectNode resp = objectMapper.createObjectNode();
            resp.put("success", true);
            resp.put("fileName", fileName);
            resp.put("driveId", driveId);
            resp.put("tenantId", tenantId);
            resp.put("bytesWritten", targetFile.toFile().length());
            sendJsonResponse(exchange, 200, resp);
        } catch (Exception e) {
            sendResponse(exchange, 500, "Upload error: " + e.getMessage());
        }
    }

    private void handleAdminSimulate429(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        String tenantId = "tenant-smb-250";
        int count = 20;
        boolean enabled = true;

        if (query != null) {
            for (String param : query.split("&")) {
                if (param.startsWith("tenantId=")) tenantId = param.substring(9);
                if (param.startsWith("burstCount=")) count = Integer.parseInt(param.substring(11));
                if (param.startsWith("enabled=")) enabled = Boolean.parseBoolean(param.substring(8));
            }
        }

        trigger429Burst(tenantId, count);
        setSimulate429Burst(tenantId, enabled);

        ObjectNode resp = objectMapper.createObjectNode();
        resp.put("status", "TRIGGERED");
        resp.put("tenantId", tenantId);
        resp.put("burstCount", count);
        resp.put("enabled", enabled);
        resp.put("message", "Simulated HTTP 429 burst on tenant: " + tenantId);
        sendJsonResponse(exchange, 200, resp);
    }

    private void handleAdminSetRateLimit(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        String tenantId = "tenant-mid-2500";
        int limit = 50;

        if (query != null) {
            for (String param : query.split("&")) {
                if (param.startsWith("tenantId=")) tenantId = param.substring(9);
                if (param.startsWith("limit=")) limit = Integer.parseInt(param.substring(6));
            }
        }

        if ("all".equalsIgnoreCase(tenantId)) {
            setRateLimit(limit);
        } else {
            setRateLimit(tenantId, limit);
        }

        ObjectNode resp = objectMapper.createObjectNode();
        resp.put("status", "UPDATED");
        resp.put("tenantId", tenantId);
        resp.put("limit", limit);
        sendJsonResponse(exchange, 200, resp);
    }

    private void handleAdminSeed(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        int count = 5000;
        if (query != null) {
            for (String param : query.split("&")) {
                if (param.startsWith("count=")) count = Integer.parseInt(param.substring(6));
            }
        }
        seedAdditionalFiles(count);
        ObjectNode resp = objectMapper.createObjectNode();
        resp.put("status", "SEEDED");
        resp.put("totalFiles", getTotalFiles());
        resp.put("message", "Successfully seeded files up to " + count);
        sendJsonResponse(exchange, 200, resp);
    }

    private void handleAdminStats(HttpExchange exchange) throws IOException {
        ObjectNode resp = objectMapper.createObjectNode();
        resp.put("status", "UP");
        resp.put("totalDrives", drivesMetadata.size());
        resp.put("totalFiles", filesInventory.size());
        ObjectNode tenants = resp.putObject("tenants");
        for (TenantProfile profile : TenantProfile.ALL_PROFILES) {
            ObjectNode t = tenants.putObject(profile.tenantId());
            t.put("name", profile.organizationName());
            t.put("tier", profile.tier().name());
            t.put("limit", getRateLimit(profile.tenantId()));
        }
        sendJsonResponse(exchange, 200, resp);
    }

    private void handleAdminChaos(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        String tenantId = "all";
        int statusCode = 503;
        int count = 5;
        int retryAfter = 3;
        double randomRate = 0.0;

        if (query != null) {
            for (String param : query.split("&")) {
                if (param.startsWith("tenantId=")) tenantId = param.substring(9);
                if (param.startsWith("status=")) statusCode = Integer.parseInt(param.substring(7));
                if (param.startsWith("count=")) count = Integer.parseInt(param.substring(6));
                if (param.startsWith("retryAfter=")) retryAfter = Integer.parseInt(param.substring(11));
                if (param.startsWith("randomRate=")) randomRate = Double.parseDouble(param.substring(11));
            }
        }

        List<TenantRateLimiter> targetLimiters = "all".equalsIgnoreCase(tenantId)
                ? new ArrayList<>(tenantLimiters.values())
                : List.of(tenantLimiters.computeIfAbsent(tenantId, id -> new TenantRateLimiter(id, 50)));

        for (TenantRateLimiter l : targetLimiters) {
            l.setCustomRetryAfter(retryAfter);
            if (randomRate > 0) {
                l.setRandomChaosProbability(randomRate);
            }
            if (count > 0) {
                switch (statusCode) {
                    case 429 -> l.triggerBurst(count);
                    case 503 -> l.trigger503(count);
                    case 504 -> l.trigger504(count);
                    case 500 -> l.trigger500(count);
                }
            }
        }

        ObjectNode resp = objectMapper.createObjectNode();
        resp.put("status", "CHAOS_INJECTED");
        resp.put("tenantId", tenantId);
        resp.put("injectedStatusCode", statusCode);
        resp.put("injectedCount", count);
        resp.put("retryAfter", retryAfter);
        resp.put("randomRate", randomRate);
        resp.put("message", "Injected " + count + " errors of HTTP " + statusCode + " on tenant: " + tenantId);
        sendJsonResponse(exchange, 200, resp);
    }

    private void triggerWebhook(String driveId) {
        try {
            HttpClient client = HttpClient.newHttpClient();
            ObjectNode notification = objectMapper.createObjectNode();
            ArrayNode val = notification.putArray("value");
            ObjectNode n = val.addObject();
            n.put("subscriptionId", "mock-sub-" + UUID.randomUUID());
            n.put("clientState", webhookClientState);
            n.put("changeType", "created");
            n.put("resource", "drives/" + driveId + "/root");
            n.put("resourceData", driveId);

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(webhookUrl))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(notification.toString()))
                    .build();

            client.sendAsync(req, HttpResponse.BodyHandlers.ofString());
            log.info("Mock Office 365 emitted real-time webhook notification to [{}] for drive [{}]", webhookUrl, driveId);
        } catch (Exception ex) {
            log.warn("Failed firing mock webhook: {}", ex.getMessage());
        }
    }

    private void sendJsonResponse(HttpExchange exchange, int statusCode, ObjectNode node) throws IOException {
        byte[] bytes = node.toString().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String responseText) throws IOException {
        byte[] bytes = responseText.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    public Map<String, DriveMetadata> getDrivesMetadata() {
        return drivesMetadata;
    }
}
