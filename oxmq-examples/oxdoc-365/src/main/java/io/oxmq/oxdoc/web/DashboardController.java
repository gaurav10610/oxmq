package io.oxmq.oxdoc.web;

import io.oxmq.model.BackoffStrategy;
import io.oxmq.model.DeduplicationOptions;
import io.oxmq.model.JobOptions;
import io.oxmq.oxdoc.connector.MockAdminClient;
import io.oxmq.oxdoc.connector.MockGraphClient;
import io.oxmq.oxdoc.model.AccessScope;
import io.oxmq.oxdoc.model.DriveDeltaSyncPayload;
import io.oxmq.oxdoc.model.IndexedDocumentRecord;
import io.oxmq.oxdoc.model.ScanOrigin;
import io.oxmq.oxdoc.model.TenantDiscoveryPayload;
import io.oxmq.oxdoc.model.TenantProfile;
import io.oxmq.oxdoc.model.TenantTier;
import io.oxmq.oxdoc.queue.OxDocQueueConstants;
import io.oxmq.oxdoc.service.DocumentCatalogRepository;
import io.oxmq.oxdoc.service.LiveMetricsService;
import io.oxmq.oxdoc.service.TenantCooldownService;
import io.oxmq.oxdoc.service.TenantSubscriptionService;
import io.oxmq.spring.OxmqTemplate;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * REST API Controller for OxDoc 365 Content Intelligence Dashboard and multi-tenant operations.
 */
@RestController
@RequestMapping("/api/v1/dashboard")
public class DashboardController {

    private static final Logger log = LoggerFactory.getLogger(DashboardController.class);

    private final DocumentCatalogRepository repository;
    private final LiveMetricsService metricsService;
    private final OxmqTemplate oxmqTemplate;
    private final TenantCooldownService cooldownService;
    private final MockAdminClient mockAdminClient;
    private final TenantSubscriptionService tenantSubscriptionService;

    @Autowired(required = false)
    private MockGraphClient mockGraphClient;

    public DashboardController(
            DocumentCatalogRepository repository,
            LiveMetricsService metricsService,
            OxmqTemplate oxmqTemplate,
            TenantCooldownService cooldownService,
            MockAdminClient mockAdminClient,
            TenantSubscriptionService tenantSubscriptionService
    ) {
        this.repository = repository;
        this.metricsService = metricsService;
        this.oxmqTemplate = oxmqTemplate;
        this.cooldownService = cooldownService;
        this.mockAdminClient = mockAdminClient;
        this.tenantSubscriptionService = tenantSubscriptionService;
    }

    /**
     * Lists all 5 onboarded enterprise tenants, license tiers, and adaptive tuning state.
     */
    @GetMapping("/tenants")
    public ResponseEntity<List<Map<String, Object>>> getTenants() {
        List<Map<String, Object>> list = new ArrayList<>();
        for (TenantProfile p : tenantSubscriptionService.getAllTenants()) {
            Map<String, Object> t = new LinkedHashMap<>();
            t.put("tenantId", p.tenantId());
            t.put("organizationName", p.organizationName());
            t.put("tier", p.tier().name());
            t.put("tierDisplayName", p.tier().getDisplayName());
            t.put("seatCount", p.seatCount());
            t.put("dailyRuBudget", p.dailyRuBudget());
            t.put("burstLimitRUsPerSec", cooldownService.getTenantBurstLimit(p.tenantId()));
            t.put("tierSource", p.tierSource().name());
            t.put("isOverridePinned", p.isOverridePinned());
            t.put("upgradeCount", p.upgradeCount());
            t.put("downgradeCount", p.downgradeCount());
            t.put("connected", true);
            list.add(t);
        }
        return ResponseEntity.ok(list);
    }

    /**
     * Exposes platform and JVM system metrics.
     */
    @GetMapping("/system-metrics")
    public ResponseEntity<Map<String, Object>> getSystemMetrics() {
        Runtime rt = Runtime.getRuntime();
        long totalMem = rt.totalMemory();
        long freeMem = rt.freeMemory();
        long maxMem = rt.maxMemory();
        long usedMem = totalMem - freeMem;

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("jvmUsedMemoryBytes", usedMem);
        map.put("jvmUsedMemoryMB", Math.round(usedMem / (1024.0 * 1024.0) * 10.0) / 10.0);
        map.put("jvmTotalMemoryMB", Math.round(totalMem / (1024.0 * 1024.0) * 10.0) / 10.0);
        map.put("jvmMaxMemoryMB", Math.round(maxMem / (1024.0 * 1024.0) * 10.0) / 10.0);
        map.put("activeThreadCount", Thread.activeCount());
        map.put("javaVersion", System.getProperty("java.version"));
        map.put("osName", System.getProperty("os.name"));

        Map<String, Object> metrics = repository.getMetrics();
        map.put("totalFilesScanned", metrics.get("totalFiles"));
        map.put("totalDocumentsIndexed", metrics.get("totalDocuments"));
        map.put("totalDrivesMonitored", metrics.get("totalDrives"));
        return ResponseEntity.ok(map);
    }

    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> getMetrics(
            @RequestParam(value = "tenantId", required = false) String tenantId
    ) {
        return ResponseEntity.ok(repository.getMetrics(tenantId));
    }

    @GetMapping("/quota")
    public ResponseEntity<Map<String, Object>> getQuotaTelemetry(
            @RequestParam(value = "tenantId", required = false) String tenantId
    ) {
        if (tenantId == null || tenantId.isBlank() || "all".equalsIgnoreCase(tenantId)) {
            return ResponseEntity.ok(cooldownService.getAllTenantsQuotaTelemetry());
        }
        return ResponseEntity.ok(cooldownService.getQuotaTelemetry(tenantId));
    }

    @PostMapping("/ratelimit")
    public ResponseEntity<Map<String, Object>> updateRateLimit(
            @RequestParam(value = "tenantId", required = false) String tenantId,
            @RequestParam(value = "limit", defaultValue = "50") int limit
    ) {
        if (tenantId != null && !tenantId.isBlank() && !"all".equalsIgnoreCase(tenantId)) {
            cooldownService.setMaxRequestsPerSecond(tenantId, limit);
            mockAdminClient.setRateLimit(tenantId, limit);
            return ResponseEntity.ok(Map.of(
                    "status", "UPDATED",
                    "tenantId", tenantId,
                    "maxRequestsPerSecond", limit,
                    "message", "Rate limit adjusted for " + tenantId + " to " + limit + " req/s"
            ));
        } else {
            cooldownService.setMaxRequestsPerSecond(limit);
            mockAdminClient.setGlobalRateLimit(limit);
            return ResponseEntity.ok(Map.of(
                    "status", "UPDATED_ALL",
                    "maxRequestsPerSecond", limit,
                    "message", "Rate limit adjusted globally to " + limit + " req/s"
            ));
        }
    }

    @PostMapping("/simulate-429")
    public ResponseEntity<Map<String, Object>> simulate429Burst(
            @RequestParam(value = "tenantId", defaultValue = "tenant-smb-250") String tenantId,
            @RequestParam(value = "enabled", defaultValue = "true") boolean enabled,
            @RequestParam(value = "burstCount", defaultValue = "20") int burstCount
    ) {
        boolean ok = mockAdminClient.trigger429Burst(tenantId, burstCount, enabled);
        if (ok) {
            return ResponseEntity.ok(Map.of(
                    "status", "TRIGGERED",
                    "tenantId", tenantId,
                    "burstCount", burstCount,
                    "enabled", enabled,
                    "message", "Triggered " + burstCount + " HTTP 429 burst responses on tenant: " + tenantId
            ));
        }
        return ResponseEntity.badRequest().body(Map.of("error", "Mock Office 365 Server not reachable on port 8081"));
    }

    @GetMapping("/files")
    public ResponseEntity<List<IndexedDocumentRecord>> getFiles(
            @RequestParam(value = "tenantId", required = false) String tenantId,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "verdict", required = false) String verdict,
            @RequestParam(value = "origin", required = false) String origin
    ) {
        List<IndexedDocumentRecord> files = repository.getDocumentsByTenant(tenantId);

        if (scope != null && !scope.isBlank()) {
            files = files.stream()
                    .filter(f -> f.accessScope().name().equalsIgnoreCase(scope))
                    .toList();
        }

        if (verdict != null && !verdict.isBlank()) {
            files = files.stream()
                    .filter(f -> f.indexingVerdict().name().equalsIgnoreCase(verdict))
                    .toList();
        }

        if (origin != null && !origin.isBlank()) {
            files = files.stream()
                    .filter(f -> f.scanOrigin().name().equalsIgnoreCase(origin))
                    .toList();
        }

        return ResponseEntity.ok(files);
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamEvents() {
        return metricsService.subscribe();
    }

    @GetMapping(value = "/sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamSse() {
        return metricsService.subscribe();
    }

    @PostMapping("/seed")
    public ResponseEntity<Map<String, Object>> seedAdditionalFiles(
            @RequestParam(value = "count", defaultValue = "5000") int count
    ) {
        boolean ok = mockAdminClient.seedAdditionalFiles(count);
        if (ok) {
            Map<String, Object> stats = mockAdminClient.getStats();
            return ResponseEntity.ok(Map.of(
                    "status", "SEEDED",
                    "totalFiles", stats.getOrDefault("totalFiles", count),
                    "message", "Successfully seeded files up to " + count
            ));
        }
        return ResponseEntity.badRequest().body(Map.of("error", "Mock Office 365 Server is not reachable on port 8081"));
    }

    /**
     * Triggers a historical backward baseline crawl for a specific tenant.
     */
    @PostMapping("/trigger/crawl")
    public ResponseEntity<Map<String, Object>> triggerBaselineCrawl(
            @RequestParam(value = "tenantId", defaultValue = "tenant-mid-2500") String tenantId
    ) {
        log.info("Manually triggering tenant discovery and backward crawl for [{}]", tenantId);

        JobOptions options = JobOptions.builder()
                .priority(5)
                .attempts(3)
                .backoff(new BackoffStrategy.Exponential(1000L))
                .build();

        oxmqTemplate.send(
                OxDocQueueConstants.QUEUE_TENANT_DISCOVERY,
                "discovery-" + tenantId + "-" + System.currentTimeMillis(),
                new TenantDiscoveryPayload(tenantId),
                options
        );

        return ResponseEntity.ok(Map.of(
                "status", "DISPATCHED",
                "tenantId", tenantId,
                "message", "Baseline historical discovery job queued for tenant: " + tenantId,
                "timestamp", System.currentTimeMillis()
        ));
    }

    /**
     * Triggers concurrent baseline crawl across ALL 5 official Microsoft 365 tenant accounts!
     */
    @PostMapping("/trigger/crawl-all")
    public ResponseEntity<Map<String, Object>> triggerCrawlAll() {
        log.info("Triggering concurrent baseline crawl across all official Microsoft 365 tenant accounts");
        List<String> dispatched = new ArrayList<>();
        for (TenantProfile profile : tenantSubscriptionService.getAllTenants()) {
            JobOptions options = JobOptions.builder()
                    .priority(5)
                    .attempts(3)
                    .backoff(new BackoffStrategy.Exponential(1000L))
                    .build();

            oxmqTemplate.send(
                    OxDocQueueConstants.QUEUE_TENANT_DISCOVERY,
                    "discovery-" + profile.tenantId() + "-" + System.currentTimeMillis(),
                    new TenantDiscoveryPayload(profile.tenantId()),
                    options
            );
            dispatched.add(profile.tenantId());
        }

        return ResponseEntity.ok(Map.of(
                "status", "ALL_DISPATCHED",
                "tenants", dispatched,
                "totalTenants", dispatched.size(),
                "message", "Dispatched concurrent discovery across all 5 enterprise tenant accounts (5,000 files)"
        ));
    }

    public ResponseEntity<Map<String, Object>> simulateForwardUpload(
            String driveId,
            String fileName,
            String scope,
            String docFormat
    ) {
        return simulateForwardUpload("tenant-mid-2500", driveId, fileName, scope, docFormat);
    }

    /**
     * Simulates a real-time Forward upload (Priority 1) with auto-save debouncing.
     */
    @PostMapping("/trigger/upload")
    public ResponseEntity<Map<String, Object>> simulateForwardUpload(
            @RequestParam(value = "tenantId", defaultValue = "tenant-mid-2500") String tenantId,
            @RequestParam(value = "driveId", defaultValue = "drive-mid-sp-logistics") String driveId,
            @RequestParam(value = "fileName", defaultValue = "Quarterly_Strategic_Roadmap.docx") String fileName,
            @RequestParam(value = "scope", defaultValue = "ORGANIZATION_WIDE") String scope,
            @RequestParam(value = "docFormat", defaultValue = "DOCUMENT") String docFormat
    ) {
        log.info("Simulating real-time forward document upload for tenant [{}]: file=[{}], drive=[{}], scope=[{}], docFormat=[{}]",
                tenantId, fileName, driveId, scope, docFormat);

        byte[] fileBytes;
        if ("SPREADSHEET".equalsIgnoreCase(docFormat)) {
            fileBytes = "Category,Quarter,Budget,Actual\nAI_Infrastructure,Q1,1200000,950000\nCloud_Compute,Q1,850000,810000".getBytes(StandardCharsets.UTF_8);
            if (!fileName.endsWith(".xlsx") && !fileName.endsWith(".csv")) fileName += ".xlsx";
        } else if ("DATASET".equalsIgnoreCase(docFormat)) {
            fileBytes = new byte[8192];
            new java.util.Random().nextBytes(fileBytes);
            if (!fileName.endsWith(".bin")) fileName += ".bin";
        } else {
            fileBytes = ("OxDoc 365 Enterprise Content Intelligence Document: " + fileName + "\nGenerated at: " + System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8);
            if (!fileName.endsWith(".docx") && !fileName.endsWith(".pdf") && !fileName.endsWith(".txt")) fileName += ".docx";
        }

        AccessScope accessScope;
        try {
            accessScope = AccessScope.valueOf(scope.toUpperCase());
        } catch (Exception e) {
            accessScope = AccessScope.ORGANIZATION_WIDE;
        }

        if (mockGraphClient != null) {
            mockGraphClient.addMockForwardFile(driveId, fileName, accessScope, fileBytes);
        }
        String scopeStr = "organization";
        if (accessScope == AccessScope.PUBLIC_ANONYMOUS) scopeStr = "anonymous";
        else if (accessScope == AccessScope.EXTERNAL_GUEST) scopeStr = "users";
        else if (accessScope == AccessScope.PRIVATE) scopeStr = "private";
        mockAdminClient.uploadForwardFile(driveId, fileName, scopeStr, fileBytes, "application/octet-stream");

        JobOptions options = JobOptions.builder()
                .priority(1)
                .attempts(3)
                .deduplication(DeduplicationOptions.builder()
                                .id("forward-upload-" + driveId)
                                .ttl(Duration.ofSeconds(15))
                                .keepLastIfActive(true)
                                .build())
                .build();

        DriveDeltaSyncPayload payload = new DriveDeltaSyncPayload(tenantId, driveId, ScanOrigin.FORWARD);

        oxmqTemplate.send(
                OxDocQueueConstants.QUEUE_DRIVE_DELTA,
                "forward-event-" + driveId + "-" + System.currentTimeMillis(),
                payload,
                options
        );

        return ResponseEntity.ok(Map.of(
                "status", "ENQUEUED",
                "tenantId", tenantId,
                "message", "Simulated forward file upload queued with Priority 1 and 15s debounce",
                "fileName", fileName,
                "scope", accessScope.name()
        ));
    }

    /**
     * Pins a static tier override on a tenant account, bypassing dynamic discovery and adaptive tuning.
     */
    @PostMapping("/tenants/{tenantId}/override-tier")
    public ResponseEntity<Map<String, Object>> setTenantOverride(
            @PathVariable String tenantId,
            @RequestParam String tier
    ) {
        log.info("API: Setting static tier override [{}] on tenant [{}]", tier, tenantId);
        TenantTier targetTier = TenantTier.valueOf(tier.trim().toUpperCase());
        TenantProfile updated = tenantSubscriptionService.setStaticOverride(tenantId, targetTier);
        return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "tenantId", tenantId,
                "tier", updated.tier().name(),
                "burstLimitRUsPerSec", updated.burstLimitRUsPerSec(),
                "isOverridePinned", updated.isOverridePinned(),
                "tierSource", updated.tierSource().name()
        ));
    }

    /**
     * Clears a static tier override on a tenant account, re-enabling dynamic discovery and adaptive tuning.
     */
    @DeleteMapping("/tenants/{tenantId}/override-tier")
    public ResponseEntity<Map<String, Object>> clearTenantOverride(@PathVariable String tenantId) {
        log.info("API: Clearing static tier override on tenant [{}]", tenantId);
        TenantProfile updated = tenantSubscriptionService.clearStaticOverride(tenantId);
        return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "tenantId", tenantId,
                "tier", updated.tier().name(),
                "burstLimitRUsPerSec", updated.burstLimitRUsPerSec(),
                "isOverridePinned", updated.isOverridePinned(),
                "tierSource", updated.tierSource().name()
        ));
    }

    /**
     * Manually triggers an adaptive tier evaluation cycle for a tenant account.
     */
    @PostMapping("/tenants/{tenantId}/evaluate-tier")
    public ResponseEntity<Map<String, Object>> evaluateTenantTier(@PathVariable String tenantId) {
        log.info("API: Evaluating adaptive tier tuning for tenant [{}]", tenantId);
        TenantProfile updated = tenantSubscriptionService.evaluateTenantTier(tenantId);
        return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "tenantId", tenantId,
                "tier", updated.tier().name(),
                "burstLimitRUsPerSec", updated.burstLimitRUsPerSec(),
                "tierSource", updated.tierSource().name(),
                "isOverridePinned", updated.isOverridePinned(),
                "upgradeCount", updated.upgradeCount(),
                "downgradeCount", updated.downgradeCount(),
                "consecutiveCleanWindows", tenantSubscriptionService.getConsecutiveCleanWindows(tenantId)
        ));
    }
}
