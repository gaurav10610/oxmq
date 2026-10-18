package io.oxmq.oxguard.mockserver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Enterprise Mock Microsoft 365 Server.
 * Exposes the exact Microsoft Graph API v1.0 contract and serves real physical files from disk.
 */
@Component
@ConditionalOnProperty(name = "oxguard.mock-server.enabled", havingValue = "true", matchIfMissing = true)
public class MockOffice365Server {

    private static final Logger log = LoggerFactory.getLogger(MockOffice365Server.class);

    private final int port;
    private final String storageDirPath;
    private final boolean simulate429Burst;
    private final int totalFilesToSeed;
    private final String webhookUrl;
    private final String webhookClientState;
    private final ObjectMapper objectMapper;

    private HttpServer httpServer;
    private final AtomicBoolean throttledOnce = new AtomicBoolean(false);
    private final Map<String, MockFileInfo> filesInventory = new ConcurrentHashMap<>();
    private final Map<String, List<String>> driveFiles = new ConcurrentHashMap<>();

    public record MockFileInfo(
            String fileId,
            String driveId,
            String name,
            String path,
            long size,
            String mimeType,
            String exposureScope, // "anonymous", "users", "organization", "private"
            String diskFileName,
            long lastModified
    ) {}

    @Autowired
    public MockOffice365Server(
            @Value("${oxguard.mock-server.port:8081}") int port,
            @Value("${oxguard.mock-server.storage-dir:./data/m365-storage}") String storageDirPath,
            @Value("${oxguard.mock-server.simulate-429-burst:false}") boolean simulate429Burst,
            @Value("${oxguard.mock-server.total-files:1000}") int totalFilesToSeed,
            @Value("${oxguard.webhook.notification-url:http://localhost:8080/api/v1/webhooks/graph}") String webhookUrl,
            @Value("${oxguard.webhook.client-state:oxguard-secret-state-token}") String webhookClientState,
            ObjectMapper objectMapper
    ) {
        this.port = port;
        this.storageDirPath = storageDirPath;
        this.simulate429Burst = simulate429Burst;
        this.totalFilesToSeed = totalFilesToSeed;
        this.webhookUrl = webhookUrl;
        this.webhookClientState = webhookClientState;
        this.objectMapper = objectMapper;
    }

    public MockOffice365Server(
            int port,
            String storageDirPath,
            boolean simulate429Burst,
            String webhookUrl,
            String webhookClientState,
            ObjectMapper objectMapper
    ) {
        this(port, storageDirPath, simulate429Burst, 20, webhookUrl, webhookClientState, objectMapper);
    }

    @PostConstruct
    public void start() throws IOException {
        Path storageDir = Paths.get(storageDirPath);
        Files.createDirectories(storageDir);

        seedRealPhysicalFiles(storageDir);
        if (totalFilesToSeed > filesInventory.size()) {
            seedBulkFiles(storageDir, totalFilesToSeed);
        }

        httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        httpServer.setExecutor(Executors.newCachedThreadPool());

        // OAuth2 Token Endpoint
        httpServer.createContext("/contoso-enterprise-tenant/oauth2/v2.0/token", this::handleOAuthToken);
        httpServer.createContext("/oauth2/v2.0/token", this::handleOAuthToken);

        // Microsoft Graph v1.0 Endpoints
        httpServer.createContext("/v1.0/sites/getAllSites", this::handleGetSites);
        httpServer.createContext("/v1.0/sites", this::handleSiteDrives);
        httpServer.createContext("/v1.0/users", this::handleUsersAndDrives);
        httpServer.createContext("/v1.0/drives", this::handleDrives);

        // Binary File Download Endpoint (Direct SAS Storage Simulation)
        httpServer.createContext("/mock-storage/download", this::handleFileDownload);

        // Mock Server Admin API
        httpServer.createContext("/mock-admin/upload-forward-file", this::handleAdminUpload);

        httpServer.start();
        log.info("==========================================================================================");
        log.info("  🌐 MOCK OFFICE 365 ENTERPRISE SERVER STARTED on http://localhost:{}", port);
        log.info("  📁 Real Physical File Storage: {}", storageDir.toAbsolutePath());
        log.info("  📊 Cataloged {} real enterprise files across SharePoint & OneDrives", filesInventory.size());
        log.info("==========================================================================================");
    }

    @PreDestroy
    public void stop() {
        if (httpServer != null) {
            httpServer.stop(0);
            log.info("Mock Office 365 Server stopped.");
        }
    }

    private void seedRealPhysicalFiles(Path storageDir) throws IOException {
        // Seed 1: Real Clean Word Doc
        createPhysicalFile(storageDir, "Corporate_Finance_2026.docx",
                "CONFIDENTIAL - Contoso Financial Strategy 2026.\nRevenue projections and quarterly targets.\nPrepared by CFO.",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "sp-drive-finance", "private");

        // Seed 2: Real Clean PDF Doc
        createPhysicalFile(storageDir, "Employee_Handbook_2026.pdf",
                "%PDF-1.5\n% Contoso Employee Handbook 2026.\n1. Code of conduct.\n2. Security standards.\n%%EOF",
                "application/pdf",
                "sp-drive-hr", "organization");

        // Seed 3: Real Clean Excel Spreadsheet
        createPhysicalFile(storageDir, "Q4_Sales_Pipeline.xlsx",
                "Client,Region,DealSize,Status\nAcme Corp,North America,$1200000,Closed Won\nInitech,EMEA,$450000,In Negotiation",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "sp-drive-finance", "users"); // External guest access

        // Seed 4: Real EICAR Standard Antivirus Test File
        createPhysicalFile(storageDir, "security_audit_eicar.txt",
                "X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*",
                "text/plain",
                "od-drive-ciso", "private");

        // Seed 5: Real High-Entropy Ransomware Payload (Shannon entropy > 7.95)
        byte[] randomEntropyBytes = new byte[16384];
        new SecureRandom().nextBytes(randomEntropyBytes);
        createPhysicalBinaryFile(storageDir, "locked_database_backup.dat.aes",
                randomEntropyBytes,
                "application/octet-stream",
                "sp-drive-it", "private");

        // Seed 6: Real Executable MIME Mismatch File (MZ Windows PE Header disguised as .docx)
        byte[] peHeaderDisguised = new byte[8192];
        peHeaderDisguised[0] = 'M';
        peHeaderDisguised[1] = 'Z';
        peHeaderDisguised[2] = (byte) 0x90;
        peHeaderDisguised[3] = 0x00;
        new SecureRandom().nextBytes(peHeaderDisguised);
        peHeaderDisguised[0] = 'M';
        peHeaderDisguised[1] = 'Z';
        createPhysicalBinaryFile(storageDir, "Executive_Payroll_Statement.docx",
                peHeaderDisguised,
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "sp-drive-finance", "anonymous"); // Public Link!

        // Seed 7: Customer PII CSV
        createPhysicalFile(storageDir, "Customer_PII_Export.csv",
                "id,name,email,ssn\n101,John Doe,john@example.com,123-45-6789\n102,Jane Smith,jane@example.com,987-65-4321",
                "text/csv",
                "od-drive-cfo", "users");

        // Seed 8: Clean Legal Contract
        createPhysicalFile(storageDir, "Master_Services_Agreement_Vendor.pdf",
                "%PDF-1.5\n% Contoso Master Services Agreement.\nNon-disclosure and data protection agreement.\n%%EOF",
                "application/pdf",
                "sp-drive-legal", "users");
    }

    public synchronized void seedAdditionalFiles(int targetTotal) {
        try {
            Path storageDir = Paths.get(storageDirPath);
            Files.createDirectories(storageDir);
            seedBulkFiles(storageDir, targetTotal);
            log.info("Mock Office 365 Server scaled inventory to {} real files on disk", filesInventory.size());
        } catch (IOException e) {
            log.error("Failed seeding additional files: {}", e.getMessage(), e);
        }
    }

    public int getTotalFiles() {
        return filesInventory.size();
    }

    private void seedBulkFiles(Path storageDir, int targetTotal) throws IOException {
        String[] drives = {
                "sp-drive-finance",
                "sp-drive-hr",
                "sp-drive-legal",
                "sp-drive-it",
                "od-drive-ciso",
                "od-drive-cfo",
                "od-drive-lead-architect"
        };

        String[] scopes = {"private", "organization", "users", "anonymous"};
        SecureRandom rng = new SecureRandom();

        int current = filesInventory.size();
        for (int i = current + 1; i <= targetTotal; i++) {
            String driveId = drives[i % drives.length];
            int typeRoll = i % 100;

            String fileName;
            String mimeType;
            String scope = scopes[i % scopes.length];

            if (typeRoll < 3) {
                // ~3% EICAR malware test
                fileName = "security_audit_sample_" + i + ".txt";
                mimeType = "text/plain";
                createPhysicalFile(storageDir, fileName,
                        "X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*",
                        mimeType, driveId, scope);
            } else if (typeRoll < 6) {
                // ~3% High entropy ransomware payload
                fileName = "encrypted_database_backup_" + i + ".dat.aes";
                mimeType = "application/octet-stream";
                byte[] entropyBytes = new byte[8192];
                rng.nextBytes(entropyBytes);
                createPhysicalBinaryFile(storageDir, fileName, entropyBytes, mimeType, driveId, scope);
            } else if (typeRoll < 9) {
                // ~3% Executable MIME Mismatch (PE MZ Header disguised as docx)
                fileName = "Executive_Bonus_Allocation_" + i + ".docx";
                mimeType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
                byte[] peBytes = new byte[4096];
                rng.nextBytes(peBytes);
                peBytes[0] = 'M';
                peBytes[1] = 'Z';
                peBytes[2] = (byte) 0x90;
                peBytes[3] = 0x00;
                createPhysicalBinaryFile(storageDir, fileName, peBytes, mimeType, driveId, scope);
            } else if (typeRoll < 40) {
                // Word Doc
                fileName = "Enterprise_Business_Plan_" + i + ".docx";
                mimeType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
                String content = "Contoso Enterprise Strategic Directive #" + i + ".\nQuarterly operational objectives and deliverables.\nConfidential.";
                createPhysicalFile(storageDir, fileName, content, mimeType, driveId, scope);
            } else if (typeRoll < 70) {
                // PDF Document
                fileName = "Corporate_Governance_Report_" + i + ".pdf";
                mimeType = "application/pdf";
                String content = "%PDF-1.5\n% Contoso Corporate Governance Report #" + i + "\nSection 1: Enterprise Compliance & Risk Management.\n%%EOF";
                createPhysicalFile(storageDir, fileName, content, mimeType, driveId, scope);
            } else if (typeRoll < 90) {
                // Excel Spreadsheet
                fileName = "Departmental_Expenditure_Q" + ((i % 4) + 1) + "_" + i + ".xlsx";
                mimeType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
                String content = "Department,CostCenter,AllocatedBudget,SpentToDate\nEngineering,CC-401,$250000,$180000\nOperations,CC-202,$120000,$95000";
                createPhysicalFile(storageDir, fileName, content, mimeType, driveId, scope);
            } else {
                // CSV Data Table
                fileName = "Client_Ledger_Extract_" + i + ".csv";
                mimeType = "text/csv";
                String content = "id,account,balance,status\n" + i + ",ACCT-" + (10000 + i) + ",$" + (5000 + (i * 12)) + ",ACTIVE";
                createPhysicalFile(storageDir, fileName, content, mimeType, driveId, scope);
            }
        }
    }

    public void saveForwardFile(String driveId, String fileName, String exposureScope, byte[] content, String mimeType) {
        try {
            Path storageDir = Paths.get(storageDirPath);
            Files.createDirectories(storageDir);
            Path targetFile = storageDir.resolve(fileName);
            Files.write(targetFile, content);
            registerFile(fileName, targetFile, mimeType, driveId, exposureScope);
            log.info("Physically wrote forward file to disk: [{}] ({} bytes)", targetFile.toAbsolutePath(), content.length);
        } catch (IOException e) {
            log.error("Failed saving forward file to disk: {}", e.getMessage(), e);
        }
    }

    private void createPhysicalFile(Path dir, String fileName, String content, String mimeType, String driveId, String exposureScope) throws IOException {
        Path filePath = dir.resolve(fileName);
        if (!Files.exists(filePath)) {
            Files.writeString(filePath, content, StandardCharsets.UTF_8);
        }
        registerFile(fileName, filePath, mimeType, driveId, exposureScope);
    }

    private void createPhysicalBinaryFile(Path dir, String fileName, byte[] content, String mimeType, String driveId, String exposureScope) throws IOException {
        Path filePath = dir.resolve(fileName);
        if (!Files.exists(filePath)) {
            Files.write(filePath, content);
        }
        registerFile(fileName, filePath, mimeType, driveId, exposureScope);
    }

    private void registerFile(String fileName, Path filePath, String mimeType, String driveId, String exposureScope) {
        String fileId = "file-" + UUID.nameUUIDFromBytes((driveId + ":" + fileName).getBytes(StandardCharsets.UTF_8)).toString().replace("-", "").substring(0, 12);
        long size = filePath.toFile().length();
        MockFileInfo info = new MockFileInfo(
                fileId, driveId, fileName, "/Documents/" + fileName, size, mimeType, exposureScope,
                filePath.getFileName().toString(), System.currentTimeMillis()
        );
        filesInventory.put(fileId, info);
        List<String> list = driveFiles.computeIfAbsent(driveId, k -> Collections.synchronizedList(new ArrayList<>()));
        if (!list.contains(fileId)) {
            list.add(fileId);
        }
    }

    // --- HTTP HANDLERS ---

    private void handleOAuthToken(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendResponse(exchange, 405, "Method Not Allowed");
            return;
        }
        ObjectNode tokenNode = objectMapper.createObjectNode();
        tokenNode.put("token_type", "Bearer");
        tokenNode.put("expires_in", 3600);
        tokenNode.put("access_token", "mock-azure-ad-jwt-token-" + System.currentTimeMillis());
        sendJsonResponse(exchange, 200, tokenNode);
    }

    private void handleGetSites(HttpExchange exchange) throws IOException {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("@odata.context", "https://graph.microsoft.com/v1.0/$metadata#sites");
        ArrayNode value = root.putArray("value");

        addSiteNode(value, "contoso.sharepoint.com,site-finance,web-finance", "Finance & Accounting", "https://contoso.sharepoint.com/sites/Finance");
        addSiteNode(value, "contoso.sharepoint.com,site-hr,web-hr", "Human Resources Portal", "https://contoso.sharepoint.com/sites/HR");
        addSiteNode(value, "contoso.sharepoint.com,site-legal,web-legal", "Legal & Compliance", "https://contoso.sharepoint.com/sites/Legal");
        addSiteNode(value, "contoso.sharepoint.com,site-it,web-it", "IT Engineering & Cloud", "https://contoso.sharepoint.com/sites/IT");

        sendJsonResponse(exchange, 200, root);
    }

    private void addSiteNode(ArrayNode array, String id, String name, String url) {
        ObjectNode site = array.addObject();
        site.put("id", id);
        site.put("name", name);
        site.put("webUrl", url);
    }

    private void handleSiteDrives(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        // Path format: /v1.0/sites/{siteId}/drives
        ObjectNode root = objectMapper.createObjectNode();
        ArrayNode value = root.putArray("value");

        String driveId = "sp-drive-finance";
        String driveName = "Finance Documents";
        if (path.contains("site-hr")) {
            driveId = "sp-drive-hr";
            driveName = "HR Policies";
        } else if (path.contains("site-legal")) {
            driveId = "sp-drive-legal";
            driveName = "Contracts Library";
        } else if (path.contains("site-it")) {
            driveId = "sp-drive-it";
            driveName = "Engineering Assets";
        }

        ObjectNode drive = value.addObject();
        drive.put("id", driveId);
        drive.put("name", driveName);
        drive.put("driveType", "documentLibrary");

        sendJsonResponse(exchange, 200, root);
    }

    private void handleUsersAndDrives(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (path.endsWith("/drive")) {
            // Path: /v1.0/users/{userId}/drive
            String userId = path.replace("/v1.0/users/", "").replace("/drive", "");
            ObjectNode drive = objectMapper.createObjectNode();
            drive.put("id", "od-drive-" + userId);
            drive.put("name", userId + "@contoso.com OneDrive");
            drive.put("driveType", "business");
            sendJsonResponse(exchange, 200, drive);
            return;
        }

        // Path: /v1.0/users
        ObjectNode root = objectMapper.createObjectNode();
        ArrayNode users = root.putArray("value");

        addUserNode(users, "ciso", "ciso@contoso.com");
        addUserNode(users, "cfo", "cfo@contoso.com");
        addUserNode(users, "lead-architect", "architect@contoso.com");

        sendJsonResponse(exchange, 200, root);
    }

    private void addUserNode(ArrayNode array, String id, String upn) {
        ObjectNode u = array.addObject();
        u.put("id", id);
        u.put("userPrincipalName", upn);
    }

    private void handleDrives(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();

        // 1. Delta query: /v1.0/drives/{driveId}/root/delta
        if (path.contains("/root/delta")) {
            String[] parts = path.split("/");
            String driveId = parts[3];

            ObjectNode root = objectMapper.createObjectNode();
            ArrayNode value = root.putArray("value");

            List<String> fileIds = driveFiles.getOrDefault(driveId, Collections.emptyList());
            for (String fid : fileIds) {
                MockFileInfo info = filesInventory.get(fid);
                if (info != null) {
                    ObjectNode item = value.addObject();
                    item.put("id", info.fileId());
                    item.put("name", info.name());
                    item.put("size", info.size());
                    item.put("webUrl", "https://contoso.sharepoint.com/documents/" + info.name());
                    item.put("@microsoft.graph.downloadUrl", "http://localhost:" + port + "/mock-storage/download/" + info.fileId());
                    item.put("eTag", "etag-" + info.lastModified());
                    item.put("cTag", "ctag-" + info.lastModified());

                    ObjectNode fileObj = item.putObject("file");
                    fileObj.put("mimeType", info.mimeType());

                    ObjectNode parentRef = item.putObject("parentReference");
                    parentRef.put("path", "/drives/" + driveId + "/root:");
                }
            }

            root.put("@odata.deltaLink", "http://localhost:" + port + "/v1.0/drives/" + driveId + "/root/delta?token=" + System.currentTimeMillis());
            sendJsonResponse(exchange, 200, root);
            return;
        }

        // 2. Permissions query: /v1.0/drives/{driveId}/items/{itemId}/permissions
        if (path.contains("/permissions")) {
            // Check Approach 5 Rate Limiting simulation
            if (simulate429Burst && throttledOnce.compareAndSet(false, true)) {
                exchange.getResponseHeaders().set("Retry-After", "3");
                sendResponse(exchange, 429, "{\"error\": {\"code\": \"activityLimitReached\", \"message\": \"Throttled by Mock Graph\"}}");
                return;
            }

            String[] parts = path.split("/");
            String itemId = parts[5];
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
                    user.put("email", "owner@contoso.com");
                }
            }

            sendJsonResponse(exchange, 200, root);
            return;
        }

        sendResponse(exchange, 404, "Not Found");
    }

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
            String driveId = json.has("driveId") ? json.get("driveId").asText() : "sp-drive-finance";
            String exposure = json.has("exposure") ? json.get("exposure").asText() : "anonymous";
            String mimeType = json.has("mimeType") ? json.get("mimeType").asText() : "application/vnd.openxmlformats-officedocument.wordprocessingml.document";

            Path storageDir = Paths.get(storageDirPath);
            Path targetFile = storageDir.resolve(fileName);
            Files.writeString(targetFile, content, StandardCharsets.UTF_8);

            registerFile(fileName, targetFile, mimeType, driveId, exposure);

            // Send webhook notification to the OxGuard webhook controller
            triggerWebhook(driveId);

            ObjectNode resp = objectMapper.createObjectNode();
            resp.put("success", true);
            resp.put("fileName", fileName);
            resp.put("driveId", driveId);
            resp.put("bytesWritten", targetFile.toFile().length());
            sendJsonResponse(exchange, 200, resp);
        } catch (Exception e) {
            sendResponse(exchange, 500, "Upload error: " + e.getMessage());
        }
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
}
