package io.oxmq.oxdoc.connector;

import io.oxmq.oxdoc.model.AccessScope;
import io.oxmq.oxdoc.model.ContentVisibility;
import io.oxmq.oxdoc.model.DriveItem;
import io.oxmq.oxdoc.model.DriveType;
import io.oxmq.oxdoc.model.MonitoredDrive;
import io.oxmq.oxdoc.model.PermissionEntry;
import io.oxmq.oxdoc.model.TenantProfile;
import io.oxmq.oxdoc.model.TenantTier;
import io.oxmq.oxdoc.model.TierSource;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * High-fidelity in-memory Microsoft Graph API simulator.
 * Allows 100% out-of-the-box local testing without requiring Azure credentials or internet connection.
 */
@Component
@ConditionalOnProperty(name = "oxdoc.client.mode", havingValue = "MOCK", matchIfMissing = true)
public class MockGraphClient implements GraphClient {

    private static final Logger log = LoggerFactory.getLogger(MockGraphClient.class);

    private final boolean simulate429Burst;
    private final AtomicInteger requestCounter = new AtomicInteger(0);

    // In-memory data store for simulated drives and items
    private final Map<String, MonitoredDrive> drives = new ConcurrentHashMap<>();
    private final Map<String, List<DriveItem>> driveItems = new ConcurrentHashMap<>();
    private final Map<String, ContentVisibility> itemPermissions = new ConcurrentHashMap<>();
    private final Map<String, byte[]> itemByteContents = new ConcurrentHashMap<>();

    public MockGraphClient(
            @Value("${oxdoc.mock.total-sites:5}") int totalSites,
            @Value("${oxdoc.mock.total-users:15}") int totalUsers,
            @Value("${oxdoc.mock.files-per-drive:30}") int filesPerDrive,
            @Value("${oxdoc.mock.simulate-429-burst:false}") boolean simulate429Burst
    ) {
        this.simulate429Burst = simulate429Burst;
        initializeMockTenant(totalSites, totalUsers, filesPerDrive);
    }

    private void initializeMockTenant(int totalSites, int totalUsers, int filesPerDrive) {
        log.info("Initializing Mock Graph Client with {} SharePoint sites, {} user OneDrives, and {} files per drive...",
                totalSites, totalUsers, filesPerDrive);

        String tenantId = "contoso-enterprise-tenant";
        String[] siteNames = {"Finance_Team_Docs", "Legal_Compliance_Vault", "Engineering_Core", "Executive_Briefings", "HR_Global_Assets"};

        for (int i = 0; i < totalSites; i++) {
            String name = (i < siteNames.length) ? siteNames[i] : "Site_Collection_" + (i + 1);
            String driveId = "sp-drive-" + (i + 1);
            MonitoredDrive drive = new MonitoredDrive(driveId, tenantId, DriveType.SHAREPOINT_LIBRARY, name, "site-admin@contoso.com", "delta-v1-0", null);
            drives.put(driveId, drive);
            populateDriveFiles(driveId, filesPerDrive);
        }

        String[] userNames = {"ceo", "cfo", "cto", "rnd-lead", "lead-architect", "platform-engineer", "hr-director", "finance-analyst", "legal-counsel", "product-mgr"};
        for (int i = 0; i < totalUsers; i++) {
            String alias = (i < userNames.length) ? userNames[i] : "user" + (i + 1);
            String email = alias + "@contoso.com";
            String driveId = "od-drive-" + alias;
            MonitoredDrive drive = new MonitoredDrive(driveId, tenantId, DriveType.USER_ONEDRIVE, alias + "'s Personal OneDrive", email, "delta-v1-0", null);
            drives.put(driveId, drive);
            populateDriveFiles(driveId, filesPerDrive);
        }

        log.info("Mock Graph initialized: total {} drives, {} files cataloged.", drives.size(), itemByteContents.size());
    }

    private void populateDriveFiles(String driveId, int count) {
        List<DriveItem> items = new ArrayList<>();

        for (int i = 1; i <= count; i++) {
            String itemId = driveId + "-file-" + i;
            String fileName;
            byte[] content;
            AccessScope accessScope;
            boolean hasUniquePermissions = (i % 3 == 0);

            if (i == 1) {
                // Publicly exposed financial sheet
                fileName = "Q3_Consolidated_Budget.xlsx";
                content = generateTextContent("Quarterly financial budget for Contoso Corp. Highly confidential.", 4096);
                accessScope = AccessScope.PUBLIC_ANONYMOUS;
            } else if (i == 2) {
                // External guest vendor contract
                fileName = "Vendor_Partnership_Agreement.docx";
                content = generateTextContent("Vendor Service Level Agreement reviewed by external partners.", 8192);
                accessScope = AccessScope.EXTERNAL_GUEST;
            } else if (i == 3) {
                // Org-wide accessible handbook
                fileName = "Enterprise_Architecture_Handbook.pdf";
                content = generateTextContent("Architecture Blueprints and Best Practices for all engineering teams.", 12000);
                accessScope = AccessScope.ORGANIZATION_WIDE;
            } else if (i == 4) {
                // Technical Knowledge Base Article
                fileName = "Knowledge_Base_System_Design.txt";
                content = generateTextContent("OxDoc 365 Enterprise Knowledge Base Article: High throughput document processing and indexing system.", 4096);
                accessScope = AccessScope.PRIVATE;
            } else if (i == 5) {
                // Dataset Archive
                fileName = "analytics_dataset_archive.bin";
                content = generateHighEntropyContent(16384);
                accessScope = AccessScope.INTERNAL_RESTRICTED;
            } else if (i == 6) {
                // System Architecture Specification Document
                fileName = "Architecture_System_Blueprint.docx";
                content = generateTextContent("Architecture Blueprint for Multi-Cloud Indexing Fabric.", 8192);
                accessScope = AccessScope.EXTERNAL_GUEST;
            } else {
                // Clean standard business files
                fileName = "Project_Specification_" + i + ".docx";
                content = generateTextContent("Normal enterprise document content for project roadmap #" + i, 2048 + (i * 256));
                accessScope = (i % 5 == 0) ? AccessScope.INTERNAL_RESTRICTED : AccessScope.PRIVATE;
            }

            DriveItem item = new DriveItem(
                    itemId,
                    driveId,
                    fileName,
                    "/Documents/" + fileName,
                    content.length,
                    resolveMimeType(fileName),
                    "https://contoso.sharepoint.com/drives/" + driveId + "/items/" + itemId,
                    "https://contoso.blob.core.windows.net/sas-download/" + itemId + "?token=sas123",
                    false,
                    "etag-" + itemId,
                    "ctag-" + itemId
            );

            items.add(item);
            itemByteContents.put(itemId, content);

            // Generate realistic ACL
            List<PermissionEntry> permissions = new ArrayList<>();
            permissions.add(new PermissionEntry(UUID.randomUUID().toString(), "owner@contoso.com", "owner", false, null, null));
            if (accessScope == AccessScope.PUBLIC_ANONYMOUS) {
                permissions.add(new PermissionEntry(UUID.randomUUID().toString(), null, "read", !hasUniquePermissions, hasUniquePermissions ? null : "parent-folder-id", "anonymous"));
            } else if (accessScope == AccessScope.EXTERNAL_GUEST) {
                permissions.add(new PermissionEntry(UUID.randomUUID().toString(), "vendor-partner@external.com", "read", !hasUniquePermissions, hasUniquePermissions ? null : "parent-folder-id", "users"));
            } else if (accessScope == AccessScope.ORGANIZATION_WIDE) {
                permissions.add(new PermissionEntry(UUID.randomUUID().toString(), "All Authenticated Users", "read", !hasUniquePermissions, hasUniquePermissions ? null : "parent-folder-id", "organization"));
            }
            itemPermissions.put(itemId, new ContentVisibility(accessScope, hasUniquePermissions, permissions));
        }

        driveItems.put(driveId, items);
    }

    private void checkSimulated429() {
        if (simulate429Burst) {
            int count = requestCounter.incrementAndGet();
            if (count % 8 == 0) { // Every 8th request simulates an HTTP 429
                log.warn("Mock Graph Client simulating HTTP 429 (Retry-After: 4s) to test dynamic deferral!");
                throw new GraphThrottlingException(4, "tenant-mid-2500", "Rate limit exceeded (Microsoft Graph simulated 429)");
            }
        }
    }

    @Override
    public List<MonitoredDrive> discoverSharePointDrives(String tenantId) {
        checkSimulated429();
        List<MonitoredDrive> result = new ArrayList<>();
        for (MonitoredDrive drive : drives.values()) {
            if (drive.driveType() == DriveType.SHAREPOINT_LIBRARY) {
                result.add(drive);
            }
        }
        return result;
    }

    @Override
    public List<MonitoredDrive> discoverUserOneDrives(String tenantId) {
        checkSimulated429();
        List<MonitoredDrive> result = new ArrayList<>();
        for (MonitoredDrive drive : drives.values()) {
            if (drive.driveType() == DriveType.USER_ONEDRIVE) {
                result.add(drive);
            }
        }
        return result;
    }

    @Override
    public DeltaSyncResult fetchDeltaChanges(String driveId, String deltaToken) {
        checkSimulated429();
        List<DriveItem> items = driveItems.getOrDefault(driveId, Collections.emptyList());
        String nextToken = "delta-token-" + System.currentTimeMillis();
        return new DeltaSyncResult(new ArrayList<>(items), Collections.emptyList(), nextToken);
    }

    @Override
    public ContentVisibility fetchPermissions(String driveId, String itemId) {
        checkSimulated429();
        ContentVisibility visibility = itemPermissions.get(itemId);
        if (visibility != null) {
            return visibility;
        }
        return new ContentVisibility(AccessScope.PRIVATE, false, List.of(
                new PermissionEntry(UUID.randomUUID().toString(), "owner@contoso.com", "owner", false, null, null)
        ));
    }

    @Override
    public InputStream streamFileContent(String driveId, String itemId, String directDownloadUrl) {
        byte[] bytes = itemByteContents.get(itemId);
        if (bytes == null) {
            bytes = "Mock default content".getBytes(StandardCharsets.UTF_8);
        }
        return new ByteArrayInputStream(bytes);
    }

    public DriveItem addMockForwardFile(String driveId, String fileName, AccessScope accessScope, byte[] content) {
        String itemId = "forward-" + System.currentTimeMillis() + "-" + UUID.randomUUID().toString().substring(0, 6);
        DriveItem item = new DriveItem(
                itemId,
                driveId,
                fileName,
                "/Uploads/" + fileName,
                content.length,
                resolveMimeType(fileName),
                "https://contoso.sharepoint.com/drives/" + driveId + "/items/" + itemId,
                "https://contoso.blob.core.windows.net/sas-download/" + itemId + "?token=sas123",
                false,
                "etag-fwd-" + itemId,
                "ctag-fwd-" + itemId
        );

        driveItems.computeIfAbsent(driveId, k -> new ArrayList<>()).add(item);
        itemByteContents.put(itemId, content);

        List<PermissionEntry> permissions = new ArrayList<>();
        permissions.add(new PermissionEntry(UUID.randomUUID().toString(), "owner@contoso.com", "owner", false, null, null));
        if (accessScope == AccessScope.PUBLIC_ANONYMOUS) {
            permissions.add(new PermissionEntry(UUID.randomUUID().toString(), null, "read", true, null, "anonymous"));
        } else if (accessScope == AccessScope.EXTERNAL_GUEST) {
            permissions.add(new PermissionEntry(UUID.randomUUID().toString(), "guest@partner.com", "read", true, null, "users"));
        } else if (accessScope == AccessScope.ORGANIZATION_WIDE) {
            permissions.add(new PermissionEntry(UUID.randomUUID().toString(), "All Authenticated Users", "read", false, "parent-folder", "organization"));
        }
        itemPermissions.put(itemId, new ContentVisibility(accessScope, true, permissions));

        return item;
    }

    private byte[] generateTextContent(String prefix, int size) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < size) {
            sb.append(prefix).append(" [Timestamp: ").append(System.currentTimeMillis()).append("]\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private byte[] generateHighEntropyContent(int size) {
        byte[] b = new byte[size];
        new Random().nextBytes(b);
        return b;
    }

    private String resolveMimeType(String fileName) {
        if (fileName.endsWith(".docx")) return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
        if (fileName.endsWith(".xlsx")) return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
        if (fileName.endsWith(".pdf")) return "application/pdf";
        if (fileName.endsWith(".txt")) return "text/plain";
        return "application/octet-stream";
    }

    public Map<String, MonitoredDrive> getDrives() {
        return drives;
    }

    @Override
    public TenantProfile discoverTenantProfile(String tenantId) {
        String tId = (tenantId == null || tenantId.isBlank()) ? "tenant-mid-2500" : tenantId;
        int seats = 2500;
        String sku = "M365_BUSINESS_PREMIUM";
        String orgName = "Contoso Logistics";

        if (tId.contains("smb") || tId.contains("250")) {
            seats = 250;
            sku = "O365_BUSINESS_PREMIUM";
            orgName = "Acme TinyCorp";
        } else if (tId.contains("mid") || tId.contains("2500")) {
            seats = 2500;
            sku = "M365_BUSINESS_PREMIUM";
            orgName = "Contoso Logistics";
        } else if (tId.contains("ent") || tId.contains("10k")) {
            seats = 10000;
            sku = "SPE_E5";
            orgName = "Fabrikam Financial";
        } else if (tId.contains("large") || tId.contains("35k")) {
            seats = 35000;
            sku = "SPE_E3";
            orgName = "Northwind Health";
        } else if (tId.contains("global") || tId.contains("100k")) {
            seats = 100000;
            sku = "SPE_E5";
            orgName = "MegaCorp Global";
        }

        TenantTier tier = TenantTier.fromSku(sku, seats);
        return new TenantProfile(
                tId,
                orgName,
                tier,
                seats,
                tier.getDailyRuBudget(),
                tier.getDefaultBurstLimitRUs(),
                sku,
                TierSource.GRAPH_SKU_DISCOVERY,
                false,
                0,
                0
        );
    }

    @Override
    public int discoverActiveUserCount(String tenantId) {
        String tId = (tenantId == null || tenantId.isBlank()) ? "tenant-mid-2500" : tenantId;
        if (tId.contains("smb") || tId.contains("250")) return 250;
        if (tId.contains("mid") || tId.contains("2500")) return 2500;
        if (tId.contains("ent") || tId.contains("10k")) return 10000;
        if (tId.contains("large") || tId.contains("35k")) return 35000;
        if (tId.contains("global") || tId.contains("100k")) return 100000;
        return 2500;
    }
}
