package io.oxmq.oxguard.connector;

import io.oxmq.oxguard.model.DriveItem;
import io.oxmq.oxguard.model.DriveType;
import io.oxmq.oxguard.model.EffectiveExposure;
import io.oxmq.oxguard.model.ExposureLevel;
import io.oxmq.oxguard.model.MonitoredDrive;
import io.oxmq.oxguard.model.PermissionEntry;
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
@ConditionalOnProperty(name = "oxguard.client.mode", havingValue = "MOCK", matchIfMissing = true)
public class MockGraphClient implements GraphClient {

    private static final Logger log = LoggerFactory.getLogger(MockGraphClient.class);

    public static final String EICAR_TEST_STRING =
            "X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*";

    private final boolean simulate429Burst;
    private final AtomicInteger requestCounter = new AtomicInteger(0);

    // In-memory data store for simulated drives and items
    private final Map<String, MonitoredDrive> drives = new ConcurrentHashMap<>();
    private final Map<String, List<DriveItem>> driveItems = new ConcurrentHashMap<>();
    private final Map<String, EffectiveExposure> itemPermissions = new ConcurrentHashMap<>();
    private final Map<String, byte[]> itemByteContents = new ConcurrentHashMap<>();

    public MockGraphClient(
            @Value("${oxguard.mock.total-sites:5}") int totalSites,
            @Value("${oxguard.mock.total-users:15}") int totalUsers,
            @Value("${oxguard.mock.files-per-drive:30}") int filesPerDrive,
            @Value("${oxguard.mock.simulate-429-burst:false}") boolean simulate429Burst
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

        String[] userNames = {"ceo", "cfo", "cto", "ciso", "lead-architect", "dev-sec-ops", "hr-director", "finance-analyst", "legal-counsel", "product-mgr"};
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
        Random random = new Random(driveId.hashCode());

        for (int i = 1; i <= count; i++) {
            String itemId = driveId + "-file-" + i;
            String fileName;
            byte[] content;
            ExposureLevel exposureLevel;
            boolean hasUniquePermissions = (i % 3 == 0);

            if (i == 1) {
                // Publicly exposed financial sheet
                fileName = "Q3_Consolidated_Budget.xlsx";
                content = generateTextContent("Quarterly financial budget for Contoso Corp. Highly confidential.", 4096);
                exposureLevel = ExposureLevel.PUBLIC_ANONYMOUS;
            } else if (i == 2) {
                // External guest vendor contract
                fileName = "Vendor_Security_SLA_External.docx";
                content = generateTextContent("Vendor Service Level Agreement reviewed by third-party legal counsel.", 8192);
                exposureLevel = ExposureLevel.EXTERNAL_GUEST;
            } else if (i == 3) {
                // Org-wide accessible handbook
                fileName = "Enterprise_Information_Security_Policy.pdf";
                content = generateTextContent("Standard Operating Procedures for all full-time employees and contractors.", 12000);
                exposureLevel = ExposureLevel.ORGANIZATION_WIDE;
            } else if (i == 4) {
                // EICAR Malware Signature test file!
                fileName = "suspected_eicar_virus.txt";
                content = EICAR_TEST_STRING.getBytes(StandardCharsets.UTF_8);
                exposureLevel = ExposureLevel.PRIVATE;
            } else if (i == 5) {
                // Ransomware high-entropy payload simulation
                fileName = "encrypted_ransom_payload.dat";
                content = generateHighEntropyContent(16384);
                exposureLevel = ExposureLevel.INTERNAL_RESTRICTED;
            } else if (i == 6) {
                // PE Executable disguised as Word document (MIME Mismatch)
                fileName = "invoice_payment_instructions.docx";
                content = generateExecutableContent("MZ", 8192);
                exposureLevel = ExposureLevel.EXTERNAL_GUEST;
            } else {
                // Clean standard business files
                fileName = "Project_Document_" + i + ".docx";
                content = generateTextContent("Normal enterprise document content. No suspicious signatures found.", 2048 + (i * 256));
                exposureLevel = (i % 5 == 0) ? ExposureLevel.INTERNAL_RESTRICTED : ExposureLevel.PRIVATE;
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
            if (exposureLevel == ExposureLevel.PUBLIC_ANONYMOUS) {
                permissions.add(new PermissionEntry(UUID.randomUUID().toString(), null, "read", !hasUniquePermissions, hasUniquePermissions ? null : "parent-folder-id", "anonymous"));
            } else if (exposureLevel == ExposureLevel.EXTERNAL_GUEST) {
                permissions.add(new PermissionEntry(UUID.randomUUID().toString(), "vendor-partner@external.com", "read", !hasUniquePermissions, hasUniquePermissions ? null : "parent-folder-id", "users"));
            } else if (exposureLevel == ExposureLevel.ORGANIZATION_WIDE) {
                permissions.add(new PermissionEntry(UUID.randomUUID().toString(), "All Authenticated Users", "read", !hasUniquePermissions, hasUniquePermissions ? null : "parent-folder-id", "organization"));
            }
            itemPermissions.put(itemId, new EffectiveExposure(exposureLevel, hasUniquePermissions, permissions));
        }

        driveItems.put(driveId, items);
    }

    private void checkSimulated429() {
        if (simulate429Burst) {
            int count = requestCounter.incrementAndGet();
            if (count % 8 == 0) { // Every 8th request simulates an HTTP 429
                log.warn("Mock Graph Client simulating HTTP 429 (Retry-After: 4s) to test Approach 5 dynamic deferral!");
                throw new GraphThrottlingException(4, "Rate limit exceeded (Microsoft Graph simulated 429)");
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
    public EffectiveExposure fetchPermissions(String driveId, String itemId) {
        checkSimulated429();
        EffectiveExposure exposure = itemPermissions.get(itemId);
        if (exposure != null) {
            return exposure;
        }
        return new EffectiveExposure(ExposureLevel.PRIVATE, false, List.of(
                new PermissionEntry(UUID.randomUUID().toString(), "owner@contoso.com", "owner", false, null, null)
        ));
    }

    @Override
    public InputStream streamFileContent(String driveId, String itemId, String directDownloadUrl) {
        // Downloading via direct SAS URL does not consume Graph API 429 quota
        byte[] bytes = itemByteContents.get(itemId);
        if (bytes == null) {
            bytes = "Mock default content".getBytes(StandardCharsets.UTF_8);
        }
        return new ByteArrayInputStream(bytes);
    }

    /**
     * Utility method for simulating a real-time Forward upload (e.g. via UI button or test).
     */
    public DriveItem addMockForwardFile(String driveId, String fileName, ExposureLevel exposureLevel, byte[] content) {
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
        permissions.add(new PermissionEntry(UUID.randomUUID().toString(), "uploader@contoso.com", "owner", false, null, null));
        if (exposureLevel == ExposureLevel.PUBLIC_ANONYMOUS) {
            permissions.add(new PermissionEntry(UUID.randomUUID().toString(), null, "read", false, null, "anonymous"));
        } else if (exposureLevel == ExposureLevel.EXTERNAL_GUEST) {
            permissions.add(new PermissionEntry(UUID.randomUUID().toString(), "guest@partner.com", "read", false, null, "users"));
        } else if (exposureLevel == ExposureLevel.ORGANIZATION_WIDE) {
            permissions.add(new PermissionEntry(UUID.randomUUID().toString(), "All Authenticated Users", "read", false, null, "organization"));
        }
        itemPermissions.put(itemId, new EffectiveExposure(exposureLevel, true, permissions));

        log.info("Simulated real-time forward file upload added: [{}] in drive [{}] with exposure [{}]",
                fileName, driveId, exposureLevel);
        return item;
    }

    private byte[] generateTextContent(String text, int targetLength) {
        StringBuilder sb = new StringBuilder(text);
        while (sb.length() < targetLength) {
            sb.append("\nAdditional enterprise metadata block with audited compliance timestamps: ").append(System.nanoTime());
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private byte[] generateHighEntropyContent(int length) {
        byte[] bytes = new byte[length];
        new Random(42).nextBytes(bytes); // High entropy pseudorandom bytes (H ~ 7.9)
        return bytes;
    }

    private byte[] generateExecutableContent(String header, int length) {
        byte[] bytes = new byte[length];
        byte[] headerBytes = header.getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(headerBytes, 0, bytes, 0, headerBytes.length);
        for (int i = headerBytes.length; i < length; i++) {
            bytes[i] = (byte) (i % 256);
        }
        return bytes;
    }

    private String resolveMimeType(String fileName) {
        if (fileName.endsWith(".xlsx")) return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
        if (fileName.endsWith(".docx")) return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
        if (fileName.endsWith(".pdf")) return "application/pdf";
        if (fileName.endsWith(".txt")) return "text/plain";
        return "application/octet-stream";
    }

    public Map<String, MonitoredDrive> getDrives() {
        return drives;
    }
}
