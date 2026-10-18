package io.oxmq.oxguard.connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.oxmq.oxguard.engine.MalwareInspectionEngine;
import io.oxmq.oxguard.mockserver.MockOffice365Server;
import io.oxmq.oxguard.model.DriveItem;
import io.oxmq.oxguard.model.EffectiveExposure;
import io.oxmq.oxguard.model.MonitoredDrive;
import io.oxmq.oxguard.model.ThreatAnalysisResult;
import io.oxmq.oxguard.model.ThreatVerdict;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end integration test verifying that LiveGraphClient communicates with MockOffice365Server
 * over real HTTP, and downloads and inspects real physical files streamed from disk.
 */
public class MockOffice365ServerLiveIntegrationTest {

    private static final int TEST_PORT = 8089;
    private static Path tempStorageDir;
    private static MockOffice365Server mockServer;
    private static LiveGraphClient liveClient;
    private static MalwareInspectionEngine malwareEngine;

    @BeforeAll
    static void setup() throws IOException {
        tempStorageDir = Files.createTempDirectory("mock-m365-test-storage");
        ObjectMapper objectMapper = new ObjectMapper();

        mockServer = new MockOffice365Server(
                TEST_PORT,
                tempStorageDir.toString(),
                false, // disable 429 burst for clean test assertions
                "http://localhost:8080/api/v1/webhooks/graph",
                "test-client-state",
                objectMapper
        );
        mockServer.start();

        liveClient = new LiveGraphClient(
                "contoso-enterprise-tenant",
                "test-client-id",
                "test-client-secret",
                "http://localhost:" + TEST_PORT + "/v1.0",
                "http://localhost:" + TEST_PORT,
                objectMapper
        );

        malwareEngine = new MalwareInspectionEngine();
    }

    @AfterAll
    static void tearDown() {
        if (mockServer != null) {
            mockServer.stop();
        }
    }

    @Test
    void testRealPhysicalFilesExistOnDisk() {
        assertTrue(Files.exists(tempStorageDir.resolve("Corporate_Finance_2026.docx")));
        assertTrue(Files.exists(tempStorageDir.resolve("security_audit_eicar.txt")));
        assertTrue(Files.exists(tempStorageDir.resolve("locked_database_backup.dat.aes")));
        assertTrue(Files.exists(tempStorageDir.resolve("Executive_Payroll_Statement.docx")));
    }

    @Test
    void testDiscoverSharePointDrivesOverHttp() {
        List<MonitoredDrive> drives = liveClient.discoverSharePointDrives("contoso-enterprise-tenant");
        assertNotNull(drives);
        assertFalse(drives.isEmpty());
        assertTrue(drives.stream().anyMatch(d -> d.name().contains("Finance Documents")));
    }

    @Test
    void testDiscoverUserOneDrivesOverHttp() {
        List<MonitoredDrive> drives = liveClient.discoverUserOneDrives("contoso-enterprise-tenant");
        assertNotNull(drives);
        assertFalse(drives.isEmpty());
        assertTrue(drives.stream().anyMatch(d -> d.name().contains("ciso@contoso.com")));
    }

    @Test
    void testFetchDeltaChangesAndStreamRealFileBytes() throws Exception {
        DeltaSyncResult deltaResult = liveClient.fetchDeltaChanges("sp-drive-finance", null);
        assertNotNull(deltaResult);
        assertFalse(deltaResult.updatedItems().isEmpty());

        // Find the Executive_Payroll_Statement.docx file (PE Header disguised as docx)
        DriveItem peFile = deltaResult.updatedItems().stream()
                .filter(i -> i.name().contains("Executive_Payroll_Statement.docx"))
                .findFirst()
                .orElseThrow();

        // Stream real bytes from Mock Server over HTTP
        try (InputStream stream = liveClient.streamFileContent(peFile.driveId(), peFile.id(), peFile.downloadUrl())) {
            ThreatAnalysisResult result = malwareEngine.inspect(peFile.name(), stream);
            assertEquals(ThreatVerdict.MIME_MISMATCH, result.verdict());
            assertTrue(result.details().toLowerCase().contains("executable"));
        }
    }

    @Test
    void testEicarMalwareDetectionFromRealStream() throws Exception {
        DeltaSyncResult deltaResult = liveClient.fetchDeltaChanges("od-drive-ciso", null);
        assertNotNull(deltaResult);

        DriveItem eicarFile = deltaResult.updatedItems().stream()
                .filter(i -> i.name().contains("security_audit_eicar.txt"))
                .findFirst()
                .orElseThrow();

        try (InputStream stream = liveClient.streamFileContent(eicarFile.driveId(), eicarFile.id(), eicarFile.downloadUrl())) {
            ThreatAnalysisResult result = malwareEngine.inspect(eicarFile.name(), stream);
            assertEquals(ThreatVerdict.MALWARE_DETECTED, result.verdict());
            assertTrue(result.matchedPattern().contains("EICAR"));
        }
    }

    @Test
    void testRansomwareHighEntropyDetectionFromRealStream() throws Exception {
        DeltaSyncResult deltaResult = liveClient.fetchDeltaChanges("sp-drive-it", null);
        assertNotNull(deltaResult);

        DriveItem ransomwareFile = deltaResult.updatedItems().stream()
                .filter(i -> i.name().contains("locked_database_backup.dat.aes"))
                .findFirst()
                .orElseThrow();

        try (InputStream stream = liveClient.streamFileContent(ransomwareFile.driveId(), ransomwareFile.id(), ransomwareFile.downloadUrl())) {
            ThreatAnalysisResult result = malwareEngine.inspect(ransomwareFile.name(), stream);
            assertEquals(ThreatVerdict.SUSPICIOUS_ENTROPY, result.verdict());
            assertTrue(result.entropy() > 7.5);
        }
    }

    @Test
    void testFetchPermissionsOverHttp() {
        DeltaSyncResult deltaResult = liveClient.fetchDeltaChanges("sp-drive-finance", null);
        DriveItem item = deltaResult.updatedItems().get(0);

        EffectiveExposure exposure = liveClient.fetchPermissions(item.driveId(), item.id());
        assertNotNull(exposure);
        assertNotNull(exposure.level());
    }
}
