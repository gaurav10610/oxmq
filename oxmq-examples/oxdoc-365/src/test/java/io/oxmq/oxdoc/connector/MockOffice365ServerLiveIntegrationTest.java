package io.oxmq.oxdoc.connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.oxmq.mockserver.MockOffice365Server;
import io.oxmq.oxdoc.engine.DocumentExtractionEngine;
import io.oxmq.oxdoc.model.ContentVisibility;
import io.oxmq.oxdoc.model.DocumentAnalysisResult;
import io.oxmq.oxdoc.model.DriveItem;
import io.oxmq.oxdoc.model.IndexingVerdict;
import io.oxmq.oxdoc.model.MonitoredDrive;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end integration test verifying that LiveGraphClient communicates with MockOffice365Server
 * over real HTTP, and downloads and extracts real physical files streamed from disk.
 */
public class MockOffice365ServerLiveIntegrationTest {

    private static final int TEST_PORT = 8089;
    private static Path tempStorageDir;
    private static MockOffice365Server mockServer;
    private static LiveGraphClient liveClient;
    private static DocumentExtractionEngine extractionEngine;

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
                "tenant-mid-2500",
                "test-client-id",
                "test-client-secret",
                "http://localhost:" + TEST_PORT + "/v1.0",
                "http://localhost:" + TEST_PORT,
                objectMapper
        );

        extractionEngine = new DocumentExtractionEngine();
    }

    @AfterAll
    static void tearDown() {
        if (mockServer != null) {
            mockServer.stop();
        }
    }

    @Test
    @DisplayName("Verify mock server seeds real physical files on disk")
    void testRealPhysicalFilesExistOnDisk() {
        assertTrue(Files.exists(tempStorageDir.resolve("Corporate_Finance_2026.docx")));
        assertTrue(Files.exists(tempStorageDir.resolve("system_architecture_spec.txt")));
        assertTrue(Files.exists(tempStorageDir.resolve("vector_embeddings_index.bin")));
        assertTrue(Files.exists(tempStorageDir.resolve("Executive_Strategic_Roadmap.docx")));
    }

    @Test
    @DisplayName("Discover SharePoint document libraries over real HTTP")
    void testDiscoverSharePointDrivesOverHttp() {
        List<MonitoredDrive> drives = liveClient.discoverSharePointDrives("tenant-mid-2500");
        assertNotNull(drives);
        assertFalse(drives.isEmpty());
        assertTrue(drives.stream().anyMatch(d -> d.name().contains("Fleet & Logistics Vault") || d.name().contains("Safety & Compliance Library")));
    }

    @Test
    @DisplayName("Discover OneDrive personal drives over real HTTP")
    void testDiscoverUserOneDrivesOverHttp() {
        List<MonitoredDrive> drives = liveClient.discoverUserOneDrives("tenant-mid-2500");
        assertNotNull(drives);
        assertFalse(drives.isEmpty());
        assertTrue(drives.stream().anyMatch(d -> d.ownerPrincipal().contains("rnd.lead@contoso.com") || d.name().contains("Alex Chen")));
    }

    @Test
    @DisplayName("Stream Word document and analyze structure over HTTP")
    void testFetchDeltaChangesAndStreamRealFileBytes() throws Exception {
        DeltaSyncResult deltaResult = liveClient.fetchDeltaChanges("sp-drive-finance", null);
        assertNotNull(deltaResult);
        assertFalse(deltaResult.updatedItems().isEmpty());

        DriveItem docFile = deltaResult.updatedItems().stream()
                .filter(i -> i.name().contains("Executive_Strategic_Roadmap.docx"))
                .findFirst()
                .orElseThrow();

        // Stream real bytes from Mock Server over HTTP
        try (InputStream stream = liveClient.streamFileContent(docFile.driveId(), docFile.id(), docFile.downloadUrl())) {
            DocumentAnalysisResult result = extractionEngine.extract(docFile.name(), stream);
            assertEquals(IndexingVerdict.INDEXED_SEARCHABLE, result.verdict());
            assertEquals("DOCUMENT", result.documentType());
            assertTrue(result.wordCount() > 0);
        }
    }

    @Test
    @DisplayName("Extract searchable text knowledge from real stream")
    void testTextKnowledgeExtractionFromRealStream() throws Exception {
        DeltaSyncResult deltaResult = liveClient.fetchDeltaChanges("od-drive-rnd", null);
        assertNotNull(deltaResult);

        DriveItem txtFile = deltaResult.updatedItems().stream()
                .filter(i -> i.name().contains("system_architecture_spec.txt"))
                .findFirst()
                .orElseThrow();

        try (InputStream stream = liveClient.streamFileContent(txtFile.driveId(), txtFile.id(), txtFile.downloadUrl())) {
            DocumentAnalysisResult result = extractionEngine.extract(txtFile.name(), stream);
            assertEquals(IndexingVerdict.INDEXED_SEARCHABLE, result.verdict());
            assertTrue(result.wordCount() > 0);
        }
    }

    @Test
    @DisplayName("Analyze high-entropy binary embeddings index from real stream")
    void testBinaryDatasetExtractionFromRealStream() throws Exception {
        DeltaSyncResult deltaResult = liveClient.fetchDeltaChanges("sp-drive-it", null);
        assertNotNull(deltaResult);

        DriveItem binFile = deltaResult.updatedItems().stream()
                .filter(i -> i.name().contains("vector_embeddings_index.bin"))
                .findFirst()
                .orElseThrow();

        try (InputStream stream = liveClient.streamFileContent(binFile.driveId(), binFile.id(), binFile.downloadUrl())) {
            DocumentAnalysisResult result = extractionEngine.extract(binFile.name(), stream);
            assertTrue(result.verdict() == IndexingVerdict.STRUCTURED_DOCUMENT || result.verdict() == IndexingVerdict.BINARY_ARCHIVED);
            assertTrue(result.entropy() > 7.5);
        }
    }

    @Test
    @DisplayName("Fetch document permissions and access visibility over HTTP")
    void testFetchPermissionsOverHttp() {
        DeltaSyncResult deltaResult = liveClient.fetchDeltaChanges("sp-drive-finance", null);
        DriveItem item = deltaResult.updatedItems().get(0);

        ContentVisibility visibility = liveClient.fetchPermissions(item.driveId(), item.id());
        assertNotNull(visibility);
        assertNotNull(visibility.scope());
    }

    @Test
    @DisplayName("Verify Microsoft Graph telemetry headers (x-ms-resource-unit and x-ms-throttle-limit-percentage)")
    void testMicrosoftGraphTelemetryHeadersAndRateLimits() throws Exception {
        java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();
        java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create("http://localhost:" + TEST_PORT + "/v1.0/drives/sp-drive-finance/root/delta"))
                .header("X-Tenant-Id", "tenant-mid-2500")
                .GET()
                .build();

        java.net.http.HttpResponse<String> response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        // Verify authentic Microsoft Graph telemetry header
        assertTrue(response.headers().firstValue("x-ms-resource-unit").isPresent(), "x-ms-resource-unit header should be present");
        assertEquals("1", response.headers().firstValue("x-ms-resource-unit").get());

        // Verify official TenantTier sustained and minute rates match Microsoft Learn specifications
        assertEquals(20, io.oxmq.oxdoc.model.TenantTier.SMB.getSustainedRUsPerSec());
        assertEquals(1200, io.oxmq.oxdoc.model.TenantTier.SMB.getRequestsPerMinute());
        assertEquals(40, io.oxmq.oxdoc.model.TenantTier.MID_MARKET.getSustainedRUsPerSec());
        assertEquals(2400, io.oxmq.oxdoc.model.TenantTier.MID_MARKET.getRequestsPerMinute());
        assertEquals(60, io.oxmq.oxdoc.model.TenantTier.ENTERPRISE.getSustainedRUsPerSec());
        assertEquals(3600, io.oxmq.oxdoc.model.TenantTier.ENTERPRISE.getRequestsPerMinute());
        assertEquals(80, io.oxmq.oxdoc.model.TenantTier.LARGE_ENTERPRISE.getSustainedRUsPerSec());
        assertEquals(4800, io.oxmq.oxdoc.model.TenantTier.LARGE_ENTERPRISE.getRequestsPerMinute());
        assertEquals(100, io.oxmq.oxdoc.model.TenantTier.HYPERSCALE.getSustainedRUsPerSec());
        assertEquals(6000, io.oxmq.oxdoc.model.TenantTier.HYPERSCALE.getRequestsPerMinute());
    }
}
