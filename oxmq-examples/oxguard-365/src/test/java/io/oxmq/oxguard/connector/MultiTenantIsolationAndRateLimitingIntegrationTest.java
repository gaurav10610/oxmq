package io.oxmq.oxguard.connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.oxmq.oxguard.mockserver.MockOffice365Server;
import io.oxmq.oxguard.model.MonitoredDrive;
import io.oxmq.oxguard.model.TenantProfile;
import io.oxmq.oxguard.model.TenantTier;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end integration test verifying multi-tenant isolation, official Microsoft license tiers,
 * and per-tenant 429 rate limiting.
 */
public class MultiTenantIsolationAndRateLimitingIntegrationTest {

    private static final int TEST_PORT = 8092;
    private static Path tempStorageDir;
    private static MockOffice365Server mockServer;
    private static LiveGraphClient liveClient;

    @BeforeAll
    static void setup() throws IOException {
        tempStorageDir = Files.createTempDirectory("mock-m365-multitenant-test");
        ObjectMapper objectMapper = new ObjectMapper();

        mockServer = new MockOffice365Server(
                TEST_PORT,
                tempStorageDir.toString(),
                false,
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
    }

    @AfterAll
    static void tearDown() {
        if (mockServer != null) {
            mockServer.stop();
        }
    }

    @Test
    void testAll5OfficialTenantProfilesConfigured() {
        assertEquals(5, TenantProfile.ALL_PROFILES.size());

        TenantProfile smb = TenantProfile.SMB_250;
        assertEquals("tenant-smb-250", smb.tenantId());
        assertEquals(TenantTier.SMB, smb.tier());
        assertEquals(250, smb.seatCount());
        assertEquals(25, smb.burstLimitRUsPerSec());

        TenantProfile mid = TenantProfile.MID_2500;
        assertEquals("tenant-mid-2500", mid.tenantId());
        assertEquals(TenantTier.MID_MARKET, mid.tier());
        assertEquals(2500, mid.seatCount());
        assertEquals(50, mid.burstLimitRUsPerSec());

        TenantProfile ent = TenantProfile.ENT_10K;
        assertEquals("tenant-ent-10k", ent.tenantId());
        assertEquals(TenantTier.ENTERPRISE, ent.tier());
        assertEquals(10000, ent.seatCount());
        assertEquals(90, ent.burstLimitRUsPerSec());

        TenantProfile large = TenantProfile.LARGE_35K;
        assertEquals("tenant-large-35k", large.tenantId());
        assertEquals(TenantTier.LARGE_ENTERPRISE, large.tier());
        assertEquals(35000, large.seatCount());
        assertEquals(150, large.burstLimitRUsPerSec());

        TenantProfile global = TenantProfile.GLOBAL_100K;
        assertEquals("tenant-global-100k", global.tenantId());
        assertEquals(TenantTier.HYPERSCALE, global.tier());
        assertEquals(100000, global.seatCount());
        assertEquals(250, global.burstLimitRUsPerSec());
    }

    @Test
    void testIndependentDriveDiscoveryAcrossAll5Tenants() {
        for (TenantProfile profile : TenantProfile.ALL_PROFILES) {
            List<MonitoredDrive> spDrives = liveClient.discoverSharePointDrives(profile.tenantId());
            assertNotNull(spDrives, "SP drives should not be null for " + profile.tenantId());
            assertFalse(spDrives.isEmpty(), "SP drives should not be empty for " + profile.tenantId());

            List<MonitoredDrive> odDrives = liveClient.discoverUserOneDrives(profile.tenantId());
            assertNotNull(odDrives, "OneDrive drives should not be null for " + profile.tenantId());
            assertFalse(odDrives.isEmpty(), "OneDrive drives should not be empty for " + profile.tenantId());

            // Verify all returned drives belong to this specific tenant
            assertTrue(spDrives.stream().allMatch(d -> profile.tenantId().equals(d.tenantId())));
            assertTrue(odDrives.stream().allMatch(d -> profile.tenantId().equals(d.tenantId())));
        }
    }

    @Test
    void testStrictPerTenantIsolationUnder429Throttling() {
        // Step 1: Trigger 429 burst ONLY on Acme SMB (Tier 1)
        mockServer.trigger429Burst("tenant-smb-250", 10);

        // Step 2: Acme SMB must throw GraphThrottlingException (HTTP 429)
        GraphThrottlingException gte = assertThrows(GraphThrottlingException.class, () -> {
            liveClient.discoverSharePointDrives("tenant-smb-250");
        });
        assertEquals(3, gte.getRetryAfterSeconds());

        // Step 3: MegaCorp Global (Tier 5) and Fabrikam Ent (Tier 3) MUST SUCCEED with 200 OK!
        List<MonitoredDrive> globalDrives = liveClient.discoverSharePointDrives("tenant-global-100k");
        assertNotNull(globalDrives);
        assertFalse(globalDrives.isEmpty());

        List<MonitoredDrive> entDrives = liveClient.discoverSharePointDrives("tenant-ent-10k");
        assertNotNull(entDrives);
        assertFalse(entDrives.isEmpty());

        // Step 4: Reset Acme SMB burst
        mockServer.trigger429Burst("tenant-smb-250", 0);
        mockServer.setSimulate429Burst("tenant-smb-250", false);
    }
}
