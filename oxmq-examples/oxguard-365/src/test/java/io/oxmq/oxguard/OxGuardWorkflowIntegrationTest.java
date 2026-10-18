package io.oxmq.oxguard;

import io.oxmq.oxguard.model.ExposureLevel;
import io.oxmq.oxguard.model.ScanOrigin;
import io.oxmq.oxguard.model.ScannedFileRecord;
import io.oxmq.oxguard.model.ThreatVerdict;
import io.oxmq.oxguard.queue.OxGuardQueueConstants;
import io.oxmq.oxguard.service.FileInventoryRepository;
import io.oxmq.oxguard.service.TenantCooldownService;
import io.oxmq.oxguard.web.DashboardController;
import io.oxmq.spring.OxmqTemplate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DisplayName("OxGuard 365: Full End-to-End DSPM & OxMQ DAG Integration Test")
class OxGuardWorkflowIntegrationTest {

    @Autowired
    private OxmqTemplate oxmqTemplate;

    @Autowired
    private DashboardController dashboardController;

    @Autowired
    private FileInventoryRepository repository;

    @Autowired
    private TenantCooldownService cooldownService;

    @BeforeEach
    void setUp() {
        repository.clear();
    }

    @AfterEach
    void tearDown() {
        try {
            oxmqTemplate.obliterate(OxGuardQueueConstants.QUEUE_TENANT_DISCOVERY);
            oxmqTemplate.obliterate(OxGuardQueueConstants.QUEUE_DRIVE_DELTA);
            oxmqTemplate.obliterate(OxGuardQueueConstants.QUEUE_FILE_PERMISSION);
            oxmqTemplate.obliterate(OxGuardQueueConstants.QUEUE_FILE_MALWARE);
            oxmqTemplate.obliterate(OxGuardQueueConstants.QUEUE_FILE_AGGREGATE);
        } catch (Exception ignored) {}
    }

    @Test
    @DisplayName("End-to-End: Baseline crawl triggers FlowProducer DAG with parallel Exposure & Malware inspection")
    void testBaselineCrawlFlowProducerDag() {
        // 1. Trigger tenant discovery and baseline crawl
        dashboardController.triggerBaselineCrawl("contoso-enterprise-tenant");

        // 2. Await FlowProducer DAG execution: Parent Aggregate awaits Child Permissions + Child Malware
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            List<ScannedFileRecord> files = repository.getAllFiles();
            assertThat(files).isNotEmpty();
            assertThat(files.size()).isGreaterThanOrEqualTo(5);

            // Verify Pillar 1 Exposure classification
            boolean hasPublicOrExternal = files.stream().anyMatch(f ->
                    f.exposureLevel() == ExposureLevel.PUBLIC_ANONYMOUS ||
                    f.exposureLevel() == ExposureLevel.EXTERNAL_GUEST ||
                    f.exposureLevel() == ExposureLevel.ORGANIZATION_WIDE);
            assertThat(hasPublicOrExternal).isTrue();

            // Verify Pillar 2 Threat Inspection results
            boolean hasThreatOrClean = files.stream().anyMatch(f ->
                    f.threatVerdict() == ThreatVerdict.CLEAN ||
                    f.threatVerdict() == ThreatVerdict.MALWARE_DETECTED ||
                    f.threatVerdict() == ThreatVerdict.SUSPICIOUS_ENTROPY ||
                    f.threatVerdict() == ThreatVerdict.MIME_MISMATCH);
            assertThat(hasThreatOrClean).isTrue();

            // Verify SHA-256 hashes are populated
            boolean hasSha256 = files.stream().anyMatch(f -> f.sha256() != null && f.sha256().length() == 64);
            assertThat(hasSha256).isTrue();
        });

        // 3. Verify Metrics API
        Map<String, Object> metrics = repository.getMetrics();
        assertThat((Long) metrics.get("totalFiles")).isGreaterThan(0L);
    }

    @Test
    @DisplayName("Priority Arbitration: Forward Scan (Priority 1) executes and is tagged with FORWARD origin")
    void testForwardRealTimeScanPrioritization() {
        // Trigger simulated real-time upload
        dashboardController.simulateForwardUpload(
                "sp-drive-1",
                "urgent_board_briefing.xlsx",
                "PUBLIC_ANONYMOUS",
                "CLEAN"
        );

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<ScannedFileRecord> files = repository.getAllFiles();
            assertThat(files).anyMatch(f ->
                    f.fileName().contains("urgent_board_briefing.xlsx") &&
                    f.scanOrigin() == ScanOrigin.FORWARD &&
                    f.exposureLevel() == ExposureLevel.PUBLIC_ANONYMOUS
            );
        });
    }

    @Test
    @DisplayName("Approach 5: Tenant Cooldown Sentinel correctly sets and reads TTL in Redis")
    void testApproach5TenantCooldownSentinel() {
        String tenant = "throttled-test-tenant-" + System.currentTimeMillis();

        assertThat(cooldownService.getRemainingCooldownMs(tenant)).isEqualTo(0L);

        // Set 429 cooldown for 4000ms
        cooldownService.setCooldown(tenant, 4000L);

        long remaining = cooldownService.getRemainingCooldownMs(tenant);
        assertThat(remaining).isGreaterThan(1000L);
        assertThat(remaining).isLessThanOrEqualTo(4000L);
    }
}
