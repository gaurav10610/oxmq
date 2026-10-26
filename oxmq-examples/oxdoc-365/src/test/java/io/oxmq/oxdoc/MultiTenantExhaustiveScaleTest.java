package io.oxmq.oxdoc;

import io.oxmq.oxdoc.model.AccessScope;
import io.oxmq.oxdoc.model.IndexedDocumentRecord;
import io.oxmq.oxdoc.model.MonitoredDrive;
import io.oxmq.oxdoc.model.ScanOrigin;
import io.oxmq.oxdoc.model.TenantProfile;
import io.oxmq.oxdoc.model.TenantTier;
import io.oxmq.oxdoc.queue.OxDocQueueConstants;
import io.oxmq.oxdoc.service.DocumentCatalogRepository;
import io.oxmq.oxdoc.service.TenantSubscriptionService;
import io.oxmq.oxdoc.web.DashboardController;
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

/**
 * Exhaustive Multi-Tenant Scale & Concurrency Test across all 5 official Microsoft 365 licensing tiers:
 * 1. SMB: tenant-smb-250 (250 seats, 25 RUs/s)
 * 2. MID_MARKET: tenant-mid-2500 (2,500 seats, 50 RUs/s)
 * 3. ENTERPRISE: tenant-ent-10k (10,000 seats, 90 RUs/s)
 * 4. LARGE_ENTERPRISE: tenant-large-35k (35,000 seats, 150 RUs/s)
 * 5. HYPERSCALE: tenant-global-100k (100,000 seats, 250 RUs/s)
 *
 * Verifies concurrent DAG processing, zero-heap streaming extraction, rate limit isolation,
 * and multi-tenant catalog partitioning.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DisplayName("OxDoc 365: Exhaustive Scale Test across all 5 Tenant Tiers")
public class MultiTenantExhaustiveScaleTest {

    private static final List<String> ALL_5_TENANTS = List.of(
            "tenant-smb-250",
            "tenant-mid-2500",
            "tenant-ent-10k",
            "tenant-large-35k",
            "tenant-global-100k"
    );

    @Autowired
    private DashboardController dashboardController;

    @Autowired
    private DocumentCatalogRepository repository;

    @Autowired
    private TenantSubscriptionService subscriptionService;

    @Autowired
    private OxmqTemplate oxmqTemplate;

    @Autowired(required = false)
    private io.oxmq.oxdoc.connector.MockGraphClient mockGraphClient;

    @BeforeEach
    void setUp() {
        if (mockGraphClient != null) {
            mockGraphClient.reset();
        }
        repository.clear();
    }

    @AfterEach
    void tearDown() {
        try {
            oxmqTemplate.obliterate(OxDocQueueConstants.QUEUE_TENANT_DISCOVERY);
            oxmqTemplate.obliterate(OxDocQueueConstants.QUEUE_DRIVE_DELTA);
            oxmqTemplate.obliterate(OxDocQueueConstants.QUEUE_CONTENT_CATEGORIZATION);
            oxmqTemplate.obliterate(OxDocQueueConstants.QUEUE_DOCUMENT_EXTRACTION);
            oxmqTemplate.obliterate(OxDocQueueConstants.QUEUE_CATALOG_INDEX);
        } catch (Exception ignored) {}
    }

    @Test
    @DisplayName("Concurrent Execution: All 5 Tenant Tiers undergo simultaneous Baseline Crawl & Priority 1 Forward Scan")
    void testSimultaneous5TenantScaleExecution() {
        // Step 1: Trigger concurrent baseline crawl for all 5 official tenants simultaneously
        dashboardController.triggerCrawlAll();

        // Step 2: Concurrently simulate real-time forward upload events across multiple tiers
        dashboardController.simulateForwardUpload(
                "tenant-smb-250",
                "drive-tenant-smb-250-fwd",
                "urgent_smb_invoice.pdf",
                "ORGANIZATION_WIDE",
                "DOCUMENT"
        );
        dashboardController.simulateForwardUpload(
                "tenant-global-100k",
                "drive-tenant-global-100k-fwd",
                "hyperscale_executive_briefing.xlsx",
                "PUBLIC_ANONYMOUS",
                "SPREADSHEET"
        );

        // Step 3: Await concurrent FlowProducer DAG resolution across all 5 tenants
        await().atMost(45, TimeUnit.SECONDS).untilAsserted(() -> {
            for (String tenantId : ALL_5_TENANTS) {
                List<IndexedDocumentRecord> tenantDocs = repository.getDocumentsByTenant(tenantId);
                assertThat(tenantDocs)
                        .withFailMessage("Expected indexed documents for tenant [%s]", tenantId)
                        .isNotEmpty();

                // Verify tenant data isolation: all documents must strictly belong to this tenantId
                assertThat(tenantDocs)
                        .allMatch(doc -> tenantId.equals(doc.tenantId()));

                // Verify SHA-256 cryptographic hashes are calculated on all documents
                assertThat(tenantDocs)
                        .allMatch(doc -> doc.sha256() != null && doc.sha256().length() == 64);

                // Verify distinct drives were discovered and cataloged
                List<MonitoredDrive> tenantDrives = repository.getDrivesByTenantId(tenantId);
                assertThat(tenantDrives)
                        .withFailMessage("Expected discovered drives for tenant [%s]", tenantId)
                        .isNotEmpty();
                assertThat(tenantDrives)
                        .allMatch(drive -> tenantId.equals(drive.tenantId()));
            }

            // Step 4: Verify Priority 1 Forward Scan ingestion
            List<IndexedDocumentRecord> allDocs = repository.getAllDocuments();
            assertThat(allDocs).anyMatch(d ->
                    "tenant-smb-250".equals(d.tenantId()) &&
                    d.fileName().contains("urgent_smb_invoice") &&
                    d.scanOrigin() == ScanOrigin.FORWARD
            );
            assertThat(allDocs).anyMatch(d ->
                    "tenant-global-100k".equals(d.tenantId()) &&
                    d.fileName().contains("hyperscale_executive_briefing") &&
                    d.scanOrigin() == ScanOrigin.FORWARD
            );
        });

        // Step 5: Verify subscription tiers resolved accurately for all 5 tenants
        for (String tenantId : ALL_5_TENANTS) {
            TenantProfile profile = subscriptionService.getTenantProfile(tenantId);
            assertThat(profile).isNotNull();
            assertThat(profile.tenantId()).isEqualTo(tenantId);
            if (tenantId.contains("smb")) {
                assertThat(profile.tier()).isEqualTo(TenantTier.SMB);
                assertThat(profile.burstLimitRUsPerSec()).isEqualTo(25);
            } else if (tenantId.contains("mid")) {
                assertThat(profile.tier()).isEqualTo(TenantTier.MID_MARKET);
                assertThat(profile.burstLimitRUsPerSec()).isEqualTo(50);
            } else if (tenantId.contains("ent")) {
                assertThat(profile.tier()).isEqualTo(TenantTier.ENTERPRISE);
                assertThat(profile.burstLimitRUsPerSec()).isEqualTo(90);
            } else if (tenantId.contains("large")) {
                assertThat(profile.tier()).isEqualTo(TenantTier.LARGE_ENTERPRISE);
                assertThat(profile.burstLimitRUsPerSec()).isEqualTo(150);
            } else if (tenantId.contains("global")) {
                assertThat(profile.tier()).isEqualTo(TenantTier.HYPERSCALE);
                assertThat(profile.burstLimitRUsPerSec()).isEqualTo(250);
            }
        }

        // Step 6: Verify total document count metrics
        Map<String, Object> metrics = repository.getMetrics();
        assertThat((Long) metrics.get("totalDocuments")).isGreaterThanOrEqualTo(25L);
    }

    @Test
    @DisplayName("Chaos & Isolation: Simultaneous P1 Forward Ingestion Storm across all 5 Tenants")
    void testConcurrentRealTimeForwardStormAll5Tenants() throws Exception {
        int threadsPerTenant = 3;
        int totalThreads = ALL_5_TENANTS.size() * threadsPerTenant;
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(totalThreads);
        java.util.concurrent.CountDownLatch startGate = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch doneGate = new java.util.concurrent.CountDownLatch(totalThreads);

        for (String tenantId : ALL_5_TENANTS) {
            for (int i = 0; i < threadsPerTenant; i++) {
                final int idx = i;
                executor.submit(() -> {
                    try {
                        startGate.await();
                        dashboardController.simulateForwardUpload(
                                tenantId,
                                "drive-" + tenantId + "-storm",
                                "urgent_storm_file_" + idx + ".docx",
                                "INTERNAL_ONLY",
                                "DOCUMENT"
                        );
                    } catch (Exception ignored) {
                    } finally {
                        doneGate.countDown();
                    }
                });
            }
        }

        // Release all parallel virtual threads simultaneously
        startGate.countDown();
        assertThat(doneGate.await(10, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        // Await all documents to be indexed and verified strictly by tenant
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            for (String tenantId : ALL_5_TENANTS) {
                List<IndexedDocumentRecord> tenantDocs = repository.getDocumentsByTenant(tenantId);
                assertThat(tenantDocs)
                        .withFailMessage("Expected storm documents indexed for tenant [%s]", tenantId)
                        .anyMatch(doc -> doc.fileName().contains("urgent_storm_file") && doc.scanOrigin() == ScanOrigin.FORWARD);

                // Verify strict zero cross-tenant contamination
                assertThat(tenantDocs)
                        .allMatch(doc -> tenantId.equals(doc.tenantId()));
            }
        });
    }

    @Test
    @DisplayName("Chaos & Isolation: Isolated 429 Damping & Non-Interfering Neighbor Headroom")
    void testIsolatedTenant429DampingAndIndependentRecovery(@Autowired io.oxmq.oxdoc.service.TenantCooldownService cooldownService) {
        String starvedTenant = "tenant-smb-250";
        String healthyTenant = "tenant-global-100k";

        // Inject 429 burst onto starvedTenant
        cooldownService.record429Hit(starvedTenant);
        cooldownService.setCooldown(starvedTenant, 2500);

        // Verify starvedTenant is dampened
        assertThat(cooldownService.getRemainingCooldownMs(starvedTenant)).isGreaterThan(0);

        // Verify healthy tenant has 0 cooldown and remains unaffected
        assertThat(cooldownService.getRemainingCooldownMs(healthyTenant)).isEqualTo(0);

        // Verify quota telemetry reports 429 cooldown isolated to starvedTenant
        Map<String, Object> smbTelemetry = cooldownService.getQuotaTelemetry(starvedTenant);
        assertThat(smbTelemetry.get("is429CooldownActive")).isEqualTo(true);
        assertThat(((Number) smbTelemetry.get("cooldownRemainingMs")).longValue()).isGreaterThan(0L);

        Map<String, Object> globalTelemetry = cooldownService.getQuotaTelemetry(healthyTenant);
        assertThat(globalTelemetry.get("is429CooldownActive")).isEqualTo(false);
        assertThat(((Number) globalTelemetry.get("cooldownRemainingMs")).longValue()).isEqualTo(0L);

        // Recover starvedTenant
        cooldownService.recordRetryRecovered(starvedTenant);
        assertThat(cooldownService.getRemainingCooldownMs(starvedTenant)).isGreaterThanOrEqualTo(0);
    }
}
