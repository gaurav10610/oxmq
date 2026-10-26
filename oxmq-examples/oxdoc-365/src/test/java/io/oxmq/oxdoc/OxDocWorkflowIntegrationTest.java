package io.oxmq.oxdoc;

import io.oxmq.oxdoc.model.AccessScope;
import io.oxmq.oxdoc.model.IndexedDocumentRecord;
import io.oxmq.oxdoc.model.IndexingVerdict;
import io.oxmq.oxdoc.model.ScanOrigin;
import io.oxmq.oxdoc.queue.OxDocQueueConstants;
import io.oxmq.oxdoc.service.DocumentCatalogRepository;
import io.oxmq.oxdoc.service.TenantCooldownService;
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

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DisplayName("OxDoc 365: Full End-to-End Enterprise Document AI & OxMQ DAG Integration Test")
class OxDocWorkflowIntegrationTest {

    @Autowired
    private OxmqTemplate oxmqTemplate;

    @Autowired
    private DashboardController dashboardController;

    @Autowired
    private DocumentCatalogRepository repository;

    @Autowired
    private TenantCooldownService cooldownService;

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
    @DisplayName("End-to-End: Baseline crawl triggers FlowProducer DAG with parallel Categorization & Extraction")
    void testBaselineCrawlFlowProducerDag() {
        // 1. Trigger tenant discovery and baseline crawl
        dashboardController.triggerBaselineCrawl("tenant-mid-2500");

        // 2. Await FlowProducer DAG execution: Parent Catalog Indexer awaits Child Categorization + Child Extraction
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            List<IndexedDocumentRecord> documents = repository.getAllDocuments();
            assertThat(documents).isNotEmpty();
            assertThat(documents.size()).isGreaterThanOrEqualTo(5);

            // Verify Access Scope classification
            boolean hasPublicOrExternal = documents.stream().anyMatch(d ->
                    d.accessScope() == AccessScope.PUBLIC_ANONYMOUS ||
                    d.accessScope() == AccessScope.EXTERNAL_GUEST ||
                    d.accessScope() == AccessScope.ORGANIZATION_WIDE);
            assertThat(hasPublicOrExternal).isTrue();

            // Verify Extraction & Indexing verdicts
            boolean hasValidVerdict = documents.stream().anyMatch(d ->
                    d.indexingVerdict() == IndexingVerdict.INDEXED_SEARCHABLE ||
                    d.indexingVerdict() == IndexingVerdict.STRUCTURED_DOCUMENT ||
                    d.indexingVerdict() == IndexingVerdict.BINARY_ARCHIVED ||
                    d.indexingVerdict() == IndexingVerdict.PARSED_METADATA_ONLY);
            assertThat(hasValidVerdict).isTrue();

            // Verify SHA-256 hashes are populated
            boolean hasSha256 = documents.stream().anyMatch(d -> d.sha256() != null && d.sha256().length() == 64);
            assertThat(hasSha256).isTrue();
        });

        // 3. Verify Metrics API
        Map<String, Object> metrics = repository.getMetrics();
        assertThat((Long) metrics.get("totalDocuments")).isGreaterThan(0L);
    }

    @Test
    @DisplayName("Priority Arbitration: Forward Scan (Priority 1) executes and is tagged with FORWARD origin")
    void testForwardRealTimeScanPrioritization() {
        // Trigger simulated real-time upload
        dashboardController.simulateForwardUpload(
                "drive-mid-sp-logistics",
                "urgent_board_briefing.xlsx",
                "PUBLIC_ANONYMOUS",
                "STRUCTURED_DOCUMENT"
        );

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            List<IndexedDocumentRecord> documents = repository.getAllDocuments();
            assertThat(documents).anyMatch(d ->
                    d.fileName().contains("urgent_board_briefing.xlsx") &&
                    d.scanOrigin() == ScanOrigin.FORWARD &&
                    d.accessScope() == AccessScope.PUBLIC_ANONYMOUS
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
