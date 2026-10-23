package io.oxmq.oxdoc.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.oxmq.model.Job;
import io.oxmq.oxdoc.model.AccessScope;
import io.oxmq.oxdoc.model.ContentVisibility;
import io.oxmq.oxdoc.model.DocumentCatalogPayload;
import io.oxmq.oxdoc.model.DocumentAnalysisResult;
import io.oxmq.oxdoc.model.IndexedDocumentRecord;
import io.oxmq.oxdoc.model.IndexingVerdict;
import io.oxmq.oxdoc.service.DocumentCatalogRepository;
import io.oxmq.oxdoc.service.LiveMetricsService;
import io.oxmq.oxdoc.service.TenantCooldownService;
import io.oxmq.spring.annotation.OxmqListener;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Parent DAG Catalog Indexer Worker:
 * Activates automatically when BOTH Pillar 1 (Categorization) and Pillar 2 (Extraction) child jobs complete.
 * Merges visibility scopes and document analysis verdicts into the persistent knowledge catalog.
 */
@Component
public class CatalogIndexWorker {

    private static final Logger log = LoggerFactory.getLogger(CatalogIndexWorker.class);

    private final DocumentCatalogRepository repository;
    private final LiveMetricsService metricsService;
    private final ObjectMapper objectMapper;
    private final TenantCooldownService cooldownService;

    public CatalogIndexWorker(
            DocumentCatalogRepository repository,
            LiveMetricsService metricsService,
            ObjectMapper objectMapper,
            TenantCooldownService cooldownService
    ) {
        this.repository = repository;
        this.metricsService = metricsService;
        this.objectMapper = objectMapper;
        this.cooldownService = cooldownService;
    }

    @OxmqListener(queue = OxDocQueueConstants.QUEUE_CATALOG_INDEX, concurrency = 15)
    public IndexedDocumentRecord processAggregate(Job<DocumentCatalogPayload> job) {
        if (job.getAttemptsMade() > 1) {
            cooldownService.recordRetryRecovered();
        }
        DocumentCatalogPayload payload = (DocumentCatalogPayload) job.getData();
        Map<String, Object> childrenValues = job.getChildrenValues();

        log.info("Parent DAG Catalog Indexer triggered for document [{}] (jobId={}). Merging child analysis verdicts...",
                payload.itemName(), job.getId());

        ContentVisibility visibility = null;
        DocumentAnalysisResult analysis = null;

        if (childrenValues != null) {
            for (Map.Entry<String, Object> entry : childrenValues.entrySet()) {
                String childKey = entry.getKey();
                Object val = entry.getValue();

                if (childKey.contains(OxDocQueueConstants.QUEUE_CONTENT_CATEGORIZATION) || childKey.contains("perm-") || childKey.contains("categorize-")) {
                    visibility = convertValue(val, ContentVisibility.class);
                } else if (childKey.contains(OxDocQueueConstants.QUEUE_DOCUMENT_EXTRACTION) || childKey.contains("malware-") || childKey.contains("extract-")) {
                    analysis = convertValue(val, DocumentAnalysisResult.class);
                }
            }
        }

        // Fallback defaults if a child failed with failParentOnFailure=false
        if (visibility == null) {
            visibility = new ContentVisibility(AccessScope.PRIVATE, false, java.util.List.of());
        }
        if (analysis == null) {
            analysis = DocumentAnalysisResult.searchable("sha256-uncomputed", 0.0, 100L, 1, "en", "DOCUMENT", "Default analysis");
        }

        IndexedDocumentRecord record = new IndexedDocumentRecord(
                payload.itemId(),
                payload.driveId(),
                payload.tenantId(),
                payload.itemName(),
                payload.itemPath(),
                payload.fileSize(),
                payload.mimeType(),
                visibility.scope(),
                visibility.hasUniquePermissions(),
                analysis.verdict(),
                analysis.details(),
                analysis.sha256(),
                payload.scanOrigin(),
                System.currentTimeMillis()
        );

        // Save to repository
        repository.saveDocument(record);

        // Broadcast to live dashboard via SSE
        metricsService.broadcastDocumentIndexed(record);

        log.info("Content Intelligence Indexing Completed for [{}]: Scope=[{}], Verdict=[{}], Origin=[{}]",
                record.fileName(), record.accessScope(), record.indexingVerdict(), record.scanOrigin());

        return record;
    }

    private <T> T convertValue(Object val, Class<T> clazz) {
        if (val == null) return null;
        if (clazz.isInstance(val)) return clazz.cast(val);
        try {
            return objectMapper.convertValue(val, clazz);
        } catch (Exception e) {
            log.debug("Could not convert child result to {}: {}", clazz.getSimpleName(), e.getMessage());
            return null;
        }
    }
}
