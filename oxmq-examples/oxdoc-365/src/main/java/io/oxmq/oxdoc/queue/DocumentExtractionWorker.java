package io.oxmq.oxdoc.queue;

import io.oxmq.oxdoc.connector.GraphClient;
import io.oxmq.oxdoc.connector.GraphThrottlingException;
import io.oxmq.oxdoc.engine.DocumentExtractionEngine;
import io.oxmq.oxdoc.model.DocumentProcessingPayload;
import io.oxmq.oxdoc.model.DocumentAnalysisResult;
import io.oxmq.oxdoc.service.TenantCooldownService;
import io.oxmq.spring.annotation.Header;
import io.oxmq.spring.annotation.OxmqListener;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Pillar 2 Worker: Streams document bytes directly from Azure Blob SAS URL and executes text/content extraction.
 * Zero-copy streaming ensures minimal JVM heap footprint in Java 17.
 */
@Component
public class DocumentExtractionWorker {

    private static final Logger log = LoggerFactory.getLogger(DocumentExtractionWorker.class);

    private final GraphClient graphClient;
    private final DocumentExtractionEngine extractionEngine;
    private final TenantCooldownService cooldownService;

    public DocumentExtractionWorker(
            GraphClient graphClient,
            DocumentExtractionEngine extractionEngine,
            TenantCooldownService cooldownService
    ) {
        this.graphClient = graphClient;
        this.extractionEngine = extractionEngine;
        this.cooldownService = cooldownService;
    }

    @OxmqListener(queue = OxDocQueueConstants.QUEUE_DOCUMENT_EXTRACTION, concurrency = 25)
    public DocumentAnalysisResult processDocumentExtraction(
            DocumentProcessingPayload payload,
            @Header("jobId") String jobId
    ) {
        try {
            cooldownService.recordTenantApiCall(payload.tenantId(), payload.scanOrigin());
            try (InputStream stream = graphClient.streamFileContent(payload.tenantId(), payload.driveId(), payload.itemId(), payload.downloadUrl())) {
                DocumentAnalysisResult result = extractionEngine.extract(payload.itemName(), stream);
                log.debug("Document extraction completed for [{}]: verdict={}, sha256={}",
                        payload.itemName(), result.verdict(), result.sha256());
                return result;
            }
        } catch (GraphThrottlingException gte) {
            throw gte;
        } catch (Exception e) {
            log.error("Failed document extraction on [{}]: {}", payload.itemName(), e.getMessage());
            return DocumentAnalysisResult.metadataOnly("unknown", "unknown", "Extraction failed: " + e.getMessage());
        }
    }
}
