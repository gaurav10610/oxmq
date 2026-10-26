package io.oxmq.oxdoc.queue;

import io.oxmq.oxdoc.connector.GraphClient;
import io.oxmq.oxdoc.engine.ContentCategorizationEngine;
import io.oxmq.oxdoc.model.ContentVisibility;
import io.oxmq.oxdoc.model.DocumentProcessingPayload;
import io.oxmq.oxdoc.service.TenantCooldownService;
import io.oxmq.spring.annotation.Header;
import io.oxmq.spring.annotation.OxmqListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Pillar 1 Worker: Resolves effective document permissions and assigns content visibility scope.
 */
@Component
public class ContentCategorizationWorker {

    private static final Logger log = LoggerFactory.getLogger(ContentCategorizationWorker.class);

    private final GraphClient graphClient;
    private final ContentCategorizationEngine categorizationEngine;

    @org.springframework.beans.factory.annotation.Autowired
    public ContentCategorizationWorker(
            GraphClient graphClient,
            ContentCategorizationEngine categorizationEngine
    ) {
        this.graphClient = graphClient;
        this.categorizationEngine = categorizationEngine;
    }

    public ContentCategorizationWorker(
            GraphClient graphClient,
            ContentCategorizationEngine categorizationEngine,
            TenantCooldownService cooldownService
    ) {
        this(graphClient, categorizationEngine);
    }

    @OxmqListener(queue = OxDocQueueConstants.QUEUE_CONTENT_CATEGORIZATION, concurrency = 30)
    public ContentVisibility processContentCategorization(
            DocumentProcessingPayload payload,
            @Header("jobId") String jobId
    ) {
        ContentVisibility exposure = graphClient.fetchPermissions(payload.tenantId(), payload.driveId(), payload.itemId());
        ContentVisibility evaluated = categorizationEngine.evaluate(exposure.permissions());

        log.debug("Evaluated categorization for [{}]: scope={}", payload.itemName(), evaluated.scope());
        return evaluated;
    }
}
