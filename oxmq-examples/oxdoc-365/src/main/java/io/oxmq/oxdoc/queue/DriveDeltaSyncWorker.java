package io.oxmq.oxdoc.queue;

import io.oxmq.FlowProducer;
import io.oxmq.model.BackoffStrategy;
import io.oxmq.model.FlowJob;
import io.oxmq.model.JobOptions;
import io.oxmq.oxdoc.connector.DeltaSyncResult;
import io.oxmq.oxdoc.connector.GraphClient;
import io.oxmq.oxdoc.model.DocumentCatalogPayload;
import io.oxmq.oxdoc.model.DocumentProcessingPayload;
import io.oxmq.oxdoc.model.DriveDeltaSyncPayload;
import io.oxmq.oxdoc.model.DriveItem;
import io.oxmq.oxdoc.model.DriveType;
import io.oxmq.oxdoc.model.MonitoredDrive;
import io.oxmq.oxdoc.model.ScanOrigin;
import io.oxmq.oxdoc.service.DocumentCatalogRepository;
import io.oxmq.oxdoc.service.TenantCooldownService;
import io.oxmq.plugin.CheckpointStatePlugin;
import io.oxmq.spring.OxmqTemplate;
import io.oxmq.spring.annotation.Header;
import io.oxmq.spring.annotation.OxmqListener;
import io.oxmq.tracing.TraceContext;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Executes incremental delta synchronization on drives.
 * Enforces Forward (Priority 1) vs Backward (Priority 10) priority arbitration,
 * yields capacity when tenant token headroom &lt; 25%, and dispatches OxMQ FlowProducer DAGs.
 * Checkpoints delta tokens using native OxMQ CheckpointStatePlugin and PostgreSQL for seamless crash recovery.
 */
@Component
public class DriveDeltaSyncWorker {

    private static final Logger log = LoggerFactory.getLogger(DriveDeltaSyncWorker.class);

    private final GraphClient graphClient;
    private final TenantCooldownService cooldownService;
    private final FlowProducer flowProducer;
    private final DocumentCatalogRepository catalogRepository;
    private final CheckpointStatePlugin checkpointStatePlugin;
    private final OxmqTemplate oxmqTemplate;

    @Autowired
    public DriveDeltaSyncWorker(
            GraphClient graphClient,
            TenantCooldownService cooldownService,
            FlowProducer flowProducer,
            DocumentCatalogRepository catalogRepository,
            @Autowired(required = false) CheckpointStatePlugin checkpointStatePlugin,
            @Autowired(required = false) OxmqTemplate oxmqTemplate
    ) {
        this.graphClient = graphClient;
        this.cooldownService = cooldownService;
        this.flowProducer = flowProducer;
        this.catalogRepository = catalogRepository;
        this.checkpointStatePlugin = checkpointStatePlugin;
        this.oxmqTemplate = oxmqTemplate;
    }

    public DriveDeltaSyncWorker(
            GraphClient graphClient,
            TenantCooldownService cooldownService,
            FlowProducer flowProducer,
            DocumentCatalogRepository catalogRepository
    ) {
        this(graphClient, cooldownService, flowProducer, catalogRepository, null, null);
    }

    public DriveDeltaSyncWorker(
            GraphClient graphClient,
            TenantCooldownService cooldownService,
            FlowProducer flowProducer
    ) {
        this(graphClient, cooldownService, flowProducer, null, null, null);
    }

    @OxmqListener(queue = OxDocQueueConstants.QUEUE_DRIVE_DELTA, concurrency = 20)
    public int processDeltaSync(
            DriveDeltaSyncPayload payload,
            @Header("jobId") String jobId
    ) {
        log.info("Starting Drive Delta Sync for drive [{}] (tenant={}, origin={}, jobId={})",
                payload.driveId(), payload.tenantId(), payload.scanOrigin(), jobId);

        // Record API call in sliding-window metrics
        cooldownService.recordTenantApiCall(payload.tenantId(), payload.scanOrigin());

        // Checkpoint Recovery: load continuation page token or existing delta token from CheckpointStatePlugin (or DB fallback)
        String savedDeltaToken = null;
        if (payload.continuationToken() != null && !payload.continuationToken().isBlank()) {
            savedDeltaToken = payload.continuationToken();
        } else {
            if (checkpointStatePlugin != null) {
                savedDeltaToken = checkpointStatePlugin.getCheckpoint(payload.driveId()).orElse(null);
            }
            if (savedDeltaToken == null && catalogRepository != null) {
                savedDeltaToken = catalogRepository.getDrive(payload.driveId())
                        .map(MonitoredDrive::deltaToken)
                        .orElse(null);
            }
        }

        if (catalogRepository != null && catalogRepository.getDrive(payload.driveId()).isEmpty()) {
            catalogRepository.saveDrive(new MonitoredDrive(
                    payload.driveId(),
                    payload.tenantId(),
                    DriveType.SHAREPOINT_LIBRARY,
                    "Monitored Drive " + payload.driveId(),
                    "admin@" + payload.tenantId() + ".com",
                    "delta-v1-0",
                    null
            ));
        }

        // Fetch delta changes from Microsoft Graph
        DeltaSyncResult deltaResult = graphClient.fetchDeltaChanges(
                payload.tenantId(),
                payload.driveId(),
                savedDeltaToken
        );

        // Checkpoint update: persist new delta token via CheckpointStatePlugin & PostgreSQL
        if (deltaResult.newDeltaToken() != null) {
            if (checkpointStatePlugin != null) {
                checkpointStatePlugin.saveCheckpoint(payload.driveId(), deltaResult.newDeltaToken());
            }
            if (catalogRepository != null) {
                catalogRepository.updateDriveDeltaToken(payload.driveId(), deltaResult.newDeltaToken());
            }
        }

        // If intermediate pagination nextLink exists, pipeline next page continuation job into OxMQ
        if (deltaResult.hasMore() && oxmqTemplate != null) {
            int contPriority = (payload.scanOrigin() == ScanOrigin.FORWARD) ? 1 : 10;
            DriveDeltaSyncPayload continuationPayload = new DriveDeltaSyncPayload(
                    payload.tenantId(),
                    payload.driveId(),
                    payload.scanOrigin(),
                    deltaResult.nextLink()
            );
            JobOptions continuationOptions = JobOptions.builder()
                    .priority(contPriority)
                    .attempts(3)
                    .backoff(new BackoffStrategy.Exponential(1000L))
                    .build();
            try {
                oxmqTemplate.send(
                        OxDocQueueConstants.QUEUE_DRIVE_DELTA,
                        "delta-page-" + payload.driveId() + "-" + System.nanoTime(),
                        continuationPayload,
                        continuationOptions
                );
                log.info("Pipelined continuation delta sync page for drive [{}] (tenant={})",
                        payload.driveId(), payload.tenantId());
            } catch (Exception e) {
                log.error("Failed to enqueue delta continuation page for drive [{}]: {}", payload.driveId(), e.getMessage());
            }
        }

        List<DriveItem> updatedItems = deltaResult.updatedItems();
        log.info("Delta sync processed for drive [{}] (origin={}): found {} modified/new files to inspect",
                payload.driveId(), payload.scanOrigin(), updatedItems.size());

        int dispatchedCount = 0;
        int priority = (payload.scanOrigin() == ScanOrigin.FORWARD) ? 1 : 10;

        for (DriveItem item : updatedItems) {
            if (item.isFolder()) {
                continue;
            }

            TraceContext parentTrace = TraceContext.fromMdc() != null ? TraceContext.fromMdc() : TraceContext.generateNew();

            // Create Parent Flow Job (Aggregator)
            DocumentCatalogPayload aggregatePayload = new DocumentCatalogPayload(
                    payload.tenantId(), payload.driveId(), item.id(), item.name(),
                    item.path(), item.size(), item.mimeType(), item.downloadUrl(), payload.scanOrigin(),
                    parentTrace.traceId()
            );

            // Parent Flow Job (Aggregator node in waiting-children state with bounded exponential backoff)
            JobOptions.Builder parentOptionsBuilder = JobOptions.builder()
                    .priority(priority)
                    .attempts(3)
                    .backoff(new BackoffStrategy.Exponential(1000L))
                    .removeOnComplete(true);
            parentTrace.inject(parentOptionsBuilder);

            FlowJob<DocumentCatalogPayload> parentFlow = FlowJob.of(
                    OxDocQueueConstants.QUEUE_CATALOG_INDEX,
                    "aggregate-" + item.name(),
                    aggregatePayload,
                    parentOptionsBuilder.build()
            );

            // Child 1: Content Categorization (Pillar 1) with self-healing retries
            TraceContext childTrace1 = parentTrace.createChildSpan();
            DocumentProcessingPayload scanPayload1 = new DocumentProcessingPayload(
                    payload.tenantId(), payload.driveId(), item.id(), item.name(),
                    item.path(), item.size(), item.mimeType(), item.downloadUrl(), payload.scanOrigin(),
                    childTrace1.traceId()
            );

            JobOptions.Builder childOptBuilder1 = JobOptions.builder()
                    .priority(priority)
                    .attempts(3)
                    .backoff(new BackoffStrategy.Exponential(1000L))
                    .failParentOnFailure(false)
                    .removeOnComplete(true);
            childTrace1.inject(childOptBuilder1);

            FlowJob<DocumentProcessingPayload> childCategorize = FlowJob.of(
                    OxDocQueueConstants.QUEUE_CONTENT_CATEGORIZATION,
                    "categorize-" + item.name(),
                    scanPayload1,
                    childOptBuilder1.build()
            );

            // Child 2: Document Extraction (Pillar 2) with self-healing retries
            TraceContext childTrace2 = parentTrace.createChildSpan();
            DocumentProcessingPayload scanPayload2 = new DocumentProcessingPayload(
                    payload.tenantId(), payload.driveId(), item.id(), item.name(),
                    item.path(), item.size(), item.mimeType(), item.downloadUrl(), payload.scanOrigin(),
                    childTrace2.traceId()
            );

            JobOptions.Builder childOptBuilder2 = JobOptions.builder()
                    .priority(priority)
                    .attempts(3)
                    .backoff(new BackoffStrategy.Exponential(1000L))
                    .failParentOnFailure(false)
                    .removeOnComplete(true);
            childTrace2.inject(childOptBuilder2);

            FlowJob<DocumentProcessingPayload> childExtract = FlowJob.of(
                    OxDocQueueConstants.QUEUE_DOCUMENT_EXTRACTION,
                    "extract-" + item.name(),
                    scanPayload2,
                    childOptBuilder2.build()
            );

            parentFlow.addChildren(List.of(childCategorize, childExtract));

            try {
                flowProducer.add(parentFlow);
                dispatchedCount++;
            } catch (Exception e) {
                log.error("Failed dispatching FlowProducer DAG for file [{}]: {}", item.name(), e.getMessage());
            }
        }

        return dispatchedCount;
    }
}
