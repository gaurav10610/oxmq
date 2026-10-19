package io.oxmq.oxguard.queue;

import io.oxmq.FlowProducer;
import io.oxmq.model.BackoffStrategy;
import io.oxmq.model.FlowJob;
import io.oxmq.model.JobOptions;
import io.oxmq.oxguard.connector.DeltaSyncResult;
import io.oxmq.oxguard.connector.GraphClient;
import io.oxmq.oxguard.model.DriveDeltaSyncPayload;
import io.oxmq.oxguard.model.DriveItem;
import io.oxmq.oxguard.model.FileAggregatePayload;
import io.oxmq.oxguard.model.FileScanPayload;
import io.oxmq.oxguard.model.MonitoredDrive;
import io.oxmq.oxguard.model.ScanOrigin;
import io.oxmq.oxguard.service.FileInventoryRepository;
import io.oxmq.oxguard.service.TenantCooldownService;
import io.oxmq.plugin.CheckpointStatePlugin;
import io.oxmq.spring.annotation.Header;
import io.oxmq.spring.annotation.OxmqListener;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Executes incremental delta synchronization on drives.
 * Enforces Forward (Priority 1) vs Backward (Priority 10) priority arbitration,
 * yields capacity when tenant token headroom < 25%, and dispatches OxMQ FlowProducer DAGs.
 * Checkpoints delta tokens using native OxMQ CheckpointStatePlugin and PostgreSQL for seamless crash recovery.
 */
@Component
public class DriveDeltaSyncWorker {

    private static final Logger log = LoggerFactory.getLogger(DriveDeltaSyncWorker.class);

    private final GraphClient graphClient;
    private final TenantCooldownService cooldownService;
    private final FlowProducer flowProducer;
    private final FileInventoryRepository fileInventoryRepository;
    private final CheckpointStatePlugin checkpointStatePlugin;

    @Autowired
    public DriveDeltaSyncWorker(
            GraphClient graphClient,
            TenantCooldownService cooldownService,
            FlowProducer flowProducer,
            FileInventoryRepository fileInventoryRepository,
            @Autowired(required = false) CheckpointStatePlugin checkpointStatePlugin
    ) {
        this.graphClient = graphClient;
        this.cooldownService = cooldownService;
        this.flowProducer = flowProducer;
        this.fileInventoryRepository = fileInventoryRepository;
        this.checkpointStatePlugin = checkpointStatePlugin;
    }

    public DriveDeltaSyncWorker(
            GraphClient graphClient,
            TenantCooldownService cooldownService,
            FlowProducer flowProducer,
            FileInventoryRepository fileInventoryRepository
    ) {
        this(graphClient, cooldownService, flowProducer, fileInventoryRepository, null);
    }

    public DriveDeltaSyncWorker(
            GraphClient graphClient,
            TenantCooldownService cooldownService,
            FlowProducer flowProducer
    ) {
        this(graphClient, cooldownService, flowProducer, null, null);
    }

    @OxmqListener(queue = OxGuardQueueConstants.QUEUE_DRIVE_DELTA, concurrency = 20)
    public int processDeltaSync(
            DriveDeltaSyncPayload payload,
            @Header("jobId") String jobId
    ) {
        cooldownService.recordTenantApiCall(payload.tenantId(), payload.scanOrigin());

        // Checkpoint Recovery: load existing delta token from CheckpointStatePlugin (or DB fallback)
        String savedDeltaToken = null;
        if (checkpointStatePlugin != null) {
            savedDeltaToken = checkpointStatePlugin.getCheckpoint(payload.driveId()).orElse(null);
        }
        if (savedDeltaToken == null && fileInventoryRepository != null) {
            savedDeltaToken = fileInventoryRepository.getDrive(payload.driveId())
                    .map(MonitoredDrive::deltaToken)
                    .orElse(null);
        }

        DeltaSyncResult deltaResult = graphClient.fetchDeltaChanges(payload.tenantId(), payload.driveId(), savedDeltaToken);

        // Checkpoint update: persist new delta token via CheckpointStatePlugin & PostgreSQL
        if (deltaResult.newDeltaToken() != null) {
            if (checkpointStatePlugin != null) {
                checkpointStatePlugin.saveCheckpoint(payload.driveId(), deltaResult.newDeltaToken());
            }
            if (fileInventoryRepository != null) {
                fileInventoryRepository.updateDriveDeltaToken(payload.driveId(), deltaResult.newDeltaToken());
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

            // Create Parent Flow Job (Aggregator)
            FileAggregatePayload aggregatePayload = new FileAggregatePayload(
                    payload.tenantId(), payload.driveId(), item.id(), item.name(),
                    item.path(), item.size(), item.mimeType(), item.downloadUrl(), payload.scanOrigin()
            );

            // Parent Flow Job (Aggregator node in waiting-children state with bounded exponential backoff)
            JobOptions parentOptions = JobOptions.builder()
                    .priority(priority)
                    .attempts(3)
                    .backoff(new BackoffStrategy.Exponential(1000L))
                    .build();

            FlowJob<FileAggregatePayload> parentFlow = FlowJob.of(
                    OxGuardQueueConstants.QUEUE_FILE_AGGREGATE,
                    "aggregate-" + item.name(),
                    aggregatePayload,
                    parentOptions
            );

            // Child 1: Permission Inspection (Pillar 1) with self-healing retries
            FileScanPayload scanPayload = new FileScanPayload(
                    payload.tenantId(), payload.driveId(), item.id(), item.name(),
                    item.path(), item.size(), item.mimeType(), item.downloadUrl(), payload.scanOrigin()
            );

            FlowJob<FileScanPayload> childPerm = FlowJob.of(
                    OxGuardQueueConstants.QUEUE_FILE_PERMISSION,
                    "perm-" + item.name(),
                    scanPayload,
                    JobOptions.builder()
                            .priority(priority)
                            .attempts(3)
                            .backoff(new BackoffStrategy.Exponential(1000L))
                            .failParentOnFailure(false)
                            .build()
            );

            // Child 2: Malware Streaming Inspection (Pillar 2) with self-healing retries
            FlowJob<FileScanPayload> childMalware = FlowJob.of(
                    OxGuardQueueConstants.QUEUE_FILE_MALWARE,
                    "malware-" + item.name(),
                    scanPayload,
                    JobOptions.builder()
                            .priority(priority)
                            .attempts(3)
                            .backoff(new BackoffStrategy.Exponential(1000L))
                            .failParentOnFailure(false)
                            .build()
            );

            parentFlow.addChildren(List.of(childPerm, childMalware));

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
