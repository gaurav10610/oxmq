package io.cloudbridge.service;

import io.lettuce.core.api.sync.RedisCommands;
import io.cloudbridge.client.BoxClient;
import io.cloudbridge.client.DropboxClient;
import io.cloudbridge.client.GitHubClient;
import io.cloudbridge.dto.FileTransferPayload;
import io.cloudbridge.dto.StorageFile;
import io.cloudbridge.dto.SyncRequest;
import io.oxmq.FlowProducer;
import io.oxmq.OxmqQueue;
import io.oxmq.Queue;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.model.FlowJob;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import io.oxmq.serializer.JacksonJobSerializer;
import io.oxmq.serializer.JobSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class SyncWorkflowService {
    private static final Logger log = LoggerFactory.getLogger(SyncWorkflowService.class);

    public static final String ORCHESTRATION_QUEUE = "sync-orchestration-queue";
    public static final String FILE_TRANSFER_QUEUE = "file-transfer-queue";

    private final FlowProducer flowProducer;
    private final RedisConnectionManager connectionManager;
    private final GitHubClient gitHubClient;
    private final DropboxClient dropboxClient;
    private final BoxClient boxClient;
    private final io.oxmq.Queue<SyncRequest> orchestrationQueue;
    private final JobSerializer serializer = new JacksonJobSerializer();

    public SyncWorkflowService(FlowProducer flowProducer,
                               RedisConnectionManager connectionManager,
                               GitHubClient gitHubClient,
                               DropboxClient dropboxClient,
                               BoxClient boxClient) {
        this.flowProducer = flowProducer;
        this.connectionManager = connectionManager;
        this.gitHubClient = gitHubClient;
        this.dropboxClient = dropboxClient;
        this.boxClient = boxClient;
        this.orchestrationQueue = OxmqQueue.<SyncRequest>builder()
                .name(ORCHESTRATION_QUEUE)
                .connectionManager(connectionManager)
                .payloadClass(SyncRequest.class)
                .build();
    }

    public String triggerSync(SyncRequest request) {
        if (request.getDropboxToken() != null && !request.getDropboxToken().isBlank()) {
            dropboxClient.setRuntimeToken(request.getDropboxToken().trim());
        }
        if (request.getBoxToken() != null && !request.getBoxToken().isBlank()) {
            boxClient.setRuntimeToken(request.getBoxToken().trim());
        }

        String syncId = "sync-" + UUID.randomUUID().toString().substring(0, 8);
        log.info("Starting CloudBridge Sync [{}] from [{}] to [{}]", syncId, request.getSource(), request.getDestination());

        // 1. Scan files from source
        List<StorageFile> files = gitHubClient.listFiles(request.getSource());

        // Store sync file count in Redis
        RedisCommands<String, String> sync = connectionManager.sync();
        sync.set("bull:meta:sync:" + syncId + ":fileCount", String.valueOf(files.size()));

        // 2. Build parent orchestration job
        FlowJob<SyncRequest> parentFlow = FlowJob.of(
                ORCHESTRATION_QUEUE,
                "orchestrate-sync-" + syncId,
                request,
                JobOptions.builder()
                        .jobId(syncId)
                        .attempts(3)
                        .fixedBackoff(Duration.ofSeconds(2))
                        .build()
        );

        // 3. Fan-out child file transfer jobs
        List<FlowJob<?>> childJobs = new ArrayList<>();
        for (int i = 0; i < files.size(); i++) {
            StorageFile file = files.get(i);
            FileTransferPayload payload = new FileTransferPayload(
                    "file-" + (i + 1),
                    file.path(),
                    file.sizeBytes(),
                    file.sha256(),
                    request.getSource(),
                    request.getDestination(),
                    syncId
            );

            FlowJob<FileTransferPayload> childFlow = FlowJob.of(
                    FILE_TRANSFER_QUEUE,
                    "transfer-" + file.path(),
                    payload,
                    JobOptions.builder()
                            .jobId(syncId + ":" + (i + 1))
                            .attempts(3)
                            .fixedBackoff(Duration.ofMillis(500))
                            .build()
            );
            childJobs.add(childFlow);
        }

        parentFlow.addChildren(childJobs);

        // 4. Atomically dispatch DAG via OxMQ FlowProducer
        String dispatchedJobId = flowProducer.add(parentFlow);
        log.info("Successfully dispatched CloudBridge DAG Flow [{}]: 1 parent job waiting on {} child transfer jobs",
                syncId, childJobs.size());

        return dispatchedJobId;
    }

    public Map<String, Object> getSyncDetails(String syncId) {
        Job<SyncRequest> parentJob = orchestrationQueue.getJob(syncId);
        if (parentJob == null) {
            return null;
        }

        RedisCommands<String, String> sync = connectionManager.sync();
        String fileCountStr = sync.get("bull:meta:sync:" + syncId + ":fileCount");
        int fileCount = fileCountStr != null ? Integer.parseInt(fileCountStr) : 8;

        List<Map<String, Object>> childFiles = new ArrayList<>();
        int completedCount = 0;
        int sumProgress = 0;

        for (int i = 1; i <= fileCount; i++) {
            String childJobId = syncId + ":" + i;
            String childKey = "bull:file-transfer-queue:" + childJobId;
            Map<String, String> childHash = sync.hgetall(childKey);

            Map<String, Object> fileInfo = new HashMap<>();
            fileInfo.put("jobId", childJobId);

            if (childHash != null && !childHash.isEmpty()) {
                String dataJson = childHash.get("data");
                if (dataJson != null) {
                    try {
                        FileTransferPayload payload = serializer.deserialize(dataJson, FileTransferPayload.class);
                        fileInfo.put("path", payload.path());
                        fileInfo.put("sizeBytes", payload.sizeBytes());
                        fileInfo.put("sha256", payload.sha256());
                    } catch (Exception e) {
                        fileInfo.put("path", childHash.getOrDefault("name", "file-" + i));
                    }
                }

                int prog = 0;
                if (childHash.containsKey("progress")) {
                    try { prog = Integer.parseInt(childHash.get("progress")); } catch (Exception ignored) {}
                }
                fileInfo.put("progress", prog);
                sumProgress += prog;

                if (childHash.containsKey("finishedOn")) {
                    fileInfo.put("status", "COMPLETED");
                    fileInfo.put("progress", 100);
                    completedCount++;
                } else if (childHash.containsKey("processedOn")) {
                    fileInfo.put("status", "STREAMING");
                } else {
                    fileInfo.put("status", "WAITING");
                }
            } else {
                fileInfo.put("path", "file-" + i);
                fileInfo.put("progress", 0);
                fileInfo.put("status", "QUEUED");
            }
            childFiles.add(fileInfo);
        }

        boolean isCompleted = parentJob.getFinishedOn() != null || completedCount == fileCount;
        int overallProgress = isCompleted ? 100 : (fileCount > 0 ? sumProgress / fileCount : 0);

        Map<String, Object> resp = new HashMap<>();
        resp.put("syncId", syncId);
        resp.put("name", parentJob.getName());
        resp.put("status", isCompleted ? "COMPLETED" : "WAITING_CHILDREN");
        resp.put("progress", overallProgress);
        resp.put("totalFiles", fileCount);
        resp.put("completedFiles", completedCount);
        resp.put("isCompleted", isCompleted);
        resp.put("finishedOn", parentJob.getFinishedOn());
        resp.put("returnvalue", parentJob.getReturnvalue());
        resp.put("files", childFiles);

        return resp;
    }

    public Map<String, Long> getQueueStats() {
        OxmqQueue<?> orchQueue = OxmqQueue.builder().name(ORCHESTRATION_QUEUE).connectionManager(connectionManager).build();
        OxmqQueue<?> transQueue = OxmqQueue.builder().name(FILE_TRANSFER_QUEUE).connectionManager(connectionManager).build();

        return Map.of(
                "orchestration_waiting", orchQueue.count(JobState.WAITING),
                "orchestration_waiting_children", orchQueue.count(JobState.WAITING_CHILDREN),
                "orchestration_completed", orchQueue.count(JobState.COMPLETED),
                "transfer_waiting", transQueue.count(JobState.WAITING),
                "transfer_active", transQueue.count(JobState.ACTIVE),
                "transfer_completed", transQueue.count(JobState.COMPLETED),
                "transfer_failed", transQueue.count(JobState.FAILED)
        );
    }
}
