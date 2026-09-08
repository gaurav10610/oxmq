package io.cloudbridge.worker;

import io.cloudbridge.client.BoxClient;
import io.cloudbridge.client.DropboxClient;
import io.cloudbridge.client.GitHubClient;
import io.cloudbridge.dto.FileTransferPayload;
import io.cloudbridge.dto.FileTransferResult;
import io.cloudbridge.dto.SyncManifest;
import io.cloudbridge.dto.SyncRequest;
import io.cloudbridge.service.SyncWorkflowService;
import io.oxmq.model.Job;
import io.oxmq.spring.annotation.OxmqListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class SyncWorkers {
    private static final Logger log = LoggerFactory.getLogger(SyncWorkers.class);

    private final GitHubClient gitHubClient;
    private final DropboxClient dropboxClient;
    private final BoxClient boxClient;

    public SyncWorkers(GitHubClient gitHubClient, DropboxClient dropboxClient, BoxClient boxClient) {
        this.gitHubClient = gitHubClient;
        this.dropboxClient = dropboxClient;
        this.boxClient = boxClient;
    }

    /**
     * High-concurrency worker running across Java 21 Virtual Threads!
     */
    @OxmqListener(queue = SyncWorkflowService.FILE_TRANSFER_QUEUE, concurrency = 20, virtualThreads = true)
    public FileTransferResult processFileTransfer(Job<FileTransferPayload> job) throws InterruptedException {
        FileTransferPayload payload = job.getData();
        long start = System.currentTimeMillis();

        log.info("[VirtualThread: {}] Starting file transfer for [{}] ({} bytes) in parent [{}]",
                Thread.currentThread().isVirtual() ? "V-Thread" : "Platform",
                payload.path(), payload.sizeBytes(), payload.parentJobId());

        // 1. Download stage (25% progress)
        job.updateProgress(25);
        byte[] content = gitHubClient.download(payload.source(), payload.path());
        Thread.sleep(100); // Simulate network read streaming

        // 2. Transfer & Verification stage (60% progress)
        job.updateProgress(60);
        Thread.sleep(150); // Simulate network latency and checksumming

        // 3. Upload stage to target storage (90% progress)
        job.updateProgress(90);
        if (payload.destination().startsWith("dropbox")) {
            dropboxClient.upload(payload.destination(), payload.path(), content, "application/octet-stream");
        } else {
            boxClient.upload(payload.destination(), payload.path(), content, "application/octet-stream");
        }

        // 4. Finished (100% progress)
        job.updateProgress(100);
        long duration = System.currentTimeMillis() - start;

        log.info("Completed transfer for [{}] in {}ms", payload.path(), duration);
        return new FileTransferResult(payload.fileId(), payload.path(), payload.sizeBytes(), payload.sha256(), "SUCCESS", duration);
    }

    /**
     * Parent DAG Orchestration worker: triggers only when ALL child file transfers have completed!
     */
    @OxmqListener(queue = SyncWorkflowService.ORCHESTRATION_QUEUE, concurrency = 5, virtualThreads = true)
    public SyncManifest processSyncCompletion(Job<SyncRequest> job) {
        SyncRequest request = job.getData();
        Map<String, Object> childrenValues = job.getChildrenValues();

        log.info("Parent Job [{}] woke up! All child transfers resolved. Processing child results...", job.getId());

        job.updateProgress(50);

        List<FileTransferResult> results = new ArrayList<>();
        long totalBytes = 0;

        if (childrenValues != null) {
            for (Map.Entry<String, Object> entry : childrenValues.entrySet()) {
                if (entry.getValue() instanceof Map<?, ?> map) {
                    String fileId = (String) map.get("fileId");
                    String path = (String) map.get("path");
                    Number bytes = (Number) map.get("bytesTransferred");
                    String sha = (String) map.get("sha256");
                    String status = (String) map.get("status");
                    Number dur = (Number) map.get("durationMs");

                    long b = bytes != null ? bytes.longValue() : 0;
                    totalBytes += b;
                    results.add(new FileTransferResult(fileId, path, b, sha, status, dur != null ? dur.longValue() : 0));
                } else if (entry.getValue() instanceof FileTransferResult ftr) {
                    totalBytes += ftr.bytesTransferred();
                    results.add(ftr);
                }
            }
        }

        job.updateProgress(100);

        SyncManifest manifest = new SyncManifest(
                job.getId(),
                request.getSource(),
                request.getDestination(),
                results.size(),
                totalBytes,
                results,
                Instant.now()
        );

        log.info("Sync [{}] COMPLETE! Transferred {} files ({} bytes) successfully.",
                job.getId(), manifest.totalFiles(), manifest.totalBytes());

        return manifest;
    }
}
