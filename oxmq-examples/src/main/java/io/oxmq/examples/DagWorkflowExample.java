package io.oxmq.examples;

import io.lettuce.core.RedisClient;
import io.oxmq.FlowProducer;
import io.oxmq.OxmqQueue;
import io.oxmq.OxmqWorker;
import io.oxmq.model.FlowJob;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real-World Recipe: Parent-Child DAG Workflows (FlowProducer)
 * Use Case: Multi-stage media transcoding pipeline (split video -> transcode chunks in parallel -> merge in parent).
 */
public class DagWorkflowExample {

    private static final Logger log = LoggerFactory.getLogger(DagWorkflowExample.class);

    public record VideoChunkPayload(String videoId, int chunkIndex, String resolution) {}
    public record VideoAssemblyPayload(String videoId, int totalChunks, String targetFormat) {}

    public static void main(String[] args) throws Exception {
        String redisUri = System.getProperty("oxmq.redis.uri", "redis://localhost:6379");
        RedisClient redisClient = RedisClient.create(redisUri);

        String chunkQueue = "video-chunks";
        String assemblyQueue = "video-assembly";

        // 1. Worker for Child Tasks (Chunk Transcoding)
        OxmqWorker<VideoChunkPayload> chunkWorker = OxmqWorker.<VideoChunkPayload>builder()
                .queueName(chunkQueue)
                .redisClient(redisClient)
                .payloadClass(VideoChunkPayload.class)
                .concurrency(10)
                .processor(job -> {
                    VideoChunkPayload chunk = job.getData();
                    log.info("Transcoding chunk #{} ({}p) for video {} on Virtual Thread...",
                            chunk.chunkIndex(), chunk.resolution(), chunk.videoId());

                    Thread.sleep(150); // Simulate video transcoding I/O
                    return Map.of("chunkIndex", chunk.chunkIndex(), "fileSizeKb", 4096, "status", "TRANSCODED");
                })
                .build();

        // 2. Worker for Parent Task (Assembly after all children complete)
        OxmqWorker<VideoAssemblyPayload> assemblyWorker = OxmqWorker.<VideoAssemblyPayload>builder()
                .queueName(assemblyQueue)
                .redisClient(redisClient)
                .payloadClass(VideoAssemblyPayload.class)
                .concurrency(5)
                .processor(job -> {
                    VideoAssemblyPayload assembly = job.getData();
                    log.info(">>> All child chunks finished! Starting assembly of video {} in {}...",
                            assembly.videoId(), assembly.targetFormat());

                    Thread.sleep(200); // Simulate final video assembly
                    return "VIDEO_READY_URL: https://cdn.example.com/videos/" + assembly.videoId() + ".mp4";
                })
                .build();

        chunkWorker.start();
        assemblyWorker.start();

        // 3. Define the DAG Tree using FlowProducer
        FlowProducer flowProducer = new FlowProducer(redisClient);

        FlowJob<VideoAssemblyPayload> parentJob = FlowJob.of(assemblyQueue, "assemble-full-video",
                new VideoAssemblyPayload("vid_4k_901", 3, "MP4_H264")
        );

        parentJob.addChild(FlowJob.of(chunkQueue, "transcode-chunk-0", new VideoChunkPayload("vid_4k_901", 0, "1080")));
        parentJob.addChild(FlowJob.of(chunkQueue, "transcode-chunk-1", new VideoChunkPayload("vid_4k_901", 1, "1080")));
        parentJob.addChild(FlowJob.of(chunkQueue, "transcode-chunk-2", new VideoChunkPayload("vid_4k_901", 2, "1080")));

        log.info("Submitting parent-child DAG workflow to Redis...");
        String parentId = flowProducer.add(parentJob);
        log.info("DAG Root Parent Job created with ID: {}", parentId);

        // Wait for pipeline execution
        TimeUnit.SECONDS.sleep(4);

        chunkWorker.close();
        assemblyWorker.close();
        flowProducer.close();
        redisClient.shutdown();
        log.info("DAG Workflow recipe finished successfully.");
    }
}
