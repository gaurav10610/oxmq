package io.oxmq.example.dag;

import io.lettuce.core.RedisClient;
import io.oxmq.FlowProducer;
import io.oxmq.OxmqQueue;
import io.oxmq.OxmqWorker;
import io.oxmq.model.FlowJob;
import io.oxmq.model.JobState;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DagWorkflowComplexIntegrationTest {

    private RedisClient redisClient;
    private FlowProducer flowProducer;
    private String chunkQueue;
    private String assemblyQueue;
    private OxmqWorker<DagWorkflowExample.VideoChunkPayload> chunkWorker;
    private OxmqWorker<DagWorkflowExample.VideoAssemblyPayload> assemblyWorker;

    @BeforeEach
    void setUp() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        chunkQueue = "test-chunks-" + suffix;
        assemblyQueue = "test-assembly-" + suffix;
        redisClient = RedisClient.create("redis://localhost:6379");
        flowProducer = new FlowProducer(redisClient);
    }

    @AfterEach
    void tearDown() {
        if (chunkWorker != null) chunkWorker.close();
        if (assemblyWorker != null) assemblyWorker.close();
        if (flowProducer != null) flowProducer.close();

        // Clean queues
        new OxmqQueue<>(chunkQueue, new io.oxmq.client.RedisConnectionManager(redisClient), null, null, null, null).obliterate();
        new OxmqQueue<>(assemblyQueue, new io.oxmq.client.RedisConnectionManager(redisClient), null, null, null, null).obliterate();

        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    @Test
    @DisplayName("Parent task in wide DAG (10 children) should not execute until all 10 children complete")
    void testWideDagWorkflow() throws InterruptedException {
        int numChildren = 10;
        CountDownLatch childrenLatch = new CountDownLatch(numChildren);
        CountDownLatch parentLatch = new CountDownLatch(1);
        Set<Integer> completedChildren = Collections.synchronizedSet(new HashSet<>());
        AtomicBoolean parentRanPrematurely = new AtomicBoolean(false);

        // 1. Start Child Worker
        chunkWorker = OxmqWorker.<DagWorkflowExample.VideoChunkPayload>builder()
                .queueName(chunkQueue)
                .redisClient(redisClient)
                .payloadClass(DagWorkflowExample.VideoChunkPayload.class)
                .concurrency(10)
                .processor(job -> {
                    DagWorkflowExample.VideoChunkPayload data = job.getData();
                    Thread.sleep(50); // Simulate work
                    completedChildren.add(data.chunkIndex());
                    childrenLatch.countDown();
                    return Map.of("chunkIndex", data.chunkIndex(), "status", "OK");
                })
                .build();

        // 2. Start Parent Worker
        assemblyWorker = OxmqWorker.<DagWorkflowExample.VideoAssemblyPayload>builder()
                .queueName(assemblyQueue)
                .redisClient(redisClient)
                .payloadClass(DagWorkflowExample.VideoAssemblyPayload.class)
                .concurrency(2)
                .processor(job -> {
                    if (completedChildren.size() < numChildren) {
                        parentRanPrematurely.set(true);
                    }
                    parentLatch.countDown();
                    return "FINAL_MERGED_VIDEO";
                })
                .build();

        chunkWorker.start();
        assemblyWorker.start();

        // 3. Build & Submit 10-Child Flow
        FlowJob<DagWorkflowExample.VideoAssemblyPayload> parentJob = FlowJob.of(assemblyQueue, "assemble-video",
                new DagWorkflowExample.VideoAssemblyPayload("vid-999", numChildren, "MP4")
        );

        for (int i = 0; i < numChildren; i++) {
            parentJob.addChild(FlowJob.of(chunkQueue, "chunk-" + i,
                    new DagWorkflowExample.VideoChunkPayload("vid-999", i, "1080p")
            ));
        }

        String parentId = flowProducer.add(parentJob);
        assertThat(parentId).isNotBlank();

        // 4. Await completions
        boolean allChildrenDone = childrenLatch.await(10, TimeUnit.SECONDS);
        boolean parentDone = parentLatch.await(10, TimeUnit.SECONDS);

        assertThat(allChildrenDone).as("All 10 children should finish").isTrue();
        assertThat(parentDone).as("Parent assembly job should finish").isTrue();
        assertThat(parentRanPrematurely.get()).as("Parent should never run before all children complete").isFalse();
        assertThat(completedChildren).hasSize(numChildren);
    }
}
