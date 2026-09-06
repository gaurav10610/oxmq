package io.oxmq;

import io.lettuce.core.RedisClient;
import io.oxmq.model.FlowJob;
import io.oxmq.model.JobState;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FlowProducerIntegrationTest {

    private RedisClient redisClient;
    private String childQueue;
    private String parentQueue;
    private OxmqQueue<String> pQueue;
    private OxmqQueue<String> cQueue;
    private FlowProducer flowProducer;

    @BeforeEach
    void setUp() {
        String redisUri = System.getProperty("oxmq.redis.uri", "redis://localhost:6379");
        redisClient = RedisClient.create(redisUri);
        childQueue = "test-flow-child-" + System.currentTimeMillis();
        parentQueue = "test-flow-parent-" + System.currentTimeMillis();

        pQueue = OxmqQueue.<String>builder().name(parentQueue).redisClient(redisClient).payloadClass(String.class).build();
        cQueue = OxmqQueue.<String>builder().name(childQueue).redisClient(redisClient).payloadClass(String.class).build();
        flowProducer = new FlowProducer(redisClient);
    }

    @AfterEach
    void tearDown() {
        if (pQueue != null) {
            try { pQueue.obliterate(); pQueue.close(); } catch (Exception ignored) {}
        }
        if (cQueue != null) {
            try { cQueue.obliterate(); cQueue.close(); } catch (Exception ignored) {}
        }
        if (flowProducer != null) {
            flowProducer.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    @Test
    void testParentChildDagResolution() throws Exception {
        CountDownLatch parentLatch = new CountDownLatch(1);
        AtomicInteger childCompletedCount = new AtomicInteger(0);

        // Child worker
        OxmqWorker<String> childWorker = OxmqWorker.<String>builder()
                .queueName(childQueue)
                .redisClient(redisClient)
                .payloadClass(String.class)
                .pollIntervalMs(20)
                .processor(job -> {
                    childCompletedCount.incrementAndGet();
                    return "CHILD_RESULT_" + job.getName();
                })
                .build();

        // Parent worker
        OxmqWorker<String> parentWorker = OxmqWorker.<String>builder()
                .queueName(parentQueue)
                .redisClient(redisClient)
                .payloadClass(String.class)
                .pollIntervalMs(20)
                .processor(job -> {
                    parentLatch.countDown();
                    return "PARENT_DONE";
                })
                .build();

        childWorker.start();
        parentWorker.start();

        // Define Flow with 2 children
        FlowJob<String> parentJob = FlowJob.of(parentQueue, "parent-task", "parent-payload");
        parentJob.addChild(FlowJob.of(childQueue, "child-1", "child-1-data"));
        parentJob.addChild(FlowJob.of(childQueue, "child-2", "child-2-data"));

        flowProducer.add(parentJob);

        // Assert that children are in wait, parent is not yet in wait
        assertThat(cQueue.count(JobState.WAITING)).isEqualTo(2);

        // Wait for all to complete
        boolean completed = parentLatch.await(5, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
        assertThat(childCompletedCount.get()).isEqualTo(2);

        childWorker.close();
        parentWorker.close();
    }
}
