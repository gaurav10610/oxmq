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

        // Define Flow with 2 children
        FlowJob<String> parentJob = FlowJob.of(parentQueue, "parent-task", "parent-payload");
        parentJob.addChild(FlowJob.of(childQueue, "child-1", "child-1-data"));
        parentJob.addChild(FlowJob.of(childQueue, "child-2", "child-2-data"));

        flowProducer.add(parentJob);

        // Assert that children are in wait, parent is not yet in wait
        assertThat(cQueue.count(JobState.WAITING)).isEqualTo(2);

        // Start workers after initial flow structure is verified
        childWorker.start();
        parentWorker.start();

        // Wait for all to complete
        boolean completed = parentLatch.await(10, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
        assertThat(childCompletedCount.get()).isEqualTo(2);

        childWorker.close();
        parentWorker.close();
    }

    @Test
    void testFlowLeafNodeDelayAndPriority() throws Exception {
        FlowJob<String> parent = FlowJob.of(parentQueue, "parent-task", "parent-data");
        FlowJob<String> delayedChild = FlowJob.of(childQueue, "delayed-child", "delayed-data",
                io.oxmq.model.JobOptions.builder().delayMs(5000).build());
        FlowJob<String> prioritizedChild = FlowJob.of(childQueue, "prio-child", "prio-data",
                io.oxmq.model.JobOptions.builder().priority(5).build());

        parent.addChild(delayedChild);
        parent.addChild(prioritizedChild);

        flowProducer.add(parent);

        // Delayed leaf child must be in DELAYED state
        assertThat(cQueue.count(JobState.DELAYED)).isEqualTo(1);
        // Prioritized leaf child must be in WAITING state (prioritized jobs in BullMQ sit in wait/prioritized zset)
        assertThat(cQueue.count(JobState.WAITING) + cQueue.count(JobState.PRIORITIZED)).isGreaterThanOrEqualTo(1);
    }

    @Test
    void testFlowTreeWithUnsuccessfulChild() throws Exception {
        FlowJob<String> parent = FlowJob.of(parentQueue, "parent-root", "root-data");
        FlowJob<String> failChild = FlowJob.of(childQueue, "fail-child", "fail-data",
                io.oxmq.model.JobOptions.builder().failParentOnFailure(true).build());

        parent.addChild(failChild);
        String parentJobId = flowProducer.add(parent);

        // Fail the child
        OxmqWorker<String> childWorker = OxmqWorker.<String>builder()
                .queueName(childQueue)
                .redisClient(redisClient)
                .payloadClass(String.class)
                .pollIntervalMs(20)
                .processor(job -> {
                    throw new RuntimeException("Child failure");
                })
                .build();

        CountDownLatch failLatch = new CountDownLatch(1);
        childWorker.onFailed((job, err) -> failLatch.countDown());
        childWorker.start();

        assertThat(failLatch.await(5, TimeUnit.SECONDS)).isTrue();
        childWorker.close();

        // Retrieve flow tree via getFlow
        io.oxmq.model.JobNode flowNode = flowProducer.getFlow(parentQueue, parentJobId);
        assertThat(flowNode).isNotNull();
        assertThat(flowNode.getChildren()).hasSize(1);
        assertThat(flowNode.getChildren().get(0).getJob().getName()).isEqualTo("fail-child");
    }
}
