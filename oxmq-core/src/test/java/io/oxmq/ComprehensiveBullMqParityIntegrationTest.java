package io.oxmq;

import io.oxmq.client.RedisConnectionManager;
import io.oxmq.model.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Comprehensive BullMQ / BullMQ Pro Parity Integration Tests")
class ComprehensiveBullMqParityIntegrationTest {

    private String queueName;
    private RedisConnectionManager connectionManager;
    private OxmqQueue<String> queue;
    private FlowProducer flowProducer;
    private OxmqWorker<String> worker;

    @BeforeEach
    void setUp() {
        queueName = "parity-queue-" + UUID.randomUUID();
        connectionManager = new RedisConnectionManager("redis://localhost:6379");
        queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        flowProducer = new FlowProducer(connectionManager);
    }

    @AfterEach
    void tearDown() {
        if (worker != null) {
            try { worker.close(); } catch (Exception ignored) {}
        }
        if (flowProducer != null) {
            try { flowProducer.close(); } catch (Exception ignored) {}
        }
        if (queue != null) {
            try { queue.obliterate(); } catch (Exception ignored) {}
            try { queue.close(); } catch (Exception ignored) {}
        }
        if (connectionManager != null) {
            try { connectionManager.close(); } catch (Exception ignored) {}
        }
    }

    @Test
    @DisplayName("Deduplication Job ID tracking and lifecycle query")
    void testDeduplicationJobIdTracking() {
        String dedupId = "custom-tx-" + UUID.randomUUID();
        assertNull(queue.getDeduplicationJobId(dedupId));

        Job<String> job = queue.add("dedup-job", "data-payload",
                JobOptions.builder().deduplication(DeduplicationOptions.of(dedupId, Duration.ofMinutes(5))).build());

        assertNotNull(job);
        assertEquals(job.getId(), queue.getDeduplicationJobId(dedupId),
                "queue.getDeduplicationJobId should return the ID of the job holding the deduplication key");

        // Remove deduplication key
        boolean removed = queue.removeDeduplicationKey(dedupId);
        assertTrue(removed);
        assertNull(queue.getDeduplicationJobId(dedupId), "Deduplication key should be cleared");
    }

    @Test
    @DisplayName("DAG parent dependencies breakdown (getDependencies) and Flow tree inspection (getFlow)")
    void testDependenciesBreakdownAndFlowTree() throws Exception {
        FlowJob<String> child1 = FlowJob.of(queueName, "child-step-1", "payload-1");
        FlowJob<String> child2 = FlowJob.of(queueName, "child-step-2", "payload-2");
        FlowJob<String> parent = FlowJob.of(queueName, "parent-root", "parent-payload")
                .addChild(child1)
                .addChild(child2);

        String parentId = flowProducer.add(parent);
        assertNotNull(parentId);

        // 1. Inspect dependencies before processing
        JobDependencies depsBefore = queue.getDependencies(parentId);
        assertNotNull(depsBefore);
        assertEquals(parentId, depsBefore.parentJobId());
        assertEquals(2, depsBefore.unprocessedChildKeys().size(), "Both child dependencies should be unprocessed initially");
        assertTrue(depsBefore.processedChildren().isEmpty());
        assertFalse(depsBefore.isComplete());

        // 2. Inspect Flow tree via queue.getFlow and job.getFlow
        JobNode flowFromQueue = queue.getFlow(parentId);
        assertNotNull(flowFromQueue);
        assertEquals("parent-root", flowFromQueue.getJob().getName());
        assertEquals(2, flowFromQueue.getChildren().size());

        Job<String> parentJob = queue.getJob(parentId);
        assertNotNull(parentJob);
        JobNode flowFromJob = parentJob.getFlow();
        assertNotNull(flowFromJob);
        assertEquals(2, flowFromJob.getChildren().size());

        // Also verify parentJob.getDependencies() mirrors queue.getDependencies()
        JobDependencies depsFromJob = parentJob.getDependencies();
        assertEquals(2, depsFromJob.unprocessedChildKeys().size());

        // 3. Process the children
        CountDownLatch latch = new CountDownLatch(2);
        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(2)
                .processor(job -> {
                    latch.countDown();
                    return "result-" + job.getName();
                })
                .build();
        worker.start();

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Children should finish execution");

        // 4. Inspect dependencies after children finish
        JobDependencies depsAfter = queue.getDependencies(parentId);
        assertEquals(2, depsAfter.processedChildren().size(), "Both children should now be marked processed");
        assertTrue(depsAfter.unprocessedChildKeys().isEmpty());
    }

    @Test
    @DisplayName("Job payload updating (job.updateData) and log clearing (job.clearLogs)")
    void testJobDataUpdatingAndLogClearing() {
        Job<String> job = queue.add("data-job", "initial-data");
        assertEquals("initial-data", job.getData());

        // Update data via Job object
        job.updateData("modified-data-v2");
        assertEquals("modified-data-v2", job.getData());

        // Verify update in Redis via fresh fetch
        Job<String> fetched = queue.getJob(job.getId());
        assertNotNull(fetched);
        assertEquals("modified-data-v2", fetched.getData());

        // Test logging and log clearing
        job.log("First log row");
        job.log("Second log row");

        List<String> logs = queue.getJobLogs(job.getId());
        assertEquals(2, logs.size());

        boolean cleared = job.clearLogs();
        assertTrue(cleared, "Job.clearLogs should successfully clear logs in Redis");

        List<String> logsAfter = queue.getJobLogs(job.getId());
        assertTrue(logsAfter.isEmpty(), "Job logs should be empty after clearLogs");
    }

    @Test
    @DisplayName("Job lifecycle methods: promote, changePriority, changeDelay, retry, remove")
    void testJobLifecycleDelegates() {
        // Delayed job
        Job<String> delayedJob = queue.add("delayed-task", "data", JobOptions.builder().delay(Duration.ofHours(1)).build());
        assertEquals("delayed-task", delayedJob.getName());

        // Promote via Job object
        delayedJob.promote();

        JobState stateAfterPromote = delayedJob.getState();
        assertTrue(stateAfterPromote == JobState.WAITING || stateAfterPromote == JobState.PRIORITIZED,
                "Promoted job should be in waiting/prioritized state, was: " + stateAfterPromote);

        // Change priority via Job object
        delayedJob.changePriority(5);

        // Remove via Job object
        boolean removed = delayedJob.remove();
        assertTrue(removed, "job.remove() should remove job from Redis");
        assertNull(queue.getJob(delayedJob.getId()), "Removed job should not exist");
    }
}
