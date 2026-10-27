package io.oxmq;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.oxmq.client.QueueKeys;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.model.FlowJob;
import io.oxmq.model.Job;
import io.oxmq.model.JobNode;
import io.oxmq.model.JobOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RecurringSchedulerAndRetryObservabilityIntegrationTest {

    private static final String REDIS_URI = "redis://localhost:6379";
    private RedisConnectionManager connectionManager;
    private List<Queue<?>> queuesToClean;
    private List<Worker<?>> workersToStop;

    @BeforeEach
    void setUp() {
        connectionManager = new RedisConnectionManager(REDIS_URI);
        queuesToClean = new ArrayList<>();
        workersToStop = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        for (Worker<?> worker : workersToStop) {
            try {
                worker.close();
            } catch (Exception ignored) {}
        }
        for (Queue<?> queue : queuesToClean) {
            try {
                queue.obliterate();
                queue.close();
            } catch (Exception ignored) {}
        }
        if (connectionManager != null) {
            connectionManager.close();
        }
    }

    @Test
    @DisplayName("Observability: failedReason and stacktrace are preserved in Redis during retry delays")
    void testFailureReasonPreservedDuringRetryDelay() throws Exception {
        String queueName = "retry-obs-" + UUID.randomUUID();
        Queue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        JobOptions opts = JobOptions.builder()
                .attempts(3)
                .backoff(io.oxmq.model.BackoffStrategy.fixed(Duration.ofSeconds(20)))
                .build();

        Job<String> enqueued = queue.add("flaky-task", "payload-data", opts);
        assertNotNull(enqueued.getId());

        CountDownLatch failureLatch = new CountDownLatch(1);
        Worker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .processor(job -> {
                    throw new RuntimeException("Simulated upstream 503 outage for testing");
                })
                .build();
        workersToStop.add(worker);

        worker.onFailed((job, err) -> failureLatch.countDown());
        worker.start();

        assertTrue(failureLatch.await(5, TimeUnit.SECONDS), "Worker should fail the first attempt");

        // Inspect Redis job hash while it is in the DELAYED set waiting for retry
        RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
        String jobKey = new QueueKeys(queueName).toJobKey(enqueued.getId());
        Map<String, String> fields = sync.hgetall(jobKey);

        assertNotNull(fields);
        assertTrue(fields.containsKey("failedReason"), "Job hash must contain 'failedReason' during retry delay");
        assertTrue(fields.get("failedReason").contains("Simulated upstream 503 outage"),
                "failedReason must contain the actual exception message");
        assertTrue(fields.containsKey("stacktrace"), "Job hash must contain 'stacktrace' during retry delay");
    }

    @Test
    @DisplayName("JobScheduler: worker automatically reschedules recurring jobs upon processing")
    void testWorkerAutomaticallyReschedulesJobScheduler() throws Exception {
        String queueName = "sched-repeat-" + UUID.randomUUID();
        Queue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        String schedulerId = "ticker-" + UUID.randomUUID();
        // Schedule every 2 seconds
        queue.upsertJobScheduler(schedulerId, Duration.ofSeconds(2), "tick-job", "tick-data", JobOptions.defaults());

        CountDownLatch firstRunLatch = new CountDownLatch(1);
        AtomicInteger runs = new AtomicInteger(0);

        Worker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .processor(job -> {
                    runs.incrementAndGet();
                    firstRunLatch.countDown();
                    return "done";
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        assertTrue(firstRunLatch.await(5, TimeUnit.SECONDS), "First scheduled job should execute");

        // Verify that the scheduler metadata hash still exists and has its next timestamp scheduled
        RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
        String schedKey = new QueueKeys(queueName).toKey("repeat:" + schedulerId);
        Map<String, String> schedData = sync.hgetall(schedKey);

        assertNotNull(schedData);
        assertEquals("tick-job", schedData.get("name"));
        assertEquals("2000", schedData.get("every"));

        // Verify next scheduled score in the repeat sorted set
        Double nextScore = sync.zscore(new QueueKeys(queueName).getKey("repeat"), schedulerId);
        assertNotNull(nextScore, "JobScheduler must have next timestamp in repeat sorted set");
        assertTrue(nextScore > 0, "Next scheduled timestamp must be positive");

        // Verify that delayed set has the next occurrence scheduled
        Long delayedCount = sync.zcard(new QueueKeys(queueName).getKey("delayed"));
        assertTrue(delayedCount != null && delayedCount >= 1, "Next occurrence must be present in delayed set");
    }

    @Test
    @DisplayName("FlowProducer: getFlow retrieves hierarchical JobNode tree with children")
    void testFlowProducerGetFlow() {
        String parentQueue = "flow-p-" + UUID.randomUUID();
        String childQueue1 = "flow-c1-" + UUID.randomUUID();
        String childQueue2 = "flow-c2-" + UUID.randomUUID();

        Queue<?> qParent = OxmqQueue.builder().name(parentQueue).connectionManager(connectionManager).build();
        Queue<?> qChild1 = OxmqQueue.builder().name(childQueue1).connectionManager(connectionManager).build();
        Queue<?> qChild2 = OxmqQueue.builder().name(childQueue2).connectionManager(connectionManager).build();
        queuesToClean.add(qParent);
        queuesToClean.add(qChild1);
        queuesToClean.add(qChild2);

        FlowProducer producer = new FlowProducer(connectionManager);

        FlowJob<String> c1 = FlowJob.of(childQueue1, "child1", "data1");
        FlowJob<String> c2 = FlowJob.of(childQueue2, "child2", "data2");
        FlowJob<String> root = FlowJob.of(parentQueue, "parent", "parentData")
                .addChild(c1)
                .addChild(c2);

        String parentJobId = producer.add(root);
        assertNotNull(parentJobId);

        // Retrieve flow tree via getFlow
        JobNode tree = producer.getFlow(parentQueue, parentJobId);
        assertNotNull(tree, "JobNode tree must be retrieved");
        assertNotNull(tree.getJob());
        assertEquals(parentJobId, tree.getJob().getId());
        assertEquals("parent", tree.getJob().getName());
        assertEquals(2, tree.getChildren().size(), "Parent must have 2 children in JobNode tree");
    }

    @Test
    @DisplayName("BUG-19: JobScheduler with limit terminates and does not repeat indefinitely")
    void testJobSchedulerStopsAfterReachingIterationLimit() throws Exception {
        String queueName = "sched-limit-" + UUID.randomUUID();
        Queue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        String schedulerId = "limited-ticker-" + UUID.randomUUID();
        // Repeat limit = 2 iterations, interval = 200 ms
        JobOptions opts = JobOptions.builder()
                .repeatLimit(2)
                .build();

        queue.upsertJobScheduler(schedulerId, Duration.ofMillis(200), "limited-task", "limit-data", opts);

        CountDownLatch secondRunLatch = new CountDownLatch(2);
        AtomicInteger runs = new AtomicInteger(0);

        Worker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .pollIntervalMs(50)
                .processor(job -> {
                    runs.incrementAndGet();
                    secondRunLatch.countDown();
                    return "done";
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        assertTrue(secondRunLatch.await(5, TimeUnit.SECONDS), "Both scheduled occurrences (limit=2) must execute");

        // Wait extra time to ensure a third execution is NOT scheduled
        TimeUnit.MILLISECONDS.sleep(800);
        assertEquals(2, runs.get(), "Worker must strictly execute exactly 2 times and stop at iteration limit");

        // Verify scheduler metadata in Redis: ic must be 2 and limit must be 2
        RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
        String schedKey = new QueueKeys(queueName).toKey("repeat:" + schedulerId);
        Map<String, String> schedData = sync.hgetall(schedKey);
        assertEquals("2", schedData.get("ic"), "Scheduler ic counter must be 2");
        assertEquals("2", schedData.get("limit"), "Scheduler limit must be 2");
    }

    @Test
    @DisplayName("BUG-19 / BUG-21: OxmqBatchWorker respects JobScheduler iteration limit and provides ignoredChildrenFailures")
    void testBatchWorkerJobSchedulerStopsAfterReachingIterationLimit() throws Exception {
        String queueName = "batch-sched-limit-" + UUID.randomUUID();
        Queue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        String schedulerId = "batch-limited-ticker-" + UUID.randomUUID();
        JobOptions opts = JobOptions.builder()
                .repeatLimit(2)
                .build();

        queue.upsertJobScheduler(schedulerId, Duration.ofMillis(200), "batch-limit-task", "batch-data", opts);

        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger runs = new AtomicInteger(0);

        OxmqBatchWorker<String> batchWorker = OxmqBatchWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .batchSize(5)
                .batchTimeout(Duration.ofMillis(50))
                .pollInterval(Duration.ofMillis(50))
                .processor(batch -> {
                    for (Job<String> j : batch) {
                        runs.incrementAndGet();
                        latch.countDown();
                        // Verify BUG-21: getIgnoredChildrenFailures is accessible and non-null
                        Map<String, String> failures = j.getIgnoredChildrenFailures();
                        assertNotNull(failures, "Ignored children failures map must be non-null in batch worker");
                    }
                    return List.of();
                })
                .build();
        workersToStop.add(batchWorker);
        batchWorker.start();

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Both batch occurrences must execute");

        TimeUnit.MILLISECONDS.sleep(800);
        assertEquals(2, runs.get(), "Batch worker must strictly execute exactly 2 times and terminate");
    }

    @Test
    @DisplayName("BUG-22: BullScripts.getState and getStateV2 safely decode byte[] without returning '[B@...'")
    void testBullScriptsGetStateByteArraySafety() {
        String queueName = "state-safety-" + UUID.randomUUID();
        Queue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        Job<String> job = queue.add("test-state", "payload");
        assertNotNull(job.getId());

        String state = queue.getState(job.getId());
        assertNotNull(state);
        assertFalse(state.startsWith("[B@"), "State must not be a raw byte array toString ('[B@...')");
        assertTrue(List.of("waiting", "delayed", "active", "completed", "failed", "unknown").contains(state),
                "State must be a canonical string name: " + state);
    }
}
