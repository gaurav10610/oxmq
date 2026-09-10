package io.oxmq;

import io.oxmq.client.RedisConnectionManager;
import io.oxmq.exception.UnrecoverableError;
import io.oxmq.model.BackoffStrategy;
import io.oxmq.model.FlowJob;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobRequest;
import io.oxmq.model.JobState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive Integration Test verifying complete feature parity between OxMQ and BullMQ v5
 * directly against live Redis.
 */
class BullMqFullFeatureParityIntegrationTest {

    private static final String REDIS_URI = "redis://localhost:6379";
    private RedisConnectionManager connectionManager;
    private List<Queue<?>> queuesToClean;
    private List<Worker> workersToStop;

    @BeforeEach
    void setUp() {
        connectionManager = new RedisConnectionManager(REDIS_URI);
        queuesToClean = new ArrayList<>();
        workersToStop = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        for (Worker worker : workersToStop) {
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

    private <T> OxmqQueue<T> createQueue(String baseName, Class<T> clazz) {
        String queueName = baseName + "-" + System.currentTimeMillis() + "-" + (int)(Math.random() * 1000);
        OxmqQueue<T> queue = OxmqQueue.<T>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .payloadClass(clazz)
                .build();
        queuesToClean.add(queue);
        return queue;
    }

    private <T> OxmqWorker<T> startWorker(OxmqQueue<T> queue, int concurrency, JobProcessor<T, Object> processor) {
        OxmqWorker<T> worker = OxmqWorker.<T>builder()
                .queueName(queue.getName())
                .connectionManager(connectionManager)
                .concurrency(concurrency)
                .pollIntervalMs(20)
                .processor(processor)
                .build();
        workersToStop.add(worker);
        worker.start();
        return worker;
    }

    private void awaitJobState(Queue<?> queue, String jobId, String expectedState, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (expectedState.equals(queue.getState(jobId))) {
                return;
            }
            Thread.sleep(20);
        }
    }

    @Test
    @DisplayName("Feature 1: Bulk Enqueue (addBulk) and FIFO Execution")
    void testAddBulkAndFifo() throws Exception {
        OxmqQueue<String> queue = createQueue("test-parity-bulk", String.class);

        List<JobRequest<String>> requests = List.of(
                JobRequest.of("task-1", "payload-1"),
                JobRequest.of("task-2", "payload-2"),
                JobRequest.of("task-3", "payload-3")
        );

        List<Job<String>> added = queue.addBulk(requests);
        assertEquals(3, added.size());
        assertEquals("task-1", added.get(0).getName());
        assertEquals("task-2", added.get(1).getName());
        assertEquals("task-3", added.get(2).getName());

        List<String> processedOrder = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(3);

        startWorker(queue, 1, job -> {
            processedOrder.add(job.getName());
            latch.countDown();
            return "done";
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(List.of("task-1", "task-2", "task-3"), processedOrder);
    }

    @Test
    @DisplayName("Feature 2: Delayed Jobs, changeDelay, and promote()")
    void testDelayedAndPromote() throws Exception {
        OxmqQueue<String> queue = createQueue("test-parity-delayed", String.class);

        // Add delayed job with 10 seconds delay
        Job<String> job = queue.add("delayed-task", "data", JobOptions.builder().delay(Duration.ofSeconds(10)).build());
        assertNotNull(job.getId());

        // Check state is delayed
        String state = queue.getState(job.getId());
        assertEquals("delayed", state);

        // Change delay to 20 seconds
        queue.changeDelay(job.getId(), Duration.ofSeconds(20));
        assertEquals("delayed", queue.getState(job.getId()));

        // Promote to waiting immediately
        queue.promote(job.getId());
        // Verify state is no longer delayed
        String promotedState = queue.getState(job.getId());
        assertTrue("waiting".equals(promotedState) || "wait".equals(promotedState) || "active".equals(promotedState));

        CountDownLatch latch = new CountDownLatch(1);
        startWorker(queue, 1, j -> {
            latch.countDown();
            return "promoted-done";
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        awaitJobState(queue, job.getId(), "completed", 3000);
        assertEquals("completed", queue.getState(job.getId()));
    }

    @Test
    @DisplayName("Feature 3: Priority Ordering and changePriority()")
    void testPriorityAndChangePriority() throws Exception {
        OxmqQueue<String> queue = createQueue("test-parity-prio", String.class);

        // Add job with low priority (high number = lower priority)
        Job<String> lowPrioJob = queue.add("low", "data-low", JobOptions.builder().priority(10).build());
        // Add job with high priority (low number = higher priority)
        Job<String> highPrioJob = queue.add("high", "data-high", JobOptions.builder().priority(2).build());

        // Change lowPrioJob to priority 1 (now highest priority)
        queue.changePriority(lowPrioJob.getId(), 1);

        List<String> order = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(2);

        startWorker(queue, 1, job -> {
            order.add(job.getName());
            latch.countDown();
            return "ok";
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals("low", order.get(0), "Job changed to priority 1 should be processed first");
        assertEquals("high", order.get(1));
    }

    @Test
    @DisplayName("Feature 4: Deduplication by Custom ID and removeDeduplicationKey()")
    void testDeduplication() {
        OxmqQueue<String> queue = createQueue("test-parity-dedup", String.class);

        String dedupId = "unique-order-8888";
        JobOptions opts = JobOptions.builder()
                .deduplicationId(dedupId)
                .deduplicationTtl(Duration.ofMinutes(5))
                .build();

        Job<String> job1 = queue.add("order-job", "first-attempt", opts);
        Job<String> job2 = queue.add("order-job", "second-attempt", opts);

        // Deduplication should return the original job ID
        assertEquals(job1.getId(), job2.getId(), "Deduplication must prevent duplicate job creation");

        // Clear deduplication key
        boolean removed = queue.removeDeduplicationKey(dedupId);
        assertTrue(removed, "removeDeduplicationKey must return true");

        // Now a new job with the same dedup ID can be added
        Job<String> job3 = queue.add("order-job", "third-attempt", opts);
        assertNotEquals(job1.getId(), job3.getId(), "After removing dedup key, new job must have different ID");
    }

    @Test
    @DisplayName("Feature 5: Retries with Backoff Strategy")
    void testRetriesWithBackoff() throws Exception {
        OxmqQueue<String> queue = createQueue("test-parity-retry", String.class);

        JobOptions opts = JobOptions.builder()
                .attempts(3)
                .backoff(BackoffStrategy.fixed(Duration.ofMillis(50)))
                .build();

        Job<String> job = queue.add("retry-task", "payload", opts);

        AtomicInteger attemptCounter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        startWorker(queue, 1, j -> {
            int attempt = attemptCounter.incrementAndGet();
            if (attempt < 3) {
                throw new RuntimeException("Simulated transient failure on attempt " + attempt);
            }
            latch.countDown();
            return "success-on-3";
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(3, attemptCounter.get());

        Job<String> finished = queue.getJob(job.getId());
        assertNotNull(finished);
        assertEquals("completed", queue.getState(job.getId()));
    }

    @Test
    @DisplayName("Feature 6: UnrecoverableError Immediately Bypasses Retries")
    void testUnrecoverableError() throws Exception {
        OxmqQueue<String> queue = createQueue("test-parity-unrecov", String.class);

        JobOptions opts = JobOptions.builder()
                .attempts(10) // 10 attempts configured
                .build();

        Job<String> job = queue.add("unrecov-task", "bad-payload", opts);

        AtomicInteger attempts = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        startWorker(queue, 1, j -> {
            attempts.incrementAndGet();
            latch.countDown();
            throw new UnrecoverableError("Invalid payload schema: cannot recover");
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        // Give worker a moment to finalize
        Thread.sleep(200);

        assertEquals(1, attempts.get(), "UnrecoverableError must not trigger retries");
        assertEquals("failed", queue.getState(job.getId()));

        Job<String> failedJob = queue.getJob(job.getId());
        assertNotNull(failedJob);
        assertNotNull(failedJob.getFailedReason());
        assertTrue(failedJob.getFailedReason().contains("UnrecoverableError"));
    }

    @Test
    @DisplayName("Feature 7: Real-time Job Progress and Step Logging (getJobLogs)")
    void testProgressAndLogs() throws Exception {
        OxmqQueue<String> queue = createQueue("test-parity-logs", String.class);

        Job<String> job = queue.add("logged-job", "data");
        CountDownLatch latch = new CountDownLatch(1);

        startWorker(queue, 1, j -> {
            j.updateProgress(25);
            j.log("Stage 1: Validation passed");
            j.updateProgress(75);
            j.log("Stage 2: Processing completed");
            j.updateProgress(100);
            latch.countDown();
            return "done";
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));

        List<String> logs = queue.getJobLogs(job.getId());
        assertEquals(2, logs.size());
        assertEquals("Stage 1: Validation passed", logs.get(0));
        assertEquals("Stage 2: Processing completed", logs.get(1));
    }

    @Test
    @DisplayName("Feature 8: Queue State Inspection & Metrics Breakdown (getJobCounts)")
    void testJobCountsAndQueueManagement() {
        OxmqQueue<String> queue = createQueue("test-parity-mgmt", String.class);

        // Add 2 waiting, 1 delayed
        queue.add("w1", "d1");
        queue.add("w2", "d2");
        queue.add("d1", "delayed", JobOptions.builder().delay(Duration.ofMinutes(1)).build());

        Map<String, Long> counts = queue.getJobCounts();
        assertNotNull(counts);
        assertEquals(2L, counts.get("wait"));
        assertEquals(1L, counts.get("delayed"));
        assertEquals(0L, counts.get("active"));
        assertEquals(0L, counts.get("completed"));
        assertEquals(0L, counts.get("failed"));

        // Drain queue
        queue.drain(true);
        Map<String, Long> countsAfterDrain = queue.getJobCounts();
        assertEquals(0L, countsAfterDrain.get("wait"));
        assertEquals(0L, countsAfterDrain.get("delayed"));
    }

    @Test
    @DisplayName("Feature 9: Pause and Resume Queue Consumption")
    void testPauseAndResume() throws Exception {
        OxmqQueue<String> queue = createQueue("test-parity-pause", String.class);

        queue.pause();
        assertTrue(queue.isPaused());

        Job<String> job = queue.add("paused-task", "data");

        CountDownLatch latch = new CountDownLatch(1);
        startWorker(queue, 1, j -> {
            latch.countDown();
            return "resumed";
        });

        // Worker shouldn't process while paused
        assertFalse(latch.await(300, TimeUnit.MILLISECONDS));

        // Resume queue
        queue.resume();
        assertFalse(queue.isPaused());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        awaitJobState(queue, job.getId(), "completed", 3000);
        assertEquals("completed", queue.getState(job.getId()));
    }

    @Test
    @DisplayName("Feature 10: Reprocess/Retry Failed Job via queue.retry()")
    void testQueueRetry() throws Exception {
        OxmqQueue<String> queue = createQueue("test-parity-qretry", String.class);

        Job<String> job = queue.add("flaky-task", "payload");

        AtomicInteger runCount = new AtomicInteger(0);
        CountDownLatch failLatch = new CountDownLatch(1);
        CountDownLatch successLatch = new CountDownLatch(1);

        startWorker(queue, 1, j -> {
            int r = runCount.incrementAndGet();
            if (r == 1) {
                failLatch.countDown();
                throw new RuntimeException("Initial failure");
            }
            successLatch.countDown();
            return "recovered";
        });

        assertTrue(failLatch.await(5, TimeUnit.SECONDS));
        // Give worker time to mark failed
        Thread.sleep(200);
        assertEquals("failed", queue.getState(job.getId()));

        // Retry via Queue API
        queue.retry(job.getId());

        assertTrue(successLatch.await(5, TimeUnit.SECONDS));
        awaitJobState(queue, job.getId(), "completed", 3000);
        assertEquals("completed", queue.getState(job.getId()));
    }

    @Test
    @DisplayName("Feature 11: Job Scheduler Upsert and Remove")
    void testJobScheduler() {
        OxmqQueue<String> queue = createQueue("test-parity-sched", String.class);

        String schedulerId = "daily-report-scheduler";
        String schedJobId = queue.upsertJobScheduler(schedulerId, Duration.ofHours(24), "daily-report", "data", JobOptions.defaults());
        assertNotNull(schedJobId);

        // Remove the job scheduler
        boolean removed = queue.removeJobScheduler(schedulerId);
        assertTrue(removed);
    }

    @Test
    @DisplayName("Feature 12: Job Data Mutation (updateData)")
    void testUpdateData() {
        OxmqQueue<String> queue = createQueue("test-parity-update", String.class);

        Job<String> job = queue.add("mutate-job", "initial-data");
        assertEquals("initial-data", queue.getJob(job.getId()).getData());

        queue.updateData(job.getId(), "updated-data");
        assertEquals("updated-data", queue.getJob(job.getId()).getData());
    }

    @Test
    @DisplayName("Feature 13: Parent-Child DAG Workflow with Result Aggregation")
    void testFlowProducerDAG() throws Exception {
        OxmqQueue<String> childQueue = createQueue("dag-child", String.class);
        OxmqQueue<String> parentQueue = createQueue("dag-parent", String.class);

        FlowJob<String> child1 = FlowJob.of(childQueue.getName(), "child1", "val-10");
        FlowJob<String> child2 = FlowJob.of(childQueue.getName(), "child2", "val-20");

        FlowJob<String> root = FlowJob.of(parentQueue.getName(), "parent", "base-100");
        root.addChild(child1);
        root.addChild(child2);

        FlowProducer flowProducer = new FlowProducer(connectionManager);
        String parentJobId = flowProducer.add(root);
        assertNotNull(parentJobId);

        CountDownLatch childrenLatch = new CountDownLatch(2);
        CountDownLatch parentLatch = new CountDownLatch(1);
        Map<String, Object> receivedChildVals = new ConcurrentHashMap<>();

        startWorker(childQueue, 5, j -> {
            childrenLatch.countDown();
            return Map.of("calculated", j.getName().equals("child1") ? 100 : 200);
        });

        startWorker(parentQueue, 1, j -> {
            if (j.getChildrenValues() != null) {
                receivedChildVals.putAll(j.getChildrenValues());
            }
            parentLatch.countDown();
            return "parent-done";
        });

        assertTrue(childrenLatch.await(5, TimeUnit.SECONDS));
        assertTrue(parentLatch.await(5, TimeUnit.SECONDS));
        assertEquals(2, receivedChildVals.size(), "Parent must receive child results");
    }
}
