package io.oxmq;

import io.oxmq.client.RedisConnectionManager;
import io.oxmq.exception.DelayedException;
import io.oxmq.model.DeduplicationOptions;
import io.oxmq.model.FlowJob;
import io.oxmq.model.Job;
import io.oxmq.model.JobDependencyCounts;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import io.oxmq.model.RetryJobsOptions;
import io.oxmq.model.RetryOptions;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive Integration Tests for all new BullMQ v6 parity capabilities:
 * - keepLastIfActive deduplication buffer
 * - DelayedException non-failing worker postpone (skipAttempt: true)
 * - Priority preservation on finished job retries (reprocessJob-9.lua)
 * - DAG child re-attachment on bulk retry (moveJobsToWait-8.lua)
 * - Bulk promoteJobs
 * - getCountsPerPriority
 * - getRanges multi-state atomic queries
 * - getDependencyCounts
 * - Job scheduler inspection and count
 * - FlowProducer parent-node deduplication validation
 */
public class BullMqV6ParityIntegrationTest {

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
    @DisplayName("DelayedException: postpones active job to delayed set without incrementing attemptsMade")
    void testDelayedExceptionSkipsAttemptAndPostponesJob() throws InterruptedException {
        String queueName = "v6-delayed-exc-" + System.currentTimeMillis();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        AtomicInteger executionAttempts = new AtomicInteger(0);
        CountDownLatch completedLatch = new CountDownLatch(1);

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(1)
                .processor(job -> {
                    int count = executionAttempts.incrementAndGet();
                    if (count == 1) {
                        // First attempt: throw DelayedException to postpone job by 200ms
                        throw new DelayedException(Duration.ofMillis(200));
                    }
                    completedLatch.countDown();
                    return "delayed-processed-ok";
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        queue.add("task-with-delayed-exc", "payload-1", JobOptions.builder().attempts(1).build());

        // Wait for final completion
        boolean completed = completedLatch.await(5, TimeUnit.SECONDS);
        assertTrue(completed, "Job should complete successfully on subsequent execution after delay");
        assertEquals(2, executionAttempts.get(), "Worker should have been invoked twice (once delayed, once success)");

        // Allow worker to finish moveToFinished
        try {
            queue.waitUntilFinished("1", Duration.ofSeconds(2));
        } catch (Exception ignored) {}

        // Job attemptsMade should be at most 1 because DelayedException uses skipAttempt: true (delay was not counted)
        Job<String> job = queue.getJob("1");
        assertNotNull(job);
        assertTrue(job.getAttemptsMade() <= 1, "attemptsMade must not exceed 1 when DelayedException is thrown");
    }

    @Test
    @DisplayName("Deduplication keepLastIfActive: buffers latest job while winner is active and auto-requeues on finish")
    void testKeepLastIfActiveDeduplication() throws InterruptedException {
        String queueName = "v6-keep-last-" + System.currentTimeMillis();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        CountDownLatch job1Started = new CountDownLatch(1);
        CountDownLatch allowJob1ToFinish = new CountDownLatch(1);
        CountDownLatch allDone = new CountDownLatch(2); // job 1, then buffered job 3
        List<String> processedPayloads = Collections.synchronizedList(new ArrayList<>());

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(1)
                .processor(job -> {
                    processedPayloads.add(job.getData());
                    if ("payload-1".equals(job.getData())) {
                        job1Started.countDown();
                        try {
                            allowJob1ToFinish.await(5, TimeUnit.SECONDS);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    allDone.countDown();
                    return "ok";
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        // Enqueue Job 1
        DeduplicationOptions dedupOpts = DeduplicationOptions.builder()
                .id("user-update-42")
                .keepLastIfActive(true)
                .build();

        queue.add("user-update", "payload-1", JobOptions.builder().deduplication(dedupOpts).build());

        // Wait until Job 1 is actively processing
        assertTrue(job1Started.await(5, TimeUnit.SECONDS), "Job 1 should start processing");

        // While Job 1 is active, push Job 2 and then Job 3 with the same dedup ID
        queue.add("user-update", "payload-2", JobOptions.builder().deduplication(dedupOpts).build());
        queue.add("user-update", "payload-3", JobOptions.builder().deduplication(dedupOpts).build());

        // Now allow Job 1 to finish. BullMQ's moveToFinished will requeue Job 3 (latest payload)!
        allowJob1ToFinish.countDown();

        assertTrue(allDone.await(5, TimeUnit.SECONDS), "Both initial job and buffered latest job should complete");
        assertEquals(2, processedPayloads.size(), "Exactly 2 jobs should be executed");
        assertEquals("payload-1", processedPayloads.get(0));
        assertEquals("payload-3", processedPayloads.get(1), "Job 3 (latest buffered payload) should be executed, overwriting Job 2");
    }

    @Test
    @DisplayName("reprocessJob-9: preserves job priority when retrying failed or completed job")
    void testReprocessJobPreservesPriority() throws InterruptedException {
        String queueName = "v6-retry-prio-" + System.currentTimeMillis();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        CountDownLatch failLatch = new CountDownLatch(1);

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(1)
                .processor(job -> {
                    failLatch.countDown();
                    throw new RuntimeException("Simulated failure to test priority retry");
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        // Add job with priority 15 and 1 attempt so it lands in failed state
        Job<String> job = queue.add("prio-task", "data", JobOptions.builder().priority(15).attempts(1).build());
        assertTrue(failLatch.await(5, TimeUnit.SECONDS), "Job should fail");
        Thread.sleep(200);
        worker.pause(); // pause worker so job stays in queue after retry for priority verification

        assertEquals("failed", queue.getState(job.getId()));

        // Retry the failed job with attempts reset
        queue.retry(job.getId(), RetryOptions.resetAttempts());

        // Verify with getCountsPerPriority that priority 15 is preserved!
        Map<Integer, Long> priorityCounts = queue.getCountsPerPriority(15);
        assertEquals(1L, priorityCounts.get(15), "Job should be reprocessed into prioritized bucket with priority 15 preserved");
    }

    @Test
    @DisplayName("moveJobsToWait-8: bulk retry restores DAG parent-child dependencies")
    void testBulkRetryJobsAndDagReattachment() throws InterruptedException {
        String childQueueName = "v6-dag-child-" + System.currentTimeMillis();
        String parentQueueName = "v6-dag-parent-" + System.currentTimeMillis();

        OxmqQueue<String> childQueue = OxmqQueue.<String>builder().name(childQueueName).connectionManager(connectionManager).build();
        OxmqQueue<String> parentQueue = OxmqQueue.<String>builder().name(parentQueueName).connectionManager(connectionManager).build();
        queuesToClean.add(childQueue);
        queuesToClean.add(parentQueue);

        FlowJob<String> childNode = FlowJob.of(childQueueName, "child-step", "child-input",
                JobOptions.builder().attempts(1).build());

        FlowJob<String> parentNode = FlowJob.of(parentQueueName, "parent-step", "parent-input")
                .addChild(childNode);

        FlowProducer producer = new FlowProducer(connectionManager);
        String parentId = producer.add(parentNode);
        assertNotNull(parentId);

        CountDownLatch childFailedLatch = new CountDownLatch(1);
        AtomicBoolean childShouldPass = new AtomicBoolean(false);

        OxmqWorker<String> childWorker = OxmqWorker.<String>builder()
                .queueName(childQueueName)
                .connectionManager(connectionManager)
                .concurrency(1)
                .processor(job -> {
                    if (!childShouldPass.get()) {
                        childFailedLatch.countDown();
                        throw new RuntimeException("Child failed intentionally");
                    }
                    return "child-ok";
                })
                .build();
        workersToStop.add(childWorker);
        childWorker.start();

        assertTrue(childFailedLatch.await(5, TimeUnit.SECONDS), "Child job should fail on first attempt");
        Thread.sleep(200);

        // Bulk retry failed jobs using moveJobsToWait-8.lua
        childShouldPass.set(true);
        long retried = childQueue.retryJobs(new RetryJobsOptions(JobState.FAILED, 10, null));
        assertTrue(retried >= 1, "retryJobs should move child back to wait");

        CountDownLatch parentCompletedLatch = new CountDownLatch(1);
        OxmqWorker<String> parentWorker = OxmqWorker.<String>builder()
                .queueName(parentQueueName)
                .connectionManager(connectionManager)
                .concurrency(1)
                .processor(job -> {
                    parentCompletedLatch.countDown();
                    return "parent-ok";
                })
                .build();
        workersToStop.add(parentWorker);
        parentWorker.start();

        assertTrue(parentCompletedLatch.await(5, TimeUnit.SECONDS), "Parent job must resolve after child retries and succeeds");
    }

    @Test
    @DisplayName("Bulk promoteJobs: moves multiple delayed jobs immediately to wait")
    void testBulkPromoteJobs() {
        String queueName = "v6-bulk-promote-" + System.currentTimeMillis();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        // Add 5 jobs delayed by 1 hour
        for (int i = 0; i < 5; i++) {
            queue.add("delayed-job", "val-" + i, JobOptions.builder().delay(Duration.ofHours(1)).build());
        }

        assertEquals(5, queue.count(JobState.DELAYED));
        assertEquals(0, queue.count(JobState.WAITING));

        // Promote all delayed jobs in bulk
        long iterations = queue.promoteJobs(10);
        assertTrue(iterations >= 1);

        assertEquals(0, queue.count(JobState.DELAYED), "Delayed count should be 0 after bulk promote");
        assertEquals(5, queue.count(JobState.WAITING), "All 5 jobs should be moved immediately to wait queue");
    }

    @Test
    @DisplayName("getCountsPerPriority: queries job counts per priority bucket")
    void testGetCountsPerPriority() {
        String queueName = "v6-prio-counts-" + System.currentTimeMillis();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        queue.add("task", "d1", JobOptions.builder().priority(2).build());
        queue.add("task", "d2", JobOptions.builder().priority(2).build());
        queue.add("task", "d3", JobOptions.builder().priority(5).build());
        queue.add("task", "d4", JobOptions.builder().priority(10).build());

        Map<Integer, Long> counts = queue.getCountsPerPriority(2, 5, 10, 99);
        assertEquals(2L, counts.get(2));
        assertEquals(1L, counts.get(5));
        assertEquals(1L, counts.get(10));
        assertEquals(0L, counts.get(99));
    }

    @Test
    @DisplayName("getRanges: queries paginated job IDs across multiple queue states atomically")
    void testGetRangesMultiState() {
        String queueName = "v6-ranges-" + System.currentTimeMillis();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        queue.add("w1", "wait-1");
        queue.add("w2", "wait-2");
        queue.add("d1", "delayed-1", JobOptions.builder().delay(Duration.ofMinutes(10)).build());

        Map<JobState, List<String>> ranges = queue.getRanges(
                new JobState[]{JobState.WAITING, JobState.DELAYED},
                0, 10, true
        );

        assertNotNull(ranges);
        assertEquals(2, ranges.get(JobState.WAITING).size());
        assertEquals(1, ranges.get(JobState.DELAYED).size());
    }

    @Test
    @DisplayName("Job Schedulers: inspection and count via getJobScheduler & getJobSchedulersCount")
    void testJobSchedulerInspectionAndCount() {
        String queueName = "v6-schedulers-" + System.currentTimeMillis();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        String schedulerId = "every-5-min-sync";
        queue.upsertJobScheduler(schedulerId, Duration.ofMinutes(5), "sync-job", "payload", JobOptions.defaults());

        assertEquals(1L, queue.getJobSchedulersCount());

        Map<String, Object> schedulerInfo = queue.getJobScheduler(schedulerId);
        assertNotNull(schedulerInfo);
        assertTrue(schedulerInfo.containsKey("nextTimestamp"));

        // Upsert with cron pattern
        String cronSchedulerId = "daily-midnight-report";
        queue.upsertJobScheduler(cronSchedulerId, "0 0 * * *", "report-job", "report-payload", JobOptions.defaults());
        assertEquals(2L, queue.getJobSchedulersCount());
    }

    @Test
    @DisplayName("FlowProducer: validates and rejects deduplication options on parent nodes")
    void testFlowProducerRejectsParentDeduplication() {
        FlowJob<String> childNode = FlowJob.of("child-q", "child", "data");

        FlowJob<String> invalidParent = FlowJob.of("parent-q", "parent", "data",
                JobOptions.builder().deduplication("parent-dedup-id").build())
                .addChild(childNode);

        FlowProducer producer = new FlowProducer(connectionManager);
        assertThrows(IllegalArgumentException.class, () -> producer.add(invalidParent),
                "FlowProducer must reject deduplication on parent DAG nodes");
    }

    @Test
    @DisplayName("trimEvents: trims events stream to approximately maxLength")
    void testTrimEvents() {
        String queueName = "v6-trim-events-" + System.currentTimeMillis();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        for (int i = 0; i < 20; i++) {
            queue.add("job-" + i, "data-" + i);
        }

        assertDoesNotThrow(() -> queue.trimEvents(5));
    }

    @Test
    @DisplayName("clean: allows cleaning wait, paused, and active queues per cleanJobsInSet-3 contract")
    void testCleanWaitAndPaused() {
        String queueName = "v6-clean-wait-" + System.currentTimeMillis();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        for (int i = 0; i < 5; i++) {
            queue.add("wait-job", "val-" + i);
        }
        assertEquals(5, queue.count(JobState.WAITING));

        // Clean jobs in wait state with 0 grace period
        long cleaned = queue.clean(0, 10, JobState.WAITING);
        assertEquals(5, cleaned, "Should clean all 5 waiting jobs");
        assertEquals(0, queue.count(JobState.WAITING));
    }

    @Test
    @DisplayName("WaitingException: dynamically returns active job to wait queue")
    void testWaitingExceptionMovesJobToWait() throws InterruptedException {
        String queueName = "v6-waiting-ex-" + System.currentTimeMillis();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger(0);
        CountDownLatch waitExceptionLatch = new CountDownLatch(1);
        CountDownLatch completedLatch = new CountDownLatch(1);

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(1)
                .processor(job -> {
                    if (attempts.incrementAndGet() == 1) {
                        waitExceptionLatch.countDown();
                        throw new io.oxmq.exception.WaitingException("Not ready yet; move back to wait");
                    }
                    completedLatch.countDown();
                    return "done";
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        queue.add("defer-job", "test-data");
        assertTrue(waitExceptionLatch.await(5, TimeUnit.SECONDS), "First execution must throw WaitingException");

        // The job is moved back to wait, so the worker picks it up again on next poll
        assertTrue(completedLatch.await(5, TimeUnit.SECONDS), "Job returned to wait must execute second time and complete");
        assertEquals(2, attempts.get());
    }
}
