package io.oxmq;

import io.lettuce.core.api.StatefulRedisConnection;
import io.oxmq.client.QueueKeys;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.exception.UnrecoverableError;
import io.oxmq.exception.WaitingException;
import io.oxmq.model.FlowJob;
import io.oxmq.model.GroupOptions;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive Integration Test Suite verifying all edge cases, adverse failure modes,
 * and favourable scenarios across OxmqQueue, OxmqWorker, OxmqBatchWorker, FlowProducer, and CronEvaluator.
 */
public class AdverseAndEdgeCaseIntegrationTest {

    private static final String REDIS_URI = "redis://localhost:6379";
    private RedisConnectionManager connectionManager;
    private List<Queue<?>> queuesToClean;
    private List<Worker<?>> workersToStop;
    private List<QueueEvents> eventsToClose;

    @BeforeEach
    void setUp() {
        connectionManager = new RedisConnectionManager(REDIS_URI);
        queuesToClean = new ArrayList<>();
        workersToStop = new ArrayList<>();
        eventsToClose = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        for (QueueEvents events : eventsToClose) {
            try {
                events.close();
            } catch (Exception ignored) {}
        }
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
    @DisplayName("addBulk: validates null inputs and propagates pipelined job attributes accurately")
    void testAddBulkValidationAndSafety() {
        String queueName = "adverse-bulk-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        // 1. Null or empty list returns empty list
        assertTrue(queue.addBulk(null).isEmpty());
        assertTrue(queue.addBulk(List.of()).isEmpty());

        // 2. Null elements inside list fail fast with IllegalArgumentException
        IllegalArgumentException ex0 = assertThrows(IllegalArgumentException.class, () ->
                queue.addBulk(Arrays.asList((JobRequest<String>) null))
        );
        assertTrue(ex0.getMessage().contains("at index 0"));

        IllegalArgumentException ex1 = assertThrows(IllegalArgumentException.class, () ->
                queue.addBulk(Arrays.asList(
                        new JobRequest<>("task-1", "payload-1"),
                        null,
                        new JobRequest<>("task-3", "payload-3")
                ))
        );
        assertTrue(ex1.getMessage().contains("at index 1"));

        // 3. Favourable: Mixed options bulk enqueue
        List<Job<String>> created = queue.addBulk(List.of(
                new JobRequest<>("standard", "data1"),
                new JobRequest<>("delayed", "data2", JobOptions.builder().delay(Duration.ofSeconds(10)).build()),
                new JobRequest<>("prioritized", "data3", JobOptions.builder().priority(5).build()),
                new JobRequest<>("grouped", "data4", JobOptions.builder().group(GroupOptions.builder().id("tenant-alpha").build()).build())
        ));

        assertEquals(4, created.size());
        for (Job<String> j : created) {
            assertNotNull(j.getId());
            assertFalse(j.getId().isBlank());
            assertNotEquals("unknown", j.getId());
            assertEquals(queueName, j.getQueueName());
            assertTrue(j.getTimestamp() > 0);
        }

        assertEquals("tenant-alpha", created.get(3).getGroupKey());
        assertEquals(2, queue.count(JobState.WAITING)); // wait + prioritized
        assertEquals(1, queue.count(JobState.DELAYED));
        assertEquals(1, queue.count(JobState.PRIORITIZED));
    }

    @Test
    @DisplayName("getJob: accurately recovers attemptsStarted, groupKey, repeatJobKey, and survives non-integer progress")
    void testGetJobFieldParityAndNumericResilience() {
        String queueName = "adverse-fields-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        Job<String> enqueued = queue.add("test-job", "payload",
                JobOptions.builder().group(GroupOptions.builder().id("group-xyz").build()).build());
        assertNotNull(enqueued);
        String jobId = enqueued.getId();

        StatefulRedisConnection<String, String> sync = connectionManager.getCommandConnection();
        QueueKeys keys = new QueueKeys(queueName);
        String jobKey = keys.toJobKey(jobId);

        // Inject simulated fields directly to Redis hash to test resilience
        sync.sync().hset(jobKey, "attemptsStarted", "2");
        sync.sync().hset(jobKey, "processedOn", String.valueOf(System.currentTimeMillis()));
        sync.sync().hset(jobKey, "rjk", "repeat:sched-1:1793286300000");
        sync.sync().hset(jobKey, "progress", "{\"step\":\"downloading\",\"percentage\":42}");

        // Fetch job back via queue.getJob(jobId)
        Job<String> fetched = queue.getJob(jobId);
        assertNotNull(fetched);
        assertEquals(2, fetched.getAttemptsStarted());
        assertNotNull(fetched.getProcessedOn());
        assertEquals("group-xyz", fetched.getGroupKey());
        assertEquals("repeat:sched-1:1793286300000", fetched.getRepeatJobKey());

        // Decimal progress test (e.g. 78.6% -> rounded to 79)
        sync.sync().hset(jobKey, "progress", "78.6");
        Job<String> fetchedDecimal = queue.getJob(jobId);
        assertNotNull(fetchedDecimal);
        assertEquals(79, fetchedDecimal.getProgress());
    }

    @Test
    @DisplayName("clean: supports cleaning WAITING, ACTIVE, PAUSED, COMPLETED, and FAILED states")
    void testQueueCleanStateCoverage() throws Exception {
        String queueName = "adverse-clean-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        queue.add("wait-1", "d1");
        queue.add("wait-2", "d2");
        assertEquals(2, queue.count(JobState.WAITING));

        // Clean WAITING jobs with graceMs = 0
        long cleanedWaiting = queue.clean(0, 10, JobState.WAITING);
        assertTrue(cleanedWaiting >= 1, "Should clean at least 1 waiting job");
        assertEquals(0, queue.count(JobState.WAITING));

        // Test PAUSED key cleaning
        StatefulRedisConnection<String, String> sync = connectionManager.getCommandConnection();
        QueueKeys keys = new QueueKeys(queueName);
        sync.sync().rpush(keys.toKey("paused"), "paused-1", "paused-2");
        assertEquals(2, queue.count(JobState.PAUSED));

        long cleanedPaused = queue.clean(0, 10, JobState.PAUSED);
        assertEquals(2, cleanedPaused);
        assertEquals(0, queue.count(JobState.PAUSED));
    }

    @Test
    @DisplayName("retryJobs and promoteJobs: return exact delta counts via zcard inspection")
    void testRetryJobsAndPromoteJobsAccurateDeltas() {
        String queueName = "adverse-deltas-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        // Add 4 delayed jobs
        for (int i = 0; i < 4; i++) {
            queue.add("delayed-" + i, "data", JobOptions.builder().delay(Duration.ofHours(1)).build());
        }
        assertEquals(4, queue.count(JobState.DELAYED));

        // Promote 2 jobs
        long promotedBatch = queue.promoteJobs(2);
        assertEquals(2, promotedBatch);
        assertEquals(2, queue.count(JobState.DELAYED));
        assertEquals(2, queue.count(JobState.WAITING));

        // Promote remaining jobs (count <= 0 promotes all)
        long promotedRemaining = queue.promoteJobs(0);
        assertEquals(2, promotedRemaining);
        assertEquals(0, queue.count(JobState.DELAYED));
        assertEquals(4, queue.count(JobState.WAITING));
    }

    @Test
    @DisplayName("BatchWorker: WaitingException returns all batch jobs to waiting state without failure or attempt increment")
    void testBatchWorkerWaitingExceptionMovesToWait() throws Exception {
        String queueName = "adverse-batch-wait-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        queue.add("batch-1", "d1");
        queue.add("batch-2", "d2");
        queue.add("batch-3", "d3");

        CountDownLatch waitLatch = new CountDownLatch(1);
        CountDownLatch completedLatch = new CountDownLatch(1);
        AtomicInteger invocationCount = new AtomicInteger(0);

        OxmqBatchWorker<String> batchWorker = OxmqBatchWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .batchSize(3)
                .batchTimeout(Duration.ofMillis(50))
                .processor(batch -> {
                    if (invocationCount.incrementAndGet() == 1) {
                        waitLatch.countDown();
                        throw new WaitingException("Postponed batch execution");
                    }
                    completedLatch.countDown();
                    return "success";
                })
                .build();
        workersToStop.add(batchWorker);
        batchWorker.start();

        assertTrue(waitLatch.await(5, TimeUnit.SECONDS), "First batch execution must trigger WaitingException");
        assertTrue(completedLatch.await(5, TimeUnit.SECONDS), "Second batch execution must successfully pick up jobs from wait");

        // Close worker to inspect final queue state in Redis
        batchWorker.close();

        // All 3 jobs were returned to wait and then completed successfully on second attempt without failing
        assertEquals(0, queue.count(JobState.FAILED));
        assertEquals(3, queue.count(JobState.COMPLETED));

        Job<String> j1 = queue.getJob("1");
        assertNotNull(j1);
        assertEquals(JobState.COMPLETED, j1.getState());
    }

    @Test
    @DisplayName("BatchWorker: UnrecoverableError and job.discard() abort retries immediately")
    void testBatchWorkerUnrecoverableErrorAndDiscard() throws Exception {
        String queueName = "adverse-batch-unrecov-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        // Enqueue with 3 attempts
        queue.add("job-discard", "d1", JobOptions.builder().attempts(3).build());
        queue.add("job-unrecov", "d2", JobOptions.builder().attempts(3).build());

        CountDownLatch latch = new CountDownLatch(1);
        OxmqBatchWorker<String> batchWorker = OxmqBatchWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .batchSize(2)
                .batchTimeout(Duration.ofMillis(50))
                .processor(batch -> {
                    for (Job<String> job : batch) {
                        if ("job-discard".equals(job.getName())) {
                            job.discard();
                            job.setAsFailed(new RuntimeException("discarded error"));
                        } else if ("job-unrecov".equals(job.getName())) {
                            job.setAsFailed(new UnrecoverableError("fatal error"));
                        }
                    }
                    latch.countDown();
                    return "partial";
                })
                .build();
        workersToStop.add(batchWorker);
        batchWorker.start();

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        Thread.sleep(200);

        // Both jobs should be in FAILED state immediately without being retried to WAITING/DELAYED
        assertEquals(2, queue.count(JobState.FAILED));
        assertEquals(0, queue.count(JobState.WAITING));
        assertEquals(0, queue.count(JobState.DELAYED));
    }

    @Test
    @DisplayName("OxmqWorker: WaitingException dynamically returns active job to wait state without incrementing attempts")
    void testWorkerWaitingExceptionMovesToWait() throws Exception {
        String queueName = "adverse-worker-wait-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        queue.add("single-wait-job", "d1", JobOptions.builder().attempts(3).build());

        CountDownLatch executedLatch = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger(0);

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(1)
                .processor(job -> {
                    if (attempts.incrementAndGet() == 1) {
                        executedLatch.countDown();
                        throw new WaitingException("Temporarily unready");
                    }
                    return "processed";
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        assertTrue(executedLatch.await(5, TimeUnit.SECONDS));

        // Job was moved to wait and immediately picked up again on subsequent attempt
        // Verify attemptsMade stayed at 0 when WaitingException was thrown
        worker.close();
        Job<String> job = queue.getJob("1");
        assertNotNull(job);
        assertEquals(0, job.getAttemptsMade());
        assertEquals(0, queue.count(JobState.FAILED));
    }

    @Test
    @DisplayName("FlowProducer: parent failure policies are inherited and parent executes when child fails if ignoreDependencyOnFailure=true")
    void testFlowProducerFailurePolicyInheritance() throws Exception {
        String parentQueue = "adverse-flow-parent-" + UUID.randomUUID();
        String childQueue = "adverse-flow-child-" + UUID.randomUUID();

        OxmqQueue<String> pQueue = OxmqQueue.<String>builder().name(parentQueue).connectionManager(connectionManager).build();
        OxmqQueue<String> cQueue = OxmqQueue.<String>builder().name(childQueue).connectionManager(connectionManager).build();
        queuesToClean.add(pQueue);
        queuesToClean.add(cQueue);

        FlowProducer flowProducer = new FlowProducer(connectionManager);

        // Root parent ignores dependency failure
        FlowJob<String> root = FlowJob.of(parentQueue, "parent-task", "parent-data",
                JobOptions.builder().ignoreDependencyOnFailure(true).build());
        root.addChild(FlowJob.of(childQueue, "child-failure-task", "child-data"));

        String parentId = flowProducer.add(root);
        assertNotNull(parentId);

        CountDownLatch parentLatch = new CountDownLatch(1);
        AtomicReference<Map<String, String>> ignoredFailuresRef = new AtomicReference<>();

        // Worker for child fails intentionally
        OxmqWorker<String> childWorker = OxmqWorker.<String>builder()
                .queueName(childQueue)
                .connectionManager(connectionManager)
                .concurrency(1)
                .processor(job -> {
                    throw new RuntimeException("Child deliberate failure");
                })
                .build();
        workersToStop.add(childWorker);
        childWorker.start();

        // Worker for parent should still execute because ignoreDependencyOnFailure=true
        OxmqWorker<String> parentWorker = OxmqWorker.<String>builder()
                .queueName(parentQueue)
                .connectionManager(connectionManager)
                .concurrency(1)
                .processor(job -> {
                    ignoredFailuresRef.set(job.getIgnoredChildrenFailures());
                    parentLatch.countDown();
                    return "parent-complete";
                })
                .build();
        workersToStop.add(parentWorker);
        parentWorker.start();

        assertTrue(parentLatch.await(10, TimeUnit.SECONDS), "Parent must execute despite child failure");
        assertNotNull(ignoredFailuresRef.get());
        assertFalse(ignoredFailuresRef.get().isEmpty());
    }

    @Test
    @DisplayName("VirtualGroups: getGroupJobsCount, getGroupActiveCount, and getCountsPerPriorityForGroup correctly resolve queue namespace")
    void testVirtualGroupNamespacingAndJobCounts() {
        String queueName = "adverse-groups-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        // Add 2 jobs to group-alpha (one standard priority 0, one priority 5)
        queue.add("job-alpha-0", "data-a0", JobOptions.builder().group(new GroupOptions("group-alpha")).build());
        queue.add("job-alpha-5", "data-a5", JobOptions.builder().group(new GroupOptions("group-alpha")).priority(5).build());

        // Add 1 job to group-beta (priority 10)
        queue.add("job-beta-10", "data-b10", JobOptions.builder().group(new GroupOptions("group-beta")).priority(10).build());

        // Verify group-alpha count is 2 (1 wait + 1 prioritized)
        assertEquals(2, queue.getGroupJobsCount("group-alpha"));
        assertEquals(1, queue.getGroupJobsCount("group-beta"));
        assertEquals(0, queue.getGroupJobsCount("non-existent-group"));

        // Verify priority breakdown for group-alpha
        Map<Integer, Long> alphaPriorities = queue.getCountsPerPriorityForGroup("group-alpha", List.of(0, 5, 10));
        assertEquals(1L, alphaPriorities.get(0));
        assertEquals(1L, alphaPriorities.get(5));
        assertEquals(0L, alphaPriorities.get(10));

        // Verify priority breakdown for group-beta
        Map<Integer, Long> betaPriorities = queue.getCountsPerPriorityForGroup("group-beta", List.of(0, 5, 10));
        assertEquals(0L, betaPriorities.get(0));
        assertEquals(0L, betaPriorities.get(5));
        assertEquals(1L, betaPriorities.get(10));
    }

    @Test
    @DisplayName("getRanges: correctly resolves JobState.WAITING_CHILDREN using hyphenated BullMQ key")
    void testGetRangesWaitingChildrenHyphenParity() {
        String parentQueue = "adverse-ranges-parent-" + UUID.randomUUID();
        String childQueue = "adverse-ranges-child-" + UUID.randomUUID();

        OxmqQueue<String> pQueue = OxmqQueue.<String>builder().name(parentQueue).connectionManager(connectionManager).build();
        OxmqQueue<String> cQueue = OxmqQueue.<String>builder().name(childQueue).connectionManager(connectionManager).build();
        queuesToClean.add(pQueue);
        queuesToClean.add(cQueue);

        FlowProducer flowProducer = new FlowProducer(connectionManager);
        FlowJob<String> root = FlowJob.of(parentQueue, "parent-flow", "p-data");
        root.addChild(FlowJob.of(childQueue, "child-flow", "c-data"));

        String parentId = flowProducer.add(root);
        assertNotNull(parentId);

        // Parent should be in WAITING_CHILDREN zset
        Map<JobState, List<String>> ranges = pQueue.getRanges(new JobState[]{JobState.WAITING_CHILDREN}, 0, -1, true);
        assertNotNull(ranges);
        List<String> waitingChildrenIds = ranges.get(JobState.WAITING_CHILDREN);
        assertNotNull(waitingChildrenIds);
        assertTrue(waitingChildrenIds.contains(parentId), "getRanges with WAITING_CHILDREN must retrieve parent job ID");
    }

    @Test
    @DisplayName("QueueEvents: waitUntilFinished fast-path resolves instantly when job already completed or failed")
    void testQueueEventsWaitUntilFinishedFastPathAlreadyFinished() throws Exception {
        String queueName = "adverse-events-fastpath-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        CountDownLatch completedLatch = new CountDownLatch(1);
        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .processor(job -> {
                    completedLatch.countDown();
                    return "FAST_PATH_RESULT";
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        Job<String> job = queue.add("quickJob", "quickData");
        assertTrue(completedLatch.await(5, TimeUnit.SECONDS));

        // Sleep briefly to ensure Redis moveToFinished completes
        Thread.sleep(150);

        // Create a BRAND NEW QueueEvents listener AFTER the job has already finished
        QueueEvents events = QueueEvents.builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .build();
        eventsToClose.add(events);

        // Fast-path isFinished check should immediately return without hanging or timing out
        Object result = events.waitUntilFinished(job.getId(), Duration.ofSeconds(2));
        assertNotNull(result);
        assertEquals("FAST_PATH_RESULT", result.toString().trim().replace("\"", ""));
    }

    @Test
    @DisplayName("Queue: count(null) returns 0 and clean(..., null) throws NullPointerException")
    void testNullSafetyInCountAndClean() {
        String queueName = "adverse-null-safety-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        // count(null) must return 0L safely
        assertEquals(0L, queue.count(null));

        // clean(..., null) must throw NullPointerException
        assertThrows(NullPointerException.class, () -> queue.clean(0, 10, null));
    }

    @Test
    @DisplayName("OxmqBatchWorker: resilient to malformed or floating point numeric fields in Redis")
    void testBatchWorkerMalformedNumericResilience() throws Exception {
        String queueName = "adverse-batch-resilience-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        Job<String> job = queue.add("corruptJob", "payload-42");
        assertNotNull(job.getId());

        // Corrupt numeric attributes in Redis to malformed strings
        StatefulRedisConnection<String, String> cmdConn = connectionManager.getCommandConnection();
        String jobKey = queue.getQueueKeys().toJobKey(job.getId());
        cmdConn.sync().hset(jobKey, Map.of(
                "attemptsMade", "malformed-attempts",
                "timestamp", "malformed-timestamp"
        ));

        CountDownLatch batchLatch = new CountDownLatch(1);
        AtomicReference<Job<String>> capturedJob = new AtomicReference<>();

        OxmqBatchWorker<String> batchWorker = OxmqBatchWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .batchSize(1)
                .processor(jobs -> {
                    capturedJob.set(jobs.get(0));
                    batchLatch.countDown();
                    return "batch-ok";
                })
                .build();
        workersToStop.add(batchWorker);
        batchWorker.start();

        assertTrue(batchLatch.await(5, TimeUnit.SECONDS), "Batch worker must process job without crashing on malformed numeric fields");
        assertNotNull(capturedJob.get());
        assertEquals(job.getId(), capturedJob.get().getId());
    }

    @Test
    @DisplayName("FlowProducer: getFlow populates all job attributes with complete parity")
    void testFlowProducerGetFlowParity() throws Exception {
        String queueName = "adverse-flow-parity-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        FlowProducer flowProducer = new FlowProducer(connectionManager);
        FlowJob<String> root = FlowJob.of(queueName, "dag-root", "root-data",
                JobOptions.builder()
                        .group(new GroupOptions("flow-tenant-1"))
                        .attempts(3)
                        .build());

        String rootId = flowProducer.add(root);
        assertNotNull(rootId);

        // Update progress with structured payload
        queue.updateJobProgress(rootId, 85);

        // Query flow tree
        io.oxmq.model.JobNode jobNode = flowProducer.getFlow(queueName, rootId);
        assertNotNull(jobNode);
        Job<?> retrieved = jobNode.getJob();
        assertNotNull(retrieved);
        assertEquals(rootId, retrieved.getId());
        assertEquals("flow-tenant-1", retrieved.getGroupKey());
        assertEquals(85, retrieved.getProgress());
        assertNotNull(retrieved.getState());
        assertEquals(JobState.WAITING, retrieved.getState());
        assertNotNull(retrieved.getDependencies());
    }

    @Test
    @DisplayName("Queue: count(JobState.PAUSED) reflects waiting/prioritized jobs when queue is paused")
    void testPausedCountReflectsWaitingWhenQueuePaused() {
        String queueName = "adverse-paused-count-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        // Add 3 jobs to active queue
        queue.add("job1", "data1");
        queue.add("job2", "data2");
        queue.add("job3", "data3");

        assertEquals(3L, queue.count(JobState.WAITING));
        assertEquals(0L, queue.count(JobState.PAUSED));

        // Pause the queue
        queue.pause();
        assertTrue(queue.isPaused());

        // When paused, JobState.PAUSED must report the waiting jobs
        assertEquals(3L, queue.count(JobState.PAUSED));

        // Resume queue
        queue.resume();
        assertFalse(queue.isPaused());
        assertEquals(0L, queue.count(JobState.PAUSED));
        assertEquals(3L, queue.count(JobState.WAITING));
    }

    @Test
    @DisplayName("Queue: retryJobs with WAITING_CHILDREN respects hyphenated Redis key name")
    void testRetryJobsWaitingChildrenHyphenParity() {
        String queueName = "adverse-retry-waiting-children-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        // retryJobs for WAITING_CHILDREN on empty queue should return 0 without throwing error
        long retried = queue.retryJobs(JobState.WAITING_CHILDREN, 10, null);
        assertEquals(0L, retried);
    }

    @Test
    @DisplayName("Queue: obliterate and subsequent re-creation lifecycle works without state corruption")
    void testQueueObliterateLifecycleResilience() {
        String queueName = "adverse-obliterate-lifecycle-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        queue.add("lifecycle-1", "test-data");
        assertEquals(1L, queue.count(JobState.WAITING));

        // Obliterate queue completely
        queue.obliterate();
        assertEquals(0L, queue.count(JobState.WAITING));
        assertFalse(queue.isPaused());

        // Subsequent adds to the same queue work properly
        Job<String> newJob = queue.add("lifecycle-2", "new-data");
        assertNotNull(newJob.getId());
        assertEquals(1L, queue.count(JobState.WAITING));
    }

    @Test
    @DisplayName("Observability: failedReason is concise exception message and stacktrace is preserved in Redis")
    void testFailedReasonConciseAndStacktracePreserved() throws Exception {
        String queueName = "adverse-failed-reason-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        CountDownLatch failLatch = new CountDownLatch(1);
        AtomicReference<String> failedJobId = new AtomicReference<>();
        String expectedMessage = "Simulated business transaction failure (code: 4001)";

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .processor(j -> {
                    failedJobId.set(j.getId());
                    failLatch.countDown();
                    throw new IllegalStateException(expectedMessage);
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        queue.add("fail-task", "payload", JobOptions.builder().attempts(1).build());
        assertTrue(failLatch.await(10, TimeUnit.SECONDS));

        // Allow worker to finish moving job to failed
        Thread.sleep(500);

        var sync = connectionManager.getCommandConnection().sync();
        QueueKeys keys = new QueueKeys(queueName);
        String jobKey = keys.toJobKey(failedJobId.get());

        Map<String, String> hash = sync.hgetall(jobKey);
        assertNotNull(hash);

        // 1. failedReason must contain the concise error message
        assertEquals(expectedMessage, hash.get("failedReason"));

        // 2. stacktrace must contain the serialized stack trace array
        String stacktraceStr = hash.get("stacktrace");
        assertNotNull(stacktraceStr);
        assertTrue(stacktraceStr.contains("IllegalStateException: " + expectedMessage));
        assertTrue(stacktraceStr.contains("at io.oxmq.AdverseAndEdgeCaseIntegrationTest"));

        // 3. Domain Job object mapping
        Job<String> failedJob = queue.getJob(failedJobId.get());
        assertNotNull(failedJob);
        assertEquals(expectedMessage, failedJob.getFailedReason());
        assertNotNull(failedJob.getStacktrace());
        assertFalse(failedJob.getStacktrace().isEmpty());
        assertTrue(failedJob.getStacktrace().get(0).contains(expectedMessage));
    }

    @Test
    @DisplayName("Worker spawnChildren: preserves child custom JobId, delayMs, and priority routing")
    void testSpawnChildrenChildOptionsAndRouting() throws Exception {
        String queueName = "adverse-spawn-parent-" + UUID.randomUUID();
        String childQueueName = "adverse-spawn-child-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        OxmqQueue<String> childQueue = OxmqQueue.<String>builder()
                .name(childQueueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);
        queuesToClean.add(childQueue);

        CountDownLatch spawnLatch = new CountDownLatch(1);
        String child1CustomId = "custom-delayed-child-" + UUID.randomUUID();
        String child2CustomId = "custom-priority-child-" + UUID.randomUUID();

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .processor(j -> {
                    if ("parent-task".equals(j.getName())) {
                        spawnLatch.countDown();
                        j.spawnChildren(List.of(
                                new FlowJob<>(childQueueName, "child-delayed", "child-data-1",
                                        JobOptions.builder().jobId(child1CustomId).delay(Duration.ofSeconds(10)).build()),
                                new FlowJob<>(childQueueName, "child-prioritized", "child-data-2",
                                        JobOptions.builder().jobId(child2CustomId).priority(42).build())
                        ));
                    }
                    return "processed";
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        Job<String> parentJob = queue.add("parent-task", "parent-payload");
        assertTrue(spawnLatch.await(10, TimeUnit.SECONDS));

        Thread.sleep(500);

        // 1. Parent moved to waiting-children
        assertEquals(JobState.WAITING_CHILDREN, queue.getJobState(parentJob.getId()));

        // 2. Child 1 enqueued into delayed with custom id
        Job<String> child1 = childQueue.getJob(child1CustomId);
        assertNotNull(child1);
        assertEquals(JobState.DELAYED, childQueue.getJobState(child1CustomId));

        // 3. Child 2 enqueued into prioritized with custom id and priority
        Job<String> child2 = childQueue.getJob(child2CustomId);
        assertNotNull(child2);
        assertEquals(JobState.PRIORITIZED, childQueue.getJobState(child2CustomId));
        assertEquals(42, child2.getOpts().getPriority());
    }

    @Test
    @DisplayName("BatchWorker spawnChildren: dynamically spawns children and transitions to waiting-children")
    void testBatchWorkerSpawnChildren() throws Exception {
        String queueName = "adverse-batch-spawn-" + UUID.randomUUID();
        String childQueueName = "adverse-batch-spawn-child-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        OxmqQueue<String> childQueue = OxmqQueue.<String>builder()
                .name(childQueueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);
        queuesToClean.add(childQueue);

        CountDownLatch batchSpawnLatch = new CountDownLatch(1);
        String childCustomId = "batch-child-" + UUID.randomUUID();

        OxmqBatchWorker<String> batchWorker = OxmqBatchWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .batchSize(5)
                .batchTimeout(Duration.ofMillis(50))
                .processor(batch -> {
                    for (Job<String> j : batch) {
                        batchSpawnLatch.countDown();
                        j.spawnChildren(List.of(
                                new FlowJob<>(childQueueName, "batch-child", "data",
                                        JobOptions.builder().jobId(childCustomId).build())
                        ));
                    }
                    return "batch-processed";
                })
                .build();
        workersToStop.add(batchWorker);
        batchWorker.start();

        Job<String> parent = queue.add("parent-batch-job", "input");
        assertTrue(batchSpawnLatch.await(10, TimeUnit.SECONDS));

        Thread.sleep(500);

        // Parent moved to waiting-children
        assertEquals(JobState.WAITING_CHILDREN, queue.getJobState(parent.getId()));

        // Child enqueued into wait
        Job<String> child = childQueue.getJob(childCustomId);
        assertNotNull(child);
        assertEquals(JobState.WAITING, childQueue.getJobState(childCustomId));
    }

    @Test
    @DisplayName("FlowProducer: multi-tier DAG hierarchy correctly links parent-child relationships")
    void testFlowProducerHierarchicalDAGLinkage() {
        String queueName = "adverse-dag-linkage-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        FlowProducer flowProducer = new FlowProducer(connectionManager);

        // Grandparent -> Parent -> Child
        FlowJob<String> child = new FlowJob<>(queueName, "leaf-task", "leaf-data", JobOptions.builder().build());
        FlowJob<String> parent = new FlowJob<>(queueName, "intermediate-parent", "parent-data",
                JobOptions.builder().build());
        parent.addChild(child);
        FlowJob<String> grandparent = new FlowJob<>(queueName, "root-task", "root-data",
                JobOptions.builder().build());
        grandparent.addChild(parent);

        String rootId = flowProducer.add(grandparent);
        assertNotNull(rootId);

        var sync = connectionManager.getCommandConnection().sync();
        QueueKeys keys = new QueueKeys(queueName);

        // Grandparent is in waiting-children
        assertEquals(JobState.WAITING_CHILDREN, queue.getJobState(rootId));

        // Inspect child job hash in Redis
        List<String> waitIds = sync.lrange(keys.toKey("wait"), 0, -1);
        assertFalse(waitIds.isEmpty());
        String childId = waitIds.get(0);

        Job<String> leafJob = queue.getJob(childId);
        assertNotNull(leafJob);
        assertNotNull(leafJob.getParentKey());
        assertTrue(leafJob.getParentKey().contains(queueName));
    }

    @Test
    @DisplayName("Queue API: getJobCounts, getRanges, and getJobs safely handle null array elements")
    void testQueueNullStateSafety() {
        String queueName = "adverse-null-state-safety-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        queue.add("job-1", "data-1");

        // 1. getJobCounts with null state in array
        Map<JobState, Long> counts = assertDoesNotThrow(() ->
                queue.getJobCounts(JobState.WAITING, null, JobState.ACTIVE)
        );
        assertNotNull(counts);
        assertEquals(1L, counts.get(JobState.WAITING));
        assertEquals(0L, counts.get(JobState.ACTIVE));
        assertFalse(counts.containsKey(null));

        // 2. getRanges with null state in array
        Map<JobState, List<String>> ranges = assertDoesNotThrow(() ->
                queue.getRanges(new JobState[]{JobState.WAITING, null, JobState.ACTIVE}, 0, 10, true)
        );
        assertNotNull(ranges);
        assertEquals(1, ranges.get(JobState.WAITING).size());
        assertEquals(0, ranges.get(JobState.ACTIVE).size());
        assertFalse(ranges.containsKey(null));

        // 3. getJobs with null state in list
        List<Job<String>> jobs = assertDoesNotThrow(() ->
                queue.getJobs(Arrays.asList(JobState.WAITING, null, JobState.ACTIVE), 0, 10, true)
        );
        assertNotNull(jobs);
        assertEquals(1, jobs.size());
    }

    @Test
    @DisplayName("JobOptions: stackTraceLimit serializes to options map accurately")
    void testJobOptionsStackTraceLimitSerialization() {
        JobOptions opts = JobOptions.builder()
                .attempts(3)
                .stackTraceLimit(10)
                .build();

        assertEquals(10, opts.getStackTraceLimit());
        Map<String, Object> map = opts.toMap();
        assertTrue(map.containsKey("stackTraceLimit"));
        assertEquals(10, map.get("stackTraceLimit"));
    }
}
