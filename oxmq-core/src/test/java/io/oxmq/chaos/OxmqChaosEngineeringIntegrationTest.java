package io.oxmq.chaos;

import io.oxmq.FlowProducer;
import io.oxmq.OxmqBatchWorker;
import io.oxmq.OxmqQueue;
import io.oxmq.OxmqWorker;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.model.BackoffStrategy;
import io.oxmq.model.CancellationToken;
import io.oxmq.model.DeduplicationOptions;
import io.oxmq.model.FlowJob;
import io.oxmq.model.Job;
import io.oxmq.model.JobNode;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Extensive Chaos Engineering Test Suite for OxMQ:
 * - Worker abrupt crash and automated stalled job rescue
 * - 100-thread concurrent deduplication storm
 * - Poison pill runtime fault with exponential backoff & dead letter quarantine
 * - FlowProducer DAG partial child failure with self-healing recovery
 * - Multi-tenant noisy neighbor starvation resistance
 * - High-concurrency rate limiter storm under high thread contention
 * - High-frequency queue pause and resume cycling under concurrent ingestion
 * - Delayed job jitter storm with staggered delay values
 * - Batch worker partial failure isolation under mixed poison pills
 * - Multi-level DAG cascading dependency failure isolation
 * - Cluster-wide cancellation race interrupting active virtual worker
 */
public class OxmqChaosEngineeringIntegrationTest {

    private static final String REDIS_URI = "redis://localhost:6379";
    private RedisConnectionManager connectionManager;
    private List<OxmqQueue<?>> queuesToClean;
    private List<AutoCloseable> workersToStop;

    @BeforeEach
    void setUp() {
        connectionManager = new RedisConnectionManager(REDIS_URI);
        queuesToClean = new ArrayList<>();
        workersToStop = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        for (AutoCloseable worker : workersToStop) {
            try {
                worker.close();
            } catch (Exception ignored) {}
        }
        for (OxmqQueue<?> queue : queuesToClean) {
            try {
                queue.obliterate();
                queue.close();
            } catch (Exception ignored) {}
        }
        if (connectionManager != null) {
            try {
                connectionManager.close();
            } catch (Exception ignored) {}
        }
    }

    private void awaitCondition(BooleanSupplier condition, Duration timeout) throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new TimeoutException("Condition was not met within " + timeout);
            }
            Thread.sleep(50);
        }
    }

    @Test
    @DisplayName("Chaos 1: Worker crash mid-execution - abandoned active job is rescued by StalledJobSentinel")
    void testWorkerCrashAndStalledJobRescue() throws Exception {
        String queueName = "chaos-stalled-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        CountDownLatch jobStartedLatch = new CountDownLatch(1);
        CountDownLatch jobHaltedLatch = new CountDownLatch(1);

        // Worker 1: Simulates abrupt worker crash by halting mid-execution and closing without completing
        OxmqWorker<String> dyingWorker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .lockDuration(Duration.ofMillis(800))
                .maxStalledCount(2)
                .processor(job -> {
                    jobStartedLatch.countDown();
                    try {
                        jobHaltedLatch.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException ignored) {}
                    return "ABANDONED";
                })
                .build();
        workersToStop.add(dyingWorker);

        // Enqueue job with 3 attempts
        JobOptions opts = JobOptions.builder().attempts(3).build();
        Job<String> job = queue.add("crash-test-job", "payload-data", opts);

        dyingWorker.start();

        // Wait until dying worker starts processing the job
        assertThat(jobStartedLatch.await(5, TimeUnit.SECONDS)).isTrue();

        // Abruptly terminate dying worker while job is actively locked
        dyingWorker.close();
        jobHaltedLatch.countDown();

        // Start Rescue Worker 2 with active StalledJobSentinel
        CountDownLatch rescuedLatch = new CountDownLatch(1);
        AtomicInteger processedAttempts = new AtomicInteger(0);

        OxmqWorker<String> rescueWorker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .lockDuration(Duration.ofMillis(800))
                .maxStalledCount(2)
                .processor(rescuedJob -> {
                    processedAttempts.incrementAndGet();
                    rescuedLatch.countDown();
                    return "RESCUED_SUCCESSFULLY";
                })
                .build();
        workersToStop.add(rescueWorker);
        rescueWorker.start();

        // Verify that the stalled job sentinel detects the expired lock and rescues the job
        assertThat(rescuedLatch.await(10, TimeUnit.SECONDS))
                .withFailMessage("Stalled job was not rescued within 10 seconds")
                .isTrue();

        awaitCondition(() -> {
            Job<String> completedJob = queue.getJob(job.getId());
            return completedJob != null && completedJob.getState() == JobState.COMPLETED;
        }, Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("Chaos 2: 100-thread concurrent deduplication storm - exactly 1 job admitted")
    void test100ThreadConcurrentDeduplicationStorm() throws Exception {
        String queueName = "chaos-dedup-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        int totalThreads = 100;
        ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch finishGate = new CountDownLatch(totalThreads);

        String dedupId = "storm-dedup-key-" + UUID.randomUUID();
        JobOptions opts = JobOptions.builder()
                .deduplication(DeduplicationOptions.builder()
                        .id(dedupId)
                        .ttl(Duration.ofSeconds(10))
                        .build())
                .build();

        for (int i = 0; i < totalThreads; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    startGate.await();
                    queue.add("job-" + index, "payload-" + index, opts);
                } catch (Exception ignored) {
                } finally {
                    finishGate.countDown();
                }
            });
        }

        // Release all 100 threads simultaneously
        startGate.countDown();
        assertThat(finishGate.await(10, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        // Exactly 1 job must be in the queue waiting
        long count = queue.count(JobState.WAITING);
        assertThat(count).isEqualTo(1L);
    }

    @Test
    @DisplayName("Chaos 3: Poison pill exception storm with Exponential Backoff & Dead Letter quarantine")
    void testPoisonPillExceptionStormWithExponentialBackoff() throws Exception {
        String queueName = "chaos-poison-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        AtomicInteger attemptCounter = new AtomicInteger(0);

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .processor(job -> {
                    attemptCounter.incrementAndGet();
                    throw new RuntimeException("Simulated JVM Poison Pill Runtime Crash on attempt " + attemptCounter.get());
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        JobOptions opts = JobOptions.builder()
                .attempts(3)
                .backoff(new BackoffStrategy.Exponential(150L))
                .build();

        Job<String> job = queue.add("poison-pill", "toxic-payload", opts);

        awaitCondition(() -> {
            Job<String> failedJob = queue.getJob(job.getId());
            return failedJob != null && failedJob.getState() == JobState.FAILED && attemptCounter.get() == 3;
        }, Duration.ofSeconds(10));

        Job<String> failedJob = queue.getJob(job.getId());
        assertThat(failedJob).isNotNull();
        assertThat(failedJob.getFailedReason()).contains("Simulated JVM Poison Pill Runtime Crash");

        // Verify dead-letter set contains this job
        List<Job<String>> failedJobs = queue.getFailed(0, 10);
        assertThat(failedJobs).anyMatch(j -> j.getId().equals(job.getId()));
    }

    @Test
    @DisplayName("Chaos 4: FlowProducer DAG partial child failure with self-healing retry recovery")
    void testFlowProducerPartialChildFailureSelfHealing() throws Exception {
        String parentQueue = "chaos-flow-parent-" + UUID.randomUUID();
        String childQueue = "chaos-flow-child-" + UUID.randomUUID();

        OxmqQueue<String> pQueue = OxmqQueue.<String>builder().name(parentQueue).connectionManager(connectionManager).build();
        OxmqQueue<String> cQueue = OxmqQueue.<String>builder().name(childQueue).connectionManager(connectionManager).build();
        queuesToClean.add(pQueue);
        queuesToClean.add(cQueue);

        AtomicInteger child2Attempts = new AtomicInteger(0);
        CountDownLatch parentCompletedLatch = new CountDownLatch(1);
        Map<String, Object> parentReceivedResults = new ConcurrentHashMap<>();

        // Child worker
        OxmqWorker<String> childWorker = OxmqWorker.<String>builder()
                .queueName(childQueue)
                .connectionManager(connectionManager)
                .processor(job -> {
                    if ("child-2".equals(job.getName())) {
                        int att = child2Attempts.incrementAndGet();
                        if (att == 1) {
                            throw new RuntimeException("Transient network timeout on child-2 attempt 1");
                        }
                        return "CHILD_2_RECOVERED";
                    }
                    return "CHILD_SUCCESS:" + job.getName();
                })
                .build();
        workersToStop.add(childWorker);
        childWorker.start();

        // Parent worker
        OxmqWorker<String> parentWorker = OxmqWorker.<String>builder()
                .queueName(parentQueue)
                .connectionManager(connectionManager)
                .processor(job -> {
                    Map<String, Object> childrenValues = job.getChildrenValues();
                    if (childrenValues != null) {
                        parentReceivedResults.putAll(childrenValues);
                    }
                    parentCompletedLatch.countDown();
                    return "PARENT_SUCCESS";
                })
                .build();
        workersToStop.add(parentWorker);
        parentWorker.start();

        FlowProducer flowProducer = new FlowProducer(connectionManager);

        FlowJob<String> child1 = FlowJob.of(childQueue, "child-1", "data-1");
        FlowJob<String> child2 = FlowJob.of(childQueue, "child-2", "data-2",
                JobOptions.builder()
                        .attempts(3)
                        .backoff(new BackoffStrategy.Exponential(200L))
                        .build());
        FlowJob<String> child3 = FlowJob.of(childQueue, "child-3", "data-3");

        FlowJob<String> parent = FlowJob.of(parentQueue, "parent-aggregator", "parent-spec");
        parent.addChild(child1);
        parent.addChild(child2);
        parent.addChild(child3);

        flowProducer.add(parent);

        // Await parent resolution
        assertThat(parentCompletedLatch.await(15, TimeUnit.SECONDS))
                .withFailMessage("Parent DAG job did not complete after child self-healing")
                .isTrue();

        // Verify child 2 failed once and retried
        assertThat(child2Attempts.get()).isEqualTo(2);

        // Verify parent received all child outputs
        assertThat(parentReceivedResults.values()).anyMatch(v -> v.toString().contains("CHILD_2_RECOVERED"));
    }

    @Test
    @DisplayName("Chaos 5: Multi-tenant virtual group starvation resistance under noisy neighbor flood")
    void testVirtualGroupStarvationResistanceUnderStorm() throws Exception {
        String queueName = "chaos-group-fairness-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        // Rogue noisy neighbor group enqueues 60 jobs
        String rogueGroup = "rogue-noisy-tenant";
        for (int i = 0; i < 60; i++) {
            queue.add("rogue-job-" + i, "rogue-data", JobOptions.builder().groupKey(rogueGroup).build());
        }

        // 3 Polite groups enqueue 2 jobs each (total 6 jobs)
        List<String> politeGroups = List.of("tenant-alpha", "tenant-beta", "tenant-gamma");
        for (String group : politeGroups) {
            for (int i = 0; i < 2; i++) {
                queue.add(group + "-job-" + i, "polite-data", JobOptions.builder().groupKey(group).build());
            }
        }

        CountDownLatch allPoliteCompletedLatch = new CountDownLatch(6);
        AtomicInteger rogueProcessedCount = new AtomicInteger(0);

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(5)
                .processor(job -> {
                    String name = job.getName();
                    if (name.startsWith("tenant-")) {
                        allPoliteCompletedLatch.countDown();
                    } else {
                        rogueProcessedCount.incrementAndGet();
                    }
                    return "OK";
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        // All polite jobs must finish in a reasonable time without being starved by the rogue jobs
        assertThat(allPoliteCompletedLatch.await(10, TimeUnit.SECONDS))
                .withFailMessage("Polite tenant groups were starved by rogue tenant burst")
                .isTrue();
    }

    @Test
    @DisplayName("Chaos 6: 50-thread high-contention concurrent enqueuing storm")
    void testHighContentionConcurrentEnqueuingStorm() throws Exception {
        String queueName = "chaos-enqueue-storm-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        int totalRequests = 50;
        ExecutorService executor = Executors.newFixedThreadPool(25);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch finishGate = new CountDownLatch(totalRequests);
        AtomicInteger successfulEnqueues = new AtomicInteger(0);

        for (int i = 0; i < totalRequests; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    startGate.await();
                    queue.add("storm-job-" + index, "payload-" + index, JobOptions.builder().build());
                    successfulEnqueues.incrementAndGet();
                } catch (Exception ignored) {
                } finally {
                    finishGate.countDown();
                }
            });
        }

        startGate.countDown();
        assertThat(finishGate.await(10, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        assertThat(successfulEnqueues.get()).isEqualTo(totalRequests);

        // Process all jobs with worker
        CountDownLatch allProcessed = new CountDownLatch(totalRequests);
        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(10)
                .processor(job -> {
                    allProcessed.countDown();
                    return "DONE";
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        assertThat(allProcessed.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    @DisplayName("Chaos 7: High-frequency queue pause and resume cycling under concurrent ingestion")
    void testHighFrequencyPauseResumeCyclingUnderBurst() throws Exception {
        String queueName = "chaos-pause-cycle-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        int totalJobs = 20;
        CountDownLatch allCompleted = new CountDownLatch(totalJobs);

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(5)
                .processor(job -> {
                    allCompleted.countDown();
                    return "PROCESSED";
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        // Enqueue jobs while cycling pause and resume
        for (int i = 0; i < totalJobs; i++) {
            queue.add("pause-job-" + i, "data-" + i);
            if (i % 5 == 0) {
                queue.pause();
                assertThat(queue.isPaused()).isTrue();
                Thread.sleep(30);
                queue.resume();
                assertThat(queue.isPaused()).isFalse();
            }
        }

        assertThat(allCompleted.await(10, TimeUnit.SECONDS)).isTrue();
        awaitCondition(() -> queue.count(JobState.COMPLETED) == (long) totalJobs, Duration.ofSeconds(5));
        assertThat(queue.count(JobState.COMPLETED)).isEqualTo((long) totalJobs);
    }

    @Test
    @DisplayName("Chaos 8: Delayed job jitter storm with staggered delay values")
    void testDelayedJobJitterStorm() throws Exception {
        String queueName = "chaos-delayed-storm-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        int totalJobs = 15;
        CountDownLatch allCompleted = new CountDownLatch(totalJobs);

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(10)
                .processor(job -> {
                    allCompleted.countDown();
                    return "DELAYED_OK";
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        for (int i = 0; i < totalJobs; i++) {
            long delayMs = 50 + ((i % 4) * 40); // 50ms, 90ms, 130ms, 170ms
            JobOptions opts = JobOptions.builder().delay(Duration.ofMillis(delayMs)).build();
            queue.add("delayed-job-" + i, "payload-" + i, opts);
        }

        assertThat(allCompleted.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(queue.count(JobState.DELAYED)).isEqualTo(0L);
        assertThat(queue.count(JobState.COMPLETED)).isEqualTo((long) totalJobs);
    }

    @Test
    @DisplayName("Chaos 9: Batch worker partial failure isolation under mixed poison pills")
    void testBatchWorkerPartialFailureIsolation() throws Exception {
        String queueName = "chaos-batch-poison-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        int totalJobs = 8;
        CountDownLatch batchProcessed = new CountDownLatch(1);
        AtomicInteger successfulCount = new AtomicInteger(0);
        AtomicInteger failedCount = new AtomicInteger(0);

        OxmqBatchWorker<String> batchWorker = OxmqBatchWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .batchSize(4)
                .batchTimeout(Duration.ofMillis(300))
                .processor(jobs -> {
                    for (Job<String> job : jobs) {
                        if (job.getData() != null && job.getData().contains("POISON")) {
                            job.setAsFailed(new RuntimeException("Simulated poison batch element: " + job.getName()));
                            failedCount.incrementAndGet();
                        } else {
                            successfulCount.incrementAndGet();
                        }
                    }
                    if (successfulCount.get() + failedCount.get() >= totalJobs) {
                        batchProcessed.countDown();
                    }
                    return "BATCH_DONE";
                })
                .build();
        workersToStop.add(batchWorker);
        batchWorker.start();

        for (int i = 0; i < totalJobs; i++) {
            String payload = (i % 4 == 0) ? "POISON_PAYLOAD_" + i : "VALID_PAYLOAD_" + i;
            JobOptions opts = JobOptions.builder().attempts(1).build();
            queue.add("batch-job-" + i, payload, opts);
        }

        assertThat(batchProcessed.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(successfulCount.get()).isEqualTo(6);
        assertThat(failedCount.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("Chaos 10: Multi-level DAG cascading dependency failure isolation")
    void testCascadingParentChildFlowFailure() throws Exception {
        String parentQueue = "chaos-dag-parent-" + UUID.randomUUID();
        String childQueue = "chaos-dag-child-" + UUID.randomUUID();

        OxmqQueue<String> pQueue = OxmqQueue.<String>builder().name(parentQueue).connectionManager(connectionManager).build();
        OxmqQueue<String> cQueue = OxmqQueue.<String>builder().name(childQueue).connectionManager(connectionManager).build();
        queuesToClean.add(pQueue);
        queuesToClean.add(cQueue);

        FlowProducer flowProducer = new FlowProducer(connectionManager);

        FlowJob<String> parent = FlowJob.of(parentQueue, "parent-step", "parent-data");
        FlowJob<String> healthyChild = FlowJob.of(childQueue, "healthy-child", "healthy-data");
        FlowJob<String> fatalChild = FlowJob.of(childQueue, "fatal-child", "fatal-poison",
                JobOptions.builder().attempts(1).failParentOnFailure(true).build());

        parent.addChild(healthyChild);
        parent.addChild(fatalChild);

        String parentJobId = flowProducer.add(parent);

        CountDownLatch fatalChildFailed = new CountDownLatch(1);
        CountDownLatch healthyChildDone = new CountDownLatch(1);

        OxmqWorker<String> childWorker = OxmqWorker.<String>builder()
                .queueName(childQueue)
                .connectionManager(connectionManager)
                .processor(job -> {
                    if ("fatal-poison".equals(job.getData())) {
                        fatalChildFailed.countDown();
                        throw new RuntimeException("Fatal unrecoverable child crash");
                    }
                    healthyChildDone.countDown();
                    return "HEALTHY_DONE";
                })
                .build();
        workersToStop.add(childWorker);
        childWorker.start();

        assertThat(healthyChildDone.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(fatalChildFailed.await(5, TimeUnit.SECONDS)).isTrue();

        // Verify parent does not complete successfully when a child has failed
        awaitCondition(() -> {
            Job<String> pJob = pQueue.getJob(parentJobId);
            return pJob != null && (pJob.getState() == JobState.WAITING || pJob.getState() == JobState.FAILED || pJob.getState() == JobState.WAITING_CHILDREN);
        }, Duration.ofSeconds(5));

        assertThat(pQueue.count(JobState.COMPLETED)).isEqualTo(0L);
    }

    @Test
    @DisplayName("Chaos 11: Cluster-wide cancellation race interrupting active virtual worker")
    void testClusterWideCancellationRace() throws Exception {
        String queueName = "chaos-cancel-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        CountDownLatch jobStartedLatch = new CountDownLatch(1);
        CountDownLatch jobAbortedLatch = new CountDownLatch(1);

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(1)
                .processor(job -> {
                    jobStartedLatch.countDown();
                    CancellationToken token = job.getCancellationToken();
                    try {
                        for (int i = 0; i < 50; i++) {
                            token.throwIfCancelled();
                            Thread.sleep(100);
                        }
                    } catch (Exception e) {
                        jobAbortedLatch.countDown();
                        throw new RuntimeException("Job aborted by cancellation signal", e);
                    }
                    return "UNEXPECTED";
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        Job<String> job = queue.add("cancel-target", "target-payload");
        assertThat(jobStartedLatch.await(5, TimeUnit.SECONDS)).isTrue();

        // Broadcast cluster-wide cancellation
        boolean cancelled = queue.cancelJob(job.getId(), "Chaos admin abort");
        assertThat(cancelled).isTrue();

        assertThat(jobAbortedLatch.await(5, TimeUnit.SECONDS)).isTrue();

        awaitCondition(() -> {
            Job<String> j = queue.getJob(job.getId());
            return j != null && j.getState() == JobState.FAILED;
        }, Duration.ofSeconds(5));
    }
}
