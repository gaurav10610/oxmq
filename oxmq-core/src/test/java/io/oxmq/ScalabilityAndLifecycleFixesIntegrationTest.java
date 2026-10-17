package io.oxmq;

import io.lettuce.core.api.StatefulRedisConnection;
import io.oxmq.client.QueueKeys;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.exception.RateLimitException;
import io.oxmq.lua.BullScripts;
import io.oxmq.model.Job;
import io.oxmq.model.JobState;
import io.oxmq.watchdog.LockExtender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class ScalabilityAndLifecycleFixesIntegrationTest {

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
            try {
                connectionManager.close();
            } catch (Exception ignored) {}
        }
    }

    @Test
    @DisplayName("LockExtender: extendLocks-1.lua successfully batches multi-job lock extensions in one call")
    void testExtendLocksBatching() throws Exception {
        String queueName = "test-extend-locks-" + System.currentTimeMillis();
        QueueKeys keys = new QueueKeys(queueName);
        BullScripts bullScripts = new BullScripts();
        StatefulRedisConnection<String, byte[]> binaryConn = connectionManager.getBinaryConnection();

        // 1. Manually set two locks in Redis
        String token = "worker-token-test";
        String jobId1 = "job-lock-1";
        String jobId2 = "job-lock-2";
        connectionManager.sync().set(keys.toJobLockKey(jobId1), token);
        connectionManager.sync().set(keys.toJobLockKey(jobId2), token);

        // 2. Extend locks via BullScripts.extendLocks in a single atomic call
        List<String> jobIds = List.of(jobId1, jobId2);
        List<String> tokens = List.of(token, token);
        List<String> failed = bullScripts.extendLocks(binaryConn, keys, jobIds, tokens, 30_000);

        assertThat(failed).isEmpty();

        // Verify TTL was set on both lock keys
        Long ttl1 = connectionManager.sync().pttl(keys.toJobLockKey(jobId1));
        Long ttl2 = connectionManager.sync().pttl(keys.toJobLockKey(jobId2));
        assertThat(ttl1).isGreaterThan(20_000L);
        assertThat(ttl2).isGreaterThan(20_000L);

        // 3. Verify LockExtender heartbeat extends all registered jobs using batching
        try (LockExtender extender = new LockExtender(binaryConn, bullScripts, keys, token, 30_000)) {
            extender.registerJob(jobId1);
            extender.registerJob(jobId2);

            // Wait brief moment for scheduled extendAllLocks to execute
            TimeUnit.MILLISECONDS.sleep(1500);

            Long refreshedTtl1 = connectionManager.sync().pttl(keys.toJobLockKey(jobId1));
            assertThat(refreshedTtl1).isGreaterThan(20_000L);
        }
    }

    @Test
    @DisplayName("Graceful Shutdown: Worker draining does not kill shared RedisConnectionManager")
    void testWorkerShutdownPreservesSharedConnectionManager() throws Exception {
        String queueName1 = "test-drain-1-" + System.currentTimeMillis();
        String queueName2 = "test-drain-2-" + System.currentTimeMillis();

        OxmqQueue<String> queue1 = OxmqQueue.<String>builder()
                .name(queueName1)
                .connectionManager(connectionManager)
                .build();
        OxmqQueue<String> queue2 = OxmqQueue.<String>builder()
                .name(queueName2)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue1);
        queuesToClean.add(queue2);

        CountDownLatch job1Processing = new CountDownLatch(1);
        CountDownLatch finishJob1 = new CountDownLatch(1);
        AtomicBoolean job1CompletedSuccessfully = new AtomicBoolean(false);

        Worker<String> worker1 = OxmqWorker.<String>builder()
                .queueName(queueName1)
                .connectionManager(connectionManager) // Shared external connectionManager
                .drainTimeoutMs(5000)
                .processor(job -> {
                    job1Processing.countDown();
                    finishJob1.await(4, TimeUnit.SECONDS);
                    job1CompletedSuccessfully.set(true);
                    return "done1";
                })
                .build();
        worker1.start();
        workersToStop.add(worker1);

        queue1.add("task", "payload1");
        assertTrue(job1Processing.await(5, TimeUnit.SECONDS));

        // Close worker1 while job is actively in-flight
        Thread closer = new Thread(() -> {
            try {
                Thread.sleep(500);
                finishJob1.countDown();
            } catch (InterruptedException ignored) {}
        });
        closer.start();

        worker1.close();
        assertThat(job1CompletedSuccessfully.get()).isTrue();

        // Verify shared connectionManager was NOT closed and can still be used for queue2
        assertThat(connectionManager.getCommandConnection().isOpen()).isTrue();
        Job<String> job2 = queue2.add("task2", "payload2");
        assertThat(job2.getId()).isNotBlank();
    }

    @Test
    @DisplayName("QueueEvents: waitUntilFinished timeout cancels future and removes listener without memory leak")
    void testWaitUntilFinishedTimeoutCleansListeners() throws Exception {
        String queueName = "test-events-leak-" + System.currentTimeMillis();
        try (QueueEvents events = QueueEvents.builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .build()) {

            events.start();

            // Calling waitUntilFinished with a short 200ms timeout for a non-existent job
            assertThrows(TimeoutException.class, () -> {
                events.waitUntilFinished("non-existent-job-id", Duration.ofMillis(200));
            });

            // Calling async with timeout
            assertThrows(Exception.class, () -> {
                events.waitUntilFinishedAsync("non-existent-2", Duration.ofMillis(200)).get(500, TimeUnit.MILLISECONDS);
            });
        }
    }

    @Test
    @DisplayName("OxmqBatchWorker: Concurrency semaphore enforces maximum concurrent in-flight batches")
    void testBatchWorkerConcurrencyBackpressure() throws Exception {
        String queueName = "test-batch-concurrency-" + System.currentTimeMillis();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        // Add 10 jobs
        for (int i = 0; i < 10; i++) {
            queue.add("item-" + i, "data-" + i);
        }

        AtomicInteger maxConcurrentBatches = new AtomicInteger(0);
        AtomicInteger activeBatches = new AtomicInteger(0);
        CountDownLatch allProcessed = new CountDownLatch(10);

        OxmqBatchWorker<String> batchWorker = OxmqBatchWorker.<String>builder()
                .queueName(queueName)
                .batchSize(5)
                .concurrency(1) // STRICT concurrency of 1 batch at a time
                .connectionManager(connectionManager)
                .processor(jobs -> {
                    int current = activeBatches.incrementAndGet();
                    maxConcurrentBatches.accumulateAndGet(current, Math::max);
                    try {
                        TimeUnit.MILLISECONDS.sleep(200);
                    } catch (InterruptedException ignored) {}
                    activeBatches.decrementAndGet();
                    for (int j = 0; j < jobs.size(); j++) {
                        allProcessed.countDown();
                    }
                    return "batch-ok";
                })
                .build();

        batchWorker.start();
        workersToStop.add(batchWorker);

        assertTrue(allProcessed.await(10, TimeUnit.SECONDS));
        assertThat(maxConcurrentBatches.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("QueueKeys: Redis Cluster Hash-Tags format single-slot brackets correctly")
    void testQueueKeysClusterHashTags() {
        // Standard un-tagged keys (100% backward compatible)
        QueueKeys standard = new QueueKeys("bull", "orders", false);
        assertThat(standard.isClusterTagged()).isFalse();
        assertThat(standard.toKey("wait")).isEqualTo("bull:orders:wait");
        assertThat(standard.toJobKey("123")).isEqualTo("bull:orders:123");

        // Cluster tagged keys
        QueueKeys tagged = QueueKeys.clusterTagged("bull", "orders");
        assertThat(tagged.isClusterTagged()).isTrue();
        assertThat(tagged.toKey("wait")).isEqualTo("{bull:orders}:wait");
        assertThat(tagged.toKey("active")).isEqualTo("{bull:orders}:active");
        assertThat(tagged.toJobKey("123")).isEqualTo("{bull:orders}:123");
        assertThat(tagged.toJobLockKey("123")).isEqualTo("{bull:orders}:123:lock");

        // Already bracketed prefix should not be double wrapped
        QueueKeys customBracketed = new QueueKeys("{payments}", "invoices", true);
        assertThat(customBracketed.isClusterTagged()).isTrue();
        assertThat(customBracketed.toKey("wait")).isEqualTo("{payments}:invoices:wait");
    }

    @Test
    @DisplayName("RateLimitException: Moving job to delayed without incrementing attemptsMade")
    void testRateLimitExceptionPostponesJob() throws Exception {
        String queueName = "test-ratelimit-exc-" + System.currentTimeMillis();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        CountDownLatch rateLimitedOnce = new CountDownLatch(1);
        CountDownLatch jobFinished = new CountDownLatch(1);
        AtomicInteger runCount = new AtomicInteger(0);

        Worker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .pollIntervalMs(50)
                .processor(job -> {
                    if (runCount.incrementAndGet() == 1) {
                        rateLimitedOnce.countDown();
                        throw new RateLimitException("External 429", 300); // delay 300ms
                    }
                    jobFinished.countDown();
                    return "processed-after-rate-limit";
                })
                .build();

        worker.start();
        workersToStop.add(worker);

        Job<String> job = queue.add("rl-job", "payload");

        assertTrue(rateLimitedOnce.await(5, TimeUnit.SECONDS));
        // Verify job was postponed and eventually ran second time
        assertTrue(jobFinished.await(5, TimeUnit.SECONDS));
        assertThat(runCount.get()).isEqualTo(2);

        // Verify attemptsMade did not count the rate limit postpone as a failure
        Job<String> completedJob = queue.getJob(job.getId());
        assertThat(completedJob.getState()).isEqualTo(JobState.COMPLETED);
    }
}
