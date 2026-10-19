package io.oxmq.plugin;

import io.oxmq.OxmqQueue;
import io.oxmq.OxmqWorker;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.exception.RateLimitCooldownException;
import io.oxmq.model.JobOptions;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class AdaptiveAndCheckpointPluginsIntegrationTest {

    private RedisConnectionManager connectionManager;
    private String queueName;
    private OxmqQueue<String> queue;
    private OxmqWorker<String> worker;

    @BeforeEach
    void setUp() {
        connectionManager = new RedisConnectionManager("redis://localhost:6379");
        queueName = "test-adaptive-ckpt-" + System.currentTimeMillis();
        queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
    }

    @AfterEach
    void tearDown() {
        if (worker != null) {
            worker.close();
        }
        if (queue != null) {
            queue.obliterate();
            queue.close();
        }
        if (connectionManager != null) {
            connectionManager.close();
        }
    }

    @Test
    void testAdaptiveRateLimiterPlugin_AimdAndDamping() throws Exception {
        AdaptiveRateLimiterPlugin adaptive = AdaptiveRateLimiterPlugin.builder()
                .connectionManager(connectionManager)
                .keyPrefix("oxmq:test:adaptive:" + System.currentTimeMillis())
                .groupKeyResolver(job -> (String) job.getData())
                .initialLimit(50)
                .minLimit(10)
                .maxLimit(100)
                .increaseStep(10)
                .backoffFactor(0.5) // Halve on throttle
                .dampingDuration(Duration.ofMillis(300)) // 300ms damping lock for test speed
                .cleanThreshold(2) // 2 clean executions needed to upgrade
                .build();

        String tenant = "tenant-aimd-test";

        // Initial limit should be 50
        assertEquals(50, adaptive.getLimit(tenant));

        // 1. Trigger throttle -> AIMD Multiplicative Decrease: 50 * 0.5 = 25
        adaptive.recordThrottle(tenant);
        assertEquals(25, adaptive.getLimit(tenant));
        assertEquals(1, adaptive.getDowngradeCount(tenant));
        assertTrue(adaptive.isDampingActive(tenant), "Damping lock must be active after throttle");

        // 2. While damping is active, clean execution should NOT upgrade
        adaptive.recordCleanExecution(tenant);
        adaptive.recordCleanExecution(tenant);
        assertEquals(25, adaptive.getLimit(tenant), "Limit must remain frozen while damping is active");

        // 3. Wait for damping lock to expire
        Thread.sleep(350);
        assertFalse(adaptive.isDampingActive(tenant), "Damping lock should have expired");

        // 4. Record sustained clean executions -> Additive Increase: 25 + 10 = 35
        adaptive.recordCleanExecution(tenant);
        adaptive.recordCleanExecution(tenant);
        assertEquals(35, adaptive.getLimit(tenant), "Limit should have increased to 35 after 2 clean executions");
        assertEquals(1, adaptive.getUpgradeCount(tenant));

        // 5. Another clean threshold reached -> Additive Increase: 35 + 10 = 45
        adaptive.recordCleanExecution(tenant);
        adaptive.recordCleanExecution(tenant);
        assertEquals(45, adaptive.getLimit(tenant));
        assertEquals(2, adaptive.getUpgradeCount(tenant));

        // 6. Test Static Override pinning
        adaptive.setStaticOverride(tenant, 200);
        assertEquals(200, adaptive.getLimit(tenant));
        assertTrue(adaptive.isStaticOverride(tenant));

        // Throttle should NOT downgrade a pinned static override
        adaptive.recordThrottle(tenant);
        assertEquals(200, adaptive.getLimit(tenant));

        // Clear static override
        adaptive.clearStaticOverride(tenant);
        assertFalse(adaptive.isStaticOverride(tenant));
    }

    @Test
    void testAdaptiveRateLimiterPlugin_WorkerExecutionFlow() throws Exception {
        AdaptiveRateLimiterPlugin adaptive = AdaptiveRateLimiterPlugin.builder()
                .connectionManager(connectionManager)
                .keyPrefix("oxmq:test:adaptive:worker:" + System.currentTimeMillis())
                .groupKeyResolver(job -> (String) job.getData())
                .initialLimit(40)
                .minLimit(10)
                .maxLimit(100)
                .increaseStep(10)
                .backoffFactor(0.5)
                .dampingDuration(Duration.ofMillis(200))
                .cleanThreshold(2)
                .build();

        AtomicInteger attemptCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(2);

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .interceptor(adaptive)
                .processor(job -> {
                    int att = attemptCount.incrementAndGet();
                    if (att == 1) {
                        // First job throws rate limit cooldown, triggering adaptive downgrade
                        throw new RateLimitCooldownException(Duration.ofMillis(100), job.getData(), "429 Throttled");
                    }
                    latch.countDown();
                    return "success";
                })
                .concurrency(1)
                .pollIntervalMs(20)
                .build();

        worker.start();

        // Enqueue 1 failing job with 3 attempts and 1 subsequent job
        queue.add("job-1", "tenant-worker-test", JobOptions.builder().attempts(3).build());
        queue.add("job-2", "tenant-worker-test", JobOptions.builder().attempts(3).build());

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Jobs should complete successfully after retry");
        // Verify that throttle was caught and limit was halved: 40 * 0.5 = 20
        assertTrue(adaptive.getDowngradeCount("tenant-worker-test") >= 1);
        assertEquals(20, adaptive.getLimit("tenant-worker-test"));
    }

    @Test
    void testCheckpointStatePlugin_SaveRetrieveAndResume() throws Exception {
        String namespace = "drives-" + System.currentTimeMillis();
        CheckpointStatePlugin ckptPlugin = CheckpointStatePlugin.builder()
                .connectionManager(connectionManager)
                .namespace(namespace)
                .keyPrefix("oxmq:test:checkpoint:" + System.currentTimeMillis())
                .groupKeyResolver(job -> (String) job.getData())
                .build();

        String driveId = "drive-xyz-987";

        // Initially no checkpoint
        Optional<String> initial = ckptPlugin.getCheckpoint(driveId);
        assertTrue(initial.isEmpty());

        // Save a delta token checkpoint
        ckptPlugin.saveCheckpoint(driveId, "delta-token-v1-initial");

        Optional<String> loaded = ckptPlugin.getCheckpoint(driveId);
        assertTrue(loaded.isPresent());
        assertEquals("delta-token-v1-initial", loaded.get());

        // Update checkpoint to newer delta token
        ckptPlugin.saveCheckpoint(driveId, "delta-token-v2-incremental");
        assertEquals("delta-token-v2-incremental", ckptPlugin.getCheckpoint(driveId).orElse(null));

        // Test declarative commit using CheckpointSupplier
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> observedContextCheckpoint = new AtomicReference<>();

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .interceptor(ckptPlugin)
                .processor(job -> {
                    // CheckpointStatePlugin automatically sets thread-local context before process
                    CheckpointStatePlugin.getCurrentCheckpoint().ifPresent(observedContextCheckpoint::set);
                    // Return result implementing CheckpointSupplier
                    return (CheckpointSupplier) () -> "delta-token-v3-auto-committed";
                })
                .interceptor(new io.oxmq.interceptor.JobInterceptor() {
                    @Override
                    public void afterProcess(io.oxmq.model.Job<?> job, Object result, Duration duration) {
                        // After all interceptors run, count down latch
                        latch.countDown();
                    }
                })
                .concurrency(1)
                .pollIntervalMs(20)
                .build();

        worker.start();

        queue.add("job-ckpt", driveId);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals("delta-token-v2-incremental", observedContextCheckpoint.get(), "Worker should observe previous checkpoint in beforeProcess");

        // Verify that CheckpointStatePlugin auto-committed the new checkpoint from the return value
        assertEquals("delta-token-v3-auto-committed", ckptPlugin.getCheckpoint(driveId).orElse(null));

        // Clear checkpoint
        ckptPlugin.clearCheckpoint(driveId);
        assertTrue(ckptPlugin.getCheckpoint(driveId).isEmpty());
    }
}
