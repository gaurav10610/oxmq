package io.oxmq.plugin;

import io.oxmq.OxmqQueue;
import io.oxmq.OxmqWorker;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.exception.RateLimitCooldownException;
import io.oxmq.model.JobOptions;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class GroupRateLimiterAndSentinelPluginsIntegrationTest {

    private RedisConnectionManager connectionManager;
    private String queueName;
    private OxmqQueue<String> queue;
    private OxmqWorker<String> worker;

    @BeforeEach
    void setUp() {
        connectionManager = new RedisConnectionManager("redis://localhost:6379");
        queueName = "test-plugins-" + System.currentTimeMillis();
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
    void testGroupRateLimiterPlugin_LimitsPerTenant() throws Exception {
        // Tenant A has limit 2/sec, Tenant B has limit 10/sec
        GroupRateLimiterPlugin limiter = GroupRateLimiterPlugin.builder()
                .connectionManager(connectionManager)
                .keyPrefix("oxmq:test:limiter:" + System.currentTimeMillis())
                .groupKeyResolver(job -> (String) job.getData()) // Payload is the tenantId
                .dynamicLimit(tenantId -> "tenant-A".equals(tenantId) ? 2 : 10)
                .windowDuration(Duration.ofSeconds(1))
                .retryDelayMs(100)
                .build();

        AtomicInteger processedTenantA = new AtomicInteger(0);
        AtomicInteger processedTenantB = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(5);

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .interceptor(limiter)
                .processor(job -> {
                    String tenant = job.getData();
                    if ("tenant-A".equals(tenant)) {
                        processedTenantA.incrementAndGet();
                    } else {
                        processedTenantB.incrementAndGet();
                    }
                    latch.countDown();
                    return "done";
                })
                .concurrency(5)
                .pollIntervalMs(20)
                .build();

        worker.start();

        // Enqueue 3 jobs for tenant-A and 2 jobs for tenant-B
        queue.add("job-a1", "tenant-A");
        queue.add("job-a2", "tenant-A");
        queue.add("job-a3", "tenant-A"); // Exceeds limit 2 in first second
        queue.add("job-b1", "tenant-B");
        queue.add("job-b2", "tenant-B");

        assertTrue(latch.await(5, TimeUnit.SECONDS), "All 5 jobs should eventually complete across time windows");
        assertEquals(3, processedTenantA.get());
        assertEquals(2, processedTenantB.get());
        assertTrue(limiter.getYieldCount("tenant-A") >= 1, "Tenant A should have had at least 1 rate-limit yield");
        assertEquals(0, limiter.getYieldCount("tenant-B"), "Tenant B should have had 0 rate-limit yields");
    }

    @Test
    void testRateLimitSentinelPlugin_External429CooldownIsolation() throws Exception {
        RateLimitSentinelPlugin sentinel = RateLimitSentinelPlugin.builder()
                .connectionManager(connectionManager)
                .keyPrefix("oxmq:test:sentinel:" + System.currentTimeMillis())
                .groupKeyResolver(job -> (String) job.getData())
                .build();

        AtomicInteger attemptsTenantA = new AtomicInteger(0);
        CountDownLatch latchB = new CountDownLatch(1);
        CountDownLatch latchA = new CountDownLatch(1);

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .interceptor(sentinel)
                .processor(job -> {
                    String tenant = job.getData();
                    if ("tenant-fail".equals(tenant)) {
                        int att = attemptsTenantA.incrementAndGet();
                        if (att == 1) {
                            // First time: simulate HTTP 429 with 500ms cooldown
                            throw new RateLimitCooldownException(Duration.ofMillis(500), "tenant-fail", "HTTP 429 Retry-After: 0.5s");
                        }
                        latchA.countDown();
                        return "tenant-fail-recovered";
                    } else {
                        latchB.countDown();
                        return "tenant-ok-done";
                    }
                })
                .concurrency(5)
                .pollIntervalMs(20)
                .build();

        worker.start();

        // Add 1 job for tenant-fail and 1 for tenant-ok
        queue.add("job-fail-1", "tenant-fail");
        queue.add("job-ok-1", "tenant-ok");

        // Tenant-ok should finish immediately
        assertTrue(latchB.await(2, TimeUnit.SECONDS), "Tenant OK should succeed immediately without being throttled");
        assertFalse(sentinel.isCooldownActive("tenant-ok"), "Tenant OK should NOT be in cooldown");

        // Meanwhile, tenant-fail is in cooldown
        assertTrue(sentinel.isCooldownActive("tenant-fail"), "Tenant fail should be in active cooldown");
        assertTrue(latchA.await(5, TimeUnit.SECONDS), "Tenant fail should recover and succeed after cooldown expires");
        assertEquals(1, sentinel.getThrottleCount("tenant-fail"), "Tenant fail should record 1 throttle");
    }

    @Test
    void testPriorityQuotaGovernorPlugin_YieldsP10ToProtectP1() throws Exception {
        GroupRateLimiterPlugin limiter = GroupRateLimiterPlugin.builder()
                .connectionManager(connectionManager)
                .keyPrefix("oxmq:test:governor:limiter:" + System.currentTimeMillis())
                .groupKeyResolver(job -> (String) job.getData())
                .staticLimit(10) // 10 req/sec
                .build();

        PriorityQuotaGovernorPlugin governor = PriorityQuotaGovernorPlugin.builder()
                .rateLimiter(limiter)
                .groupKeyResolver(job -> (String) job.getData())
                .headroomYieldThreshold(0.30) // Yield when headroom < 30%
                .priorityThreshold(1)         // P1 is protected, P10 yields
                .yieldDelayMs(50)
                .build();

        AtomicInteger executedP1 = new AtomicInteger(0);
        AtomicInteger executedP10 = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(2);

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .interceptor(limiter)
                .interceptor(governor)
                .processor(job -> {
                    if (job.getOpts() != null && job.getOpts().getPriority() == 1) {
                        executedP1.incrementAndGet();
                    } else {
                        executedP10.incrementAndGet();
                    }
                    latch.countDown();
                    return "done";
                })
                .concurrency(2)
                .pollIntervalMs(20)
                .build();

        worker.start();

        // Enqueue P1 and P10
        queue.add("job-p10", "tenant-shared", JobOptions.builder().priority(10).build());
        queue.add("job-p1", "tenant-shared", JobOptions.builder().priority(1).build());

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Both jobs should finish");
        assertEquals(1, executedP1.get());
        assertEquals(1, executedP10.get());
    }
}
