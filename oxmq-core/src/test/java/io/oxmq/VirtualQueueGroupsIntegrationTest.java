package io.oxmq;

import io.oxmq.client.RedisConnectionManager;
import io.oxmq.exception.GroupMaxSizeExceededException;
import io.oxmq.lua.ExtensionScripts;
import io.oxmq.model.GroupOptions;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Virtual Queue Groups & Fair Multi-Tenant Scheduling Integration Tests")
class VirtualQueueGroupsIntegrationTest {

    private String queueName;
    private RedisConnectionManager connectionManager;
    private OxmqQueue<String> queue;
    private OxmqWorker<String> worker;
    private OxmqBatchWorker<String> batchWorker;

    @BeforeEach
    void setUp() {
        queueName = "groups-queue-" + UUID.randomUUID();
        connectionManager = new RedisConnectionManager("redis://localhost:6379");
        queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
    }

    @AfterEach
    void tearDown() {
        if (worker != null) {
            try { worker.close(); } catch (Exception ignored) {}
        }
        if (batchWorker != null) {
            try { batchWorker.close(); } catch (Exception ignored) {}
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
    @DisplayName("Fair round-robin scheduling interleaves jobs across virtual groups without starvation")
    void testFairRoundRobinSchedulingInterleavesTenants() throws Exception {
        int tenantAJobs = 10;
        int tenantBJobs = 5;
        int totalJobs = tenantAJobs + tenantBJobs;

        List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(totalJobs);

        for (int i = 0; i < tenantAJobs; i++) {
            queue.add("job-A-" + i, "data-A-" + i, JobOptions.builder().group("tenant-A").build());
        }
        for (int i = 0; i < tenantBJobs; i++) {
            queue.add("job-B-" + i, "data-B-" + i, JobOptions.builder().group("tenant-B").build());
        }

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(1)
                .processor(job -> {
                    executionOrder.add(job.getGroupKey());
                    latch.countDown();
                    return "ok";
                })
                .build();

        worker.start();
        assertTrue(latch.await(10, TimeUnit.SECONDS), "All jobs should complete within 10s");

        assertEquals(totalJobs, executionOrder.size());

        // Verify interleaving: Tenant B jobs should not wait until all Tenant A jobs are done
        int firstTenantBIndex = executionOrder.indexOf("tenant-B");
        assertTrue(firstTenantBIndex < 3, "Tenant B should be processed early via round-robin, was at index: " + firstTenantBIndex);

        // Verify fair rotation (no group dominates completely)
        boolean hasAlternation = false;
        for (int i = 0; i < executionOrder.size() - 1; i++) {
            if (!executionOrder.get(i).equals(executionOrder.get(i + 1))) {
                hasAlternation = true;
                break;
            }
        }
        assertTrue(hasAlternation, "Jobs from different tenants must alternate fairly");
    }

    @Test
    @DisplayName("Per-group concurrency cap restricts active jobs per tenant even under high overall concurrency")
    void testGroupConcurrencyLimitsSimultaneousTenantJobs() throws Exception {
        int totalTenantJobs = 8;
        int maxGroupConcurrency = 2;
        AtomicInteger activeConcurrentForTenant = new AtomicInteger(0);
        AtomicInteger maxObservedConcurrent = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(totalTenantJobs);

        for (int i = 0; i < totalTenantJobs; i++) {
            queue.add("job-tenant-C-" + i, "payload", JobOptions.builder().group("tenant-C").build());
        }

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(10) // Overall worker can take up to 10 jobs
                .groupConcurrency(maxGroupConcurrency) // But tenant-C can run at most 2 at a time!
                .processor(job -> {
                    int current = activeConcurrentForTenant.incrementAndGet();
                    maxObservedConcurrent.accumulateAndGet(current, Math::max);
                    try {
                        Thread.sleep(150); // Simulate processing time
                    } finally {
                        activeConcurrentForTenant.decrementAndGet();
                        latch.countDown();
                    }
                    return "ok";
                })
                .build();

        worker.start();
        assertTrue(latch.await(10, TimeUnit.SECONDS), "All jobs should finish within 10s");

        assertEquals(totalTenantJobs, latch.getCount() == 0 ? totalTenantJobs : 0);
        assertTrue(maxObservedConcurrent.get() <= maxGroupConcurrency,
                "Observed simultaneous jobs (" + maxObservedConcurrent.get() + ") must not exceed groupConcurrency (" + maxGroupConcurrency + ")");
    }

    @Test
    @DisplayName("Group maxSize bounds backlog and throws GroupMaxSizeExceededException when exceeded")
    void testGroupMaxSizeEnforcementThrowsException() {
        GroupOptions boundedGroup = GroupOptions.builder()
                .id("tenant-bounded")
                .maxSize(3)
                .build();

        // Adding up to maxSize (3) should succeed
        assertDoesNotThrow(() -> queue.add("job-1", "d1", JobOptions.builder().group(boundedGroup).build()));
        assertDoesNotThrow(() -> queue.add("job-2", "d2", JobOptions.builder().group(boundedGroup).build()));
        assertDoesNotThrow(() -> queue.add("job-3", "d3", JobOptions.builder().group(boundedGroup).build()));

        // Adding 4th job must fail with GroupMaxSizeExceededException
        GroupMaxSizeExceededException ex = assertThrows(GroupMaxSizeExceededException.class, () -> {
            queue.add("job-4", "d4", JobOptions.builder().group(boundedGroup).build());
        });

        assertEquals("tenant-bounded", ex.getGroupId());
        assertEquals(3, ex.getMaxSize());
    }

    @Test
    @DisplayName("Dynamic rate limiting throttles a specific virtual group across workers")
    void testGroupRateLimitingThrottlesTenant() throws Exception {
        CountDownLatch tenantELatch = new CountDownLatch(2);
        CountDownLatch tenantDLatch = new CountDownLatch(2);

        for (int i = 0; i < 2; i++) {
            queue.add("job-D-" + i, "d", JobOptions.builder().group("tenant-D").build());
            queue.add("job-E-" + i, "e", JobOptions.builder().group("tenant-E").build());
        }

        // Apply 800ms rate limit to tenant-D
        queue.setGroupRateLimit("tenant-D", Duration.ofMillis(800));

        List<String> completionOrder = Collections.synchronizedList(new ArrayList<>());

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(2)
                .processor(job -> {
                    completionOrder.add(job.getGroupKey());
                    if ("tenant-E".equals(job.getGroupKey())) {
                        tenantELatch.countDown();
                    } else if ("tenant-D".equals(job.getGroupKey())) {
                        tenantDLatch.countDown();
                    }
                    return "done";
                })
                .build();

        worker.start();

        // Tenant-E should complete immediately (< 1s)
        assertTrue(tenantELatch.await(1500, TimeUnit.MILLISECONDS), "Tenant E should complete without delay");
        assertEquals(2, tenantDLatch.getCount(), "Tenant D must still be throttled and incomplete");

        // Tenant-D should complete after rate limit expires
        assertTrue(tenantDLatch.await(5000, TimeUnit.MILLISECONDS), "Tenant D should complete after rate limit expires");
    }

    @Test
    @DisplayName("Pausing and resuming a virtual group halts and continues only that tenant")
    void testPauseAndResumeGroup() throws Exception {
        CountDownLatch activeLatch = new CountDownLatch(2);
        CountDownLatch pausedLatch = new CountDownLatch(2);

        // Pause tenant-P
        queue.pauseGroup("tenant-P");

        for (int i = 0; i < 2; i++) {
            queue.add("job-P-" + i, "p", JobOptions.builder().group("tenant-P").build());
            queue.add("job-Q-" + i, "q", JobOptions.builder().group("tenant-Q").build());
        }

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(2)
                .processor(job -> {
                    if ("tenant-Q".equals(job.getGroupKey())) {
                        activeLatch.countDown();
                    } else if ("tenant-P".equals(job.getGroupKey())) {
                        pausedLatch.countDown();
                    }
                    return "ok";
                })
                .build();

        worker.start();

        assertTrue(activeLatch.await(2, TimeUnit.SECONDS), "Tenant Q jobs must process immediately");
        assertEquals(2, pausedLatch.getCount(), "Tenant P jobs must not process while paused");

        // Resume tenant-P
        queue.resumeGroup("tenant-P");

        assertTrue(pausedLatch.await(3, TimeUnit.SECONDS), "Tenant P jobs must resume and process after unpausing");
    }

    @Test
    @DisplayName("Deleting a virtual group purges all its waiting and prioritized jobs")
    void testDeleteGroupPurgesJobsAndMetadata() {
        for (int i = 0; i < 5; i++) {
            queue.add("job-del-" + i, "del", JobOptions.builder().group("tenant-delete").build());
        }

        assertEquals(5, queue.getGroupJobsCount("tenant-delete"));

        long deletedCount = queue.deleteGroup("tenant-delete");
        assertEquals(5, deletedCount);

        assertEquals(0, queue.getGroupJobsCount("tenant-delete"));
    }

    @Test
    @DisplayName("OxmqBatchWorker with groupAffinity(true) delivers strictly homogeneous tenant batches")
    void testBatchWorkerWithGroupAffinity() throws Exception {
        int jobsPerTenant = 4;
        for (int i = 0; i < jobsPerTenant; i++) {
            queue.add("job-T1-" + i, "t1", JobOptions.builder().group("tenant-batch-1").build());
            queue.add("job-T2-" + i, "t2", JobOptions.builder().group("tenant-batch-2").build());
        }

        List<List<String>> observedBatches = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(jobsPerTenant * 2);

        batchWorker = OxmqBatchWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .batchSize(4)
                .groupAffinity(true) // homogeneous batches!
                .concurrency(2)
                .processor(jobs -> {
                    List<String> groupsInBatch = new ArrayList<>();
                    for (Job<String> job : jobs) {
                        groupsInBatch.add(job.getGroupKey());
                        latch.countDown();
                    }
                    observedBatches.add(groupsInBatch);
                    return "batch-ok";
                })
                .build();

        batchWorker.start();

        assertTrue(latch.await(10, TimeUnit.SECONDS), "All batch jobs should be processed");

        // Verify that every single batch contains jobs from only ONE group!
        for (List<String> batch : observedBatches) {
            assertFalse(batch.isEmpty());
            String expectedGroup = batch.get(0);
            for (String group : batch) {
                assertEquals(expectedGroup, group, "Every batch must be homogeneous with groupAffinity enabled!");
            }
        }
    }

    @Test
    @DisplayName("Group status inspection and paginated job queries operate seamlessly")
    void testGetGroupsByStatusAndGroupJobs() {
        queue.add("inspect-1", "d1", JobOptions.builder().group("group-alpha").build());
        queue.add("inspect-2", "d2", JobOptions.builder().group("group-beta").build());

        List<ExtensionScripts.GroupEntry> waitingGroups = queue.getGroupsByStatus("waiting", 0, 10);
        assertNotNull(waitingGroups);
        assertTrue(waitingGroups.size() >= 2);

        boolean foundAlpha = waitingGroups.stream().anyMatch(g -> "group-alpha".equals(g.id()));
        boolean foundBeta = waitingGroups.stream().anyMatch(g -> "group-beta".equals(g.id()));
        assertTrue(foundAlpha, "group-alpha should be in waiting groups list");
        assertTrue(foundBeta, "group-beta should be in waiting groups list");

        List<Job<String>> alphaJobs = queue.getGroupJobs("group-alpha", 0, 10);
        assertEquals(1, alphaJobs.size());
        assertEquals("inspect-1", alphaJobs.get(0).getName());
        assertEquals("group-alpha", alphaJobs.get(0).getGroupKey());
    }

    @Test
    @DisplayName("Intra-group priority ordering and priority count queries")
    void testIntraGroupPriorityOrderingAndPriorityCounts() throws Exception {
        String tenant = "prio-tenant";
        // 1. Enqueue lower-priority first, then VIP priority, then FIFO
        queue.add("job-low", "d-low", JobOptions.builder().group(GroupOptions.builder().id(tenant).priority(10).build()).build());
        queue.add("job-vip", "d-vip", JobOptions.builder().group(GroupOptions.builder().id(tenant).priority(1).build()).build());
        queue.add("job-fifo", "d-fifo", JobOptions.builder().group(GroupOptions.builder().id(tenant).priority(0).build()).build());

        // 2. Query priority counts
        Map<Integer, Long> counts = queue.getCountsPerPriorityForGroup(tenant, List.of(0, 1, 10));
        assertNotNull(counts);
        assertEquals(1L, counts.get(1), "Priority 1 should have 1 job");
        assertEquals(1L, counts.get(10), "Priority 10 should have 1 job");
        assertEquals(1L, counts.get(0), "Priority 0 should have 1 job");

        // 3. Worker dequeues jobs; VIP (priority 1) must be processed before priority 10, then FIFO
        List<String> order = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(3);

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(1)
                .processor(job -> {
                    order.add(job.getName());
                    latch.countDown();
                    return "ok";
                })
                .build();

        worker.start();
        assertTrue(latch.await(5, TimeUnit.SECONDS), "All jobs should finish");

        assertEquals(List.of("job-vip", "job-low", "job-fifo"), order,
                "Intra-group priority: VIP (1) must execute first, then 10, then FIFO (0)");
    }

    @Test
    @DisplayName("Dynamic per-group concurrency overrides in Redis")
    void testDynamicPerGroupConcurrencyOverrides() throws Exception {
        queue.setGroupConcurrency("vip-tenant", 3);
        assertEquals(3, queue.getGroupConcurrency("vip-tenant"));

        AtomicInteger activeVip = new AtomicInteger();
        AtomicInteger maxObservedVip = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(3);

        for (int i = 0; i < 3; i++) {
            queue.add("vip-job-" + i, "d", JobOptions.builder().group("vip-tenant").build());
        }

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(10)
                .groupConcurrency(1) // Default is 1, but vip-tenant has override of 3!
                .processor(job -> {
                    int current = activeVip.incrementAndGet();
                    maxObservedVip.accumulateAndGet(current, Math::max);
                    try {
                        Thread.sleep(150);
                    } catch (InterruptedException ignored) {}
                    activeVip.decrementAndGet();
                    latch.countDown();
                    return "ok";
                })
                .build();

        worker.start();
        assertTrue(latch.await(5, TimeUnit.SECONDS), "All VIP jobs should execute");
        assertTrue(maxObservedVip.get() >= 2, "VIP tenant should achieve concurrency > default (1), observed: " + maxObservedVip.get());

        // Test removal of override
        assertTrue(queue.removeGroupConcurrency("vip-tenant"));
        assertNull(queue.getGroupConcurrency("vip-tenant"));
    }
}
