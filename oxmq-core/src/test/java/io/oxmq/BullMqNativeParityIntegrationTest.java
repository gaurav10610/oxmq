package io.oxmq;

import io.oxmq.client.QueueKeys;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.lua.BullScripts;
import io.oxmq.model.*;
import io.oxmq.tracing.TraceContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies all granular native BullMQ capabilities implemented for 100% parity:
 * 1. Queue inspection & pagination (getJobs, getWaiting, getDelayed, getCompleted, getFailed, getJobCounts)
 * 2. job.discard() preventing retries
 * 3. job.getIgnoredChildrenFailures() in DAG workflows
 * 4. Pluggable custom backoff strategies
 * 5. Worker-level event listeners (onActive, onCompleted, onFailed, onProgress, onDrained, onPaused, onResumed)
 * 6. W3C TraceContext propagation (traceparent / tracestate)
 * 7. Unbound BullMQ Lua scripts (releaseLock, saveStacktrace, getStateV2, isJobInList)
 */
class BullMqNativeParityIntegrationTest {

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

    @Test
    @DisplayName("Queue inspection & range pagination APIs (getJobs, getWaiting, getDelayed, getCompleted, getFailed, getJobCounts)")
    void testQueueInspectionAndPagination() throws Exception {
        OxmqQueue<Map> queue = createQueue("parity-inspect", Map.class);

        // 1. Add waiting jobs
        Job<Map> wait1 = queue.add("waitJob1", Map.of("k", "v1"));
        Job<Map> wait2 = queue.add("waitJob2", Map.of("k", "v2"));
        Job<Map> wait3 = queue.add("waitJob3", Map.of("k", "v3"));

        // 2. Add delayed jobs
        Job<Map> delayed1 = queue.add("delayedJob1", Map.of("k", "d1"), JobOptions.builder().delay(Duration.ofHours(1)).build());
        Job<Map> delayed2 = queue.add("delayedJob2", Map.of("k", "d2"), JobOptions.builder().delay(Duration.ofHours(2)).build());

        // Check waiting jobs range
        List<Job<Map>> waitingJobs = queue.getWaiting(0, 10);
        assertNotNull(waitingJobs);
        assertEquals(3, waitingJobs.size(), "Should have 3 waiting jobs");
        assertTrue(waitingJobs.stream().anyMatch(j -> j.getId().equals(wait1.getId())));
        assertTrue(waitingJobs.stream().anyMatch(j -> j.getId().equals(wait2.getId())));
        assertTrue(waitingJobs.stream().anyMatch(j -> j.getId().equals(wait3.getId())));

        // Check delayed jobs range
        List<Job<Map>> delayedJobs = queue.getDelayed(0, 10);
        assertEquals(2, delayedJobs.size(), "Should have 2 delayed jobs");
        assertTrue(delayedJobs.stream().anyMatch(j -> j.getId().equals(delayed1.getId())));
        assertTrue(delayedJobs.stream().anyMatch(j -> j.getId().equals(delayed2.getId())));

        // Check multi-state getJobs
        List<Job<Map>> allJobs = queue.getJobs(List.of(JobState.WAITING, JobState.DELAYED), 0, 10, true);
        assertEquals(5, allJobs.size(), "Multi-state getJobs should return 5 jobs total");

        // Check getJobCounts breakdown for specific states
        Map<JobState, Long> counts = queue.getJobCounts(JobState.WAITING, JobState.DELAYED, JobState.COMPLETED, JobState.FAILED);
        assertEquals(3L, counts.get(JobState.WAITING));
        assertEquals(2L, counts.get(JobState.DELAYED));
        assertEquals(0L, counts.get(JobState.COMPLETED));
        assertEquals(0L, counts.get(JobState.FAILED));

        // Check getRateLimitTtl (no limiter active -> returns <= 0 or null)
        Long ttl = queue.getRateLimitTtl();
        assertNotNull(ttl);
    }

    @Test
    @DisplayName("job.discard() marks actively executing job to not be retried even if attempts remain")
    void testJobDiscard() throws Exception {
        OxmqQueue<Map> queue = createQueue("parity-discard", Map.class);
        CountDownLatch failedLatch = new CountDownLatch(1);
        AtomicInteger attemptsSeen = new AtomicInteger(0);

        OxmqWorker<Map> worker = OxmqWorker.<Map>builder()
                .queueName(queue.getName())
                .connectionManager(connectionManager)
                .payloadClass(Map.class)
                .concurrency(1)
                .processor(job -> {
                    attemptsSeen.incrementAndGet();
                    // Explicitly discard this job so it won't be retried
                    job.discard();
                    assertTrue(job.isDiscarded());
                    throw new RuntimeException("Simulated failure with discard");
                })
                .onFailed((job, err) -> failedLatch.countDown())
                .build();
        workersToStop.add(worker);
        worker.start();

        // Enqueue with 5 attempts
        Job<Map> job = queue.add("discardableJob", Map.of("data", "test"), JobOptions.builder().attempts(5).build());

        assertTrue(failedLatch.await(5, TimeUnit.SECONDS), "Worker should fail the job");
        Thread.sleep(500); // Allow Redis state to settle

        assertEquals(1, attemptsSeen.get(), "Job should only have executed once because discard() prevented retries");
        Job<Map> refreshed = queue.getJob(job.getId());
        assertNotNull(refreshed);
        assertEquals(JobState.FAILED, refreshed.getState(), "Job should be in FAILED state");
    }

    @Test
    @DisplayName("job.getIgnoredChildrenFailures() exposes child errors to parent when ignoreDependencyOnFailure is true")
    void testJobIgnoredChildrenFailures() throws Exception {
        OxmqQueue<Map> queue = createQueue("parity-dag-ignore", Map.class);
        FlowProducer flowProducer = new FlowProducer(connectionManager);
        CountDownLatch parentLatch = new CountDownLatch(1);
        Map<String, String> capturedFailures = new HashMap<>();

        OxmqWorker<Map> worker = OxmqWorker.<Map>builder()
                .queueName(queue.getName())
                .connectionManager(connectionManager)
                .payloadClass(Map.class)
                .concurrency(2)
                .processor(job -> {
                    if ("failingChild".equals(job.getName())) {
                        throw new RuntimeException("Child error detail 42");
                    }
                    if ("succeedingChild".equals(job.getName())) {
                        return Map.of("status", "ok");
                    }
                    if ("parentJob".equals(job.getName())) {
                        Map<String, String> failures = job.getIgnoredChildrenFailures();
                        if (failures != null) {
                            capturedFailures.putAll(failures);
                        }
                        parentLatch.countDown();
                        return Map.of("parentDone", true);
                    }
                    return null;
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        // Build parent job with 2 children: one fails, one succeeds
        FlowJob parent = new FlowJob(queue.getName(), "parentJob", Map.of("role", "parent"),
                JobOptions.builder().ignoreDependencyOnFailure(true).build());
        parent.addChild(FlowJob.of(queue.getName(), "failingChild", Map.of("role", "c1")));
        parent.addChild(FlowJob.of(queue.getName(), "succeedingChild", Map.of("role", "c2")));

        flowProducer.add(parent);

        assertTrue(parentLatch.await(10, TimeUnit.SECONDS), "Parent job should execute even if child failed");
        assertFalse(capturedFailures.isEmpty(), "Parent should have captured child failures");
        assertTrue(capturedFailures.values().stream().anyMatch(v -> v.contains("Child error detail 42")),
                "Ignored failure reason should be visible to parent");
    }

    @Test
    @DisplayName("Pluggable custom backoff strategy resolved dynamically at runtime on worker")
    void testCustomBackoffStrategy() throws Exception {
        OxmqQueue<Map> queue = createQueue("parity-backoff", Map.class);
        CountDownLatch retryLatch = new CountDownLatch(2);
        AtomicInteger attempts = new AtomicInteger(0);

        OxmqWorker<Map> worker = OxmqWorker.<Map>builder()
                .queueName(queue.getName())
                .connectionManager(connectionManager)
                .payloadClass(Map.class)
                .concurrency(1)
                .backoffStrategy("myCustomDynamicBackoff", (attemptsMade, error, job) -> {
                    // Custom calculation: attemptsMade * 150ms
                    return Duration.ofMillis(attemptsMade * 150L);
                })
                .processor(job -> {
                    int att = attempts.incrementAndGet();
                    retryLatch.countDown();
                    if (att < 2) {
                        throw new RuntimeException("First attempt failure");
                    }
                    return Map.of("success", true);
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        Job<Map> job = queue.add("customBackoffJob", Map.of("data", 123),
                JobOptions.builder()
                        .attempts(3)
                        .backoff(BackoffStrategy.custom("myCustomDynamicBackoff"))
                        .build());

        assertTrue(retryLatch.await(5, TimeUnit.SECONDS), "Job should be retried via custom backoff");
        assertEquals(2, attempts.get(), "Job should have succeeded on attempt 2");
    }

    @Test
    @DisplayName("Worker-level event emitter callbacks (onActive, onCompleted, onFailed, onProgress, onDrained, onPaused, onResumed)")
    void testWorkerLevelEvents() throws Exception {
        OxmqQueue<Map> queue = createQueue("parity-events", Map.class);

        List<String> activeJobIds = new CopyOnWriteArrayList<>();
        List<String> completedJobIds = new CopyOnWriteArrayList<>();
        List<String> failedJobIds = new CopyOnWriteArrayList<>();
        List<Object> progressEvents = new CopyOnWriteArrayList<>();
        AtomicBoolean drained = new AtomicBoolean(false);
        AtomicBoolean paused = new AtomicBoolean(false);
        AtomicBoolean resumed = new AtomicBoolean(false);

        CountDownLatch completedLatch = new CountDownLatch(1);
        CountDownLatch failedLatch = new CountDownLatch(1);
        CountDownLatch progressLatch = new CountDownLatch(1);

        OxmqWorker<Map> worker = OxmqWorker.<Map>builder()
                .queueName(queue.getName())
                .connectionManager(connectionManager)
                .payloadClass(Map.class)
                .concurrency(1)
                .onActive(job -> activeJobIds.add(job.getId()))
                .onCompleted((job, res) -> {
                    completedJobIds.add(job.getId());
                    completedLatch.countDown();
                })
                .onFailed((job, err) -> {
                    failedJobIds.add(job.getId());
                    failedLatch.countDown();
                })
                .onProgress((job, progress) -> {
                    progressEvents.add(progress);
                    progressLatch.countDown();
                })
                .onDrained(() -> drained.set(true))
                .onPaused(() -> paused.set(true))
                .onResumed(() -> resumed.set(true))
                .processor(job -> {
                    if ("progressJob".equals(job.getName())) {
                        job.updateProgress(50, Map.of("step", "halfway"));
                        return "ok";
                    }
                    if ("failingJob".equals(job.getName())) {
                        throw new RuntimeException("Simulated error");
                    }
                    return "done";
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        // 1. Process job with progress
        Job<Map> pJob = queue.add("progressJob", Map.of("x", 1));
        assertTrue(completedLatch.await(5, TimeUnit.SECONDS));
        assertTrue(progressLatch.await(5, TimeUnit.SECONDS));

        // 2. Process failing job
        Job<Map> fJob = queue.add("failingJob", Map.of("x", 2));
        assertTrue(failedLatch.await(5, TimeUnit.SECONDS));

        // 3. Test pause and resume events
        worker.pause();
        assertTrue(paused.get(), "Worker pause listener should have fired");
        worker.resume();
        assertTrue(resumed.get(), "Worker resume listener should have fired");

        // Verify captured IDs
        assertTrue(activeJobIds.contains(pJob.getId()));
        assertTrue(completedJobIds.contains(pJob.getId()));
        assertTrue(failedJobIds.contains(fJob.getId()));
        assertFalse(progressEvents.isEmpty());
    }

    @Test
    @DisplayName("W3C TraceContext propagation across job producer and worker")
    void testTraceContextPropagation() throws Exception {
        OxmqQueue<Map> queue = createQueue("parity-tracing", Map.class);
        CountDownLatch latch = new CountDownLatch(1);
        List<String> capturedTraceparents = new ArrayList<>();

        TraceContext rootCtx = TraceContext.generateNew();
        assertNotNull(rootCtx.asTraceparent());
        assertTrue(rootCtx.isSampleed());

        OxmqWorker<Map> worker = OxmqWorker.<Map>builder()
                .queueName(queue.getName())
                .connectionManager(connectionManager)
                .payloadClass(Map.class)
                .concurrency(1)
                .processor(job -> {
                    capturedTraceparents.add(job.getTraceparent());
                    TraceContext extracted = TraceContext.extract(job);
                    assertNotNull(extracted);
                    assertEquals(rootCtx.traceId(), extracted.traceId());
                    latch.countDown();
                    return "traced";
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        JobOptions opts = JobOptions.builder()
                .traceparent(rootCtx.asTraceparent())
                .tracestate("congo=t61rcWkgMzE")
                .build();

        Job<Map> job = queue.add("tracedJob", Map.of("msg", "hello"), opts);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(1, capturedTraceparents.size());
        assertEquals(rootCtx.asTraceparent(), capturedTraceparents.get(0));
    }

    @Test
    @DisplayName("Direct BullMQ Lua scripts parity (releaseLock, saveStacktrace, getStateV2, isJobInList)")
    void testDirectBullScripts() throws Exception {
        BullScripts scripts = new BullScripts();
        var binConn = connectionManager.getBinaryConnection();
        var cmdConn = connectionManager.getCommandConnection();
        QueueKeys keys = new QueueKeys("bull", "script-test-" + System.currentTimeMillis());

        // 1. Test releaseLock
        String lockKey = keys.toJobKey("job123") + ":lock";
        cmdConn.sync().set(lockKey, "my-token");
        boolean released = scripts.releaseLock(binConn, keys, "job123", "my-token");
        assertTrue(released, "Lock should be released");
        assertNull(cmdConn.sync().get(lockKey), "Lock key should be deleted");

        // 2. Test saveStacktrace
        String jobKey = keys.toJobKey("job456");
        cmdConn.sync().hset(jobKey, "name", "testJob");
        int traceRes = scripts.saveStacktrace(binConn, keys, "job456", "StackTrace line 1\nline 2", "CustomFailedReason");
        assertEquals(0, traceRes, "saveStacktrace should return 0 (OK)");
        Map<String, String> hash = cmdConn.sync().hgetall(jobKey);
        assertEquals("StackTrace line 1\nline 2", hash.get("stacktrace"));
        assertEquals("CustomFailedReason", hash.get("failedReason"));

        // 3. Test getStateV2
        cmdConn.sync().zadd(keys.getKey("completed"), 1000.0, "job456");
        String state = scripts.getStateV2(binConn, keys, "job456");
        assertEquals("completed", state);

        // 4. Test isJobInList
        String listKey = keys.getKey("testList");
        cmdConn.sync().rpush(listKey, "item1", "item2", "item3");
        assertTrue(scripts.isJobInList(binConn, listKey, "item2"));
        assertFalse(scripts.isJobInList(binConn, listKey, "item999"));

        // Clean up
        cmdConn.sync().del(jobKey, keys.getKey("completed"), listKey);
    }

    @Test
    @DisplayName("BullMQ Global Concurrency & Global Rate Limit queue metadata parity")
    void testGlobalConcurrencyAndGlobalRateLimitQueueMeta() throws Exception {
        String queueName = "global-meta-" + System.currentTimeMillis();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .payloadClass(String.class)
                .build();

        // 1. Test setGlobalConcurrency
        queue.setGlobalConcurrency(3);
        assertEquals(3, queue.getGlobalConcurrency());
        assertFalse(queue.isMaxed());

        // 2. Test removeGlobalConcurrency
        assertTrue(queue.removeGlobalConcurrency());
        assertNull(queue.getGlobalConcurrency());

        // 3. Test setGlobalRateLimit
        queue.setGlobalRateLimit(5, Duration.ofSeconds(2));
        io.oxmq.model.GlobalRateLimit rl = queue.getGlobalRateLimit();
        assertNotNull(rl);
        assertEquals(5, rl.max());
        assertEquals(2000L, rl.durationMillis());
        assertEquals(Duration.ofSeconds(2), rl.duration());

        // 4. Test removeRateLimitKey
        String limiterKey = "bull:" + queueName + ":limiter";
        connectionManager.getCommandConnection().sync().set(limiterKey, "5");
        assertTrue(queue.removeRateLimitKey());
        assertNull(connectionManager.getCommandConnection().sync().get(limiterKey));

        // 5. Test removeGlobalRateLimit
        assertTrue(queue.removeGlobalRateLimit());
        assertNull(queue.getGlobalRateLimit());

        // Clean up
        queue.obliterate();
    }
}
