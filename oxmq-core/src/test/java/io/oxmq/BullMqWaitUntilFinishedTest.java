package io.oxmq;

import io.oxmq.client.RedisConnectionManager;
import io.oxmq.model.FlowJob;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import io.oxmq.model.KeepJobs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class BullMqWaitUntilFinishedTest {

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
        try {
            connectionManager.close();
        } catch (Exception ignored) {}
    }

    @Test
    @DisplayName("waitUntilFinished blocks until worker completes job and returns result")
    void testWaitUntilFinishedSuccess() throws Exception {
        String queueName = "test-wait-finished-" + System.currentTimeMillis();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .payloadClass(String.class)
                .build();
        queuesToClean.add(queue);

        QueueEvents events = QueueEvents.builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .build()
                .start();
        eventsToClose.add(events);

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .payloadClass(String.class)
                .processor(job -> "COMPUTED:" + job.getData())
                .build();
        workersToStop.add(worker);
        worker.start();

        Job<String> job = queue.add("computeJob", "sample-42");
        assertNotNull(job.getId());

        Object result = job.waitUntilFinished(events, Duration.ofSeconds(5));
        assertNotNull(result);
        assertEquals("COMPUTED:sample-42", result.toString().trim().replace("\"", ""));

        assertTrue(queue.isCompleted(job.getId()));
        assertFalse(queue.isFailed(job.getId()));
    }

    @Test
    @DisplayName("waitUntilFinished propagates failure exception when worker job fails")
    void testWaitUntilFinishedFailure() {
        String queueName = "test-wait-failed-" + System.currentTimeMillis();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .payloadClass(String.class)
                .build();
        queuesToClean.add(queue);

        QueueEvents events = QueueEvents.builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .build()
                .start();
        eventsToClose.add(events);

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .payloadClass(String.class)
                .processor(job -> {
                    throw new RuntimeException("Payment card expired");
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        Job<String> job = queue.add("failJob", "order-99", JobOptions.builder().attempts(1).build());

        Exception ex = assertThrows(Exception.class, () -> job.waitUntilFinished(events, Duration.ofSeconds(5)));
        assertTrue(ex.getMessage().contains("Payment card expired"));

        assertTrue(queue.isFailed(job.getId()));
        assertFalse(queue.isCompleted(job.getId()));
    }

    @Test
    @DisplayName("waitUntilFinished returns immediately when job is already completed (via isFinished Lua)")
    void testWaitUntilFinishedAlreadyFinished() throws Exception {
        String queueName = "test-wait-already-" + System.currentTimeMillis();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .payloadClass(String.class)
                .build();
        queuesToClean.add(queue);

        CountDownLatch processedLatch = new CountDownLatch(1);
        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .payloadClass(String.class)
                .processor(job -> {
                    processedLatch.countDown();
                    return "INSTANT_RESULT";
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        Job<String> job = queue.add("instantJob", "payload-1");
        assertTrue(processedLatch.await(5, TimeUnit.SECONDS));

        // Sleep briefly to ensure moveToFinished completes in Redis
        Thread.sleep(200);

        // Call waitUntilFinished on queue without active QueueEvents stream; isFinished-3.lua resolves it instantly
        Object result = queue.waitUntilFinished(job.getId(), Duration.ofSeconds(2));
        assertNotNull(result);
        assertEquals("INSTANT_RESULT", result.toString().trim().replace("\"", ""));

        Job<String> fetchedJob = queue.getJob(job.getId());
        assertNotNull(fetchedJob);
        assertEquals(JobState.COMPLETED, fetchedJob.getState());
        assertTrue(fetchedJob.isCompleted());
        assertFalse(fetchedJob.isFailed());
    }

    @Test
    @DisplayName("KeepJobs retention policy trims completed jobs to configured count")
    void testKeepJobsRetentionPolicy() throws Exception {
        String queueName = "test-retention-" + System.currentTimeMillis();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .payloadClass(String.class)
                .build();
        queuesToClean.add(queue);

        CountDownLatch completedLatch = new CountDownLatch(5);
        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .payloadClass(String.class)
                .processor(job -> {
                    completedLatch.countDown();
                    return "done";
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        // Enqueue 5 jobs configured to keep at most 2 completed jobs
        JobOptions opts = JobOptions.builder()
                .removeOnComplete(KeepJobs.count(2))
                .build();

        for (int i = 0; i < 5; i++) {
            queue.add("retentionJob", "item-" + i, opts);
        }

        assertTrue(completedLatch.await(6, TimeUnit.SECONDS));
        Thread.sleep(300);

        Map<String, Long> counts = queue.getJobCounts();
        Long completedCount = counts.get("completed");
        assertNotNull(completedCount);
        // Completed set must have been trimmed by moveToFinished Lua script to at most 2
        assertTrue(completedCount <= 2, "Expected completed jobs <= 2, found: " + completedCount);
    }

    @Test
    @DisplayName("removeOnComplete(true) deletes completed job immediately")
    void testRemoveImmediatelyOnComplete() throws Exception {
        String queueName = "test-remove-immediate-" + System.currentTimeMillis();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .payloadClass(String.class)
                .build();
        queuesToClean.add(queue);

        CountDownLatch latch = new CountDownLatch(1);
        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .payloadClass(String.class)
                .processor(job -> {
                    latch.countDown();
                    return "temp";
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        Job<String> job = queue.add("tempJob", "ephemeral", JobOptions.builder().removeOnComplete(true).build());
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        Thread.sleep(200);

        // Job keys should be completely removed from Redis
        Job<String> fetched = queue.getJob(job.getId());
        assertNull(fetched, "Job with removeOnComplete(true) should be deleted after completion");
    }

    @Test
    @DisplayName("FlowProducer.addBulk atomically schedules multiple DAG trees")
    void testFlowProducerAddBulk() {
        String queueName = "test-flow-bulk-" + System.currentTimeMillis();
        FlowProducer flowProducer = new FlowProducer(connectionManager);
        try {
            FlowJob<String> tree1 = FlowJob.of(queueName, "parent1", "p1")
                    .addChild(FlowJob.of(queueName, "child1a", "c1a"))
                    .addChild(FlowJob.of(queueName, "child1b", "c1b"));

            FlowJob<String> tree2 = FlowJob.of(queueName, "parent2", "p2")
                    .addChild(FlowJob.of(queueName, "child2a", "c2a"));

            List<String> parentIds = flowProducer.addBulk(List.of(tree1, tree2));
            assertNotNull(parentIds);
            assertEquals(2, parentIds.size());
            assertNotNull(parentIds.get(0));
            assertNotNull(parentIds.get(1));
            assertNotEquals(parentIds.get(0), parentIds.get(1));
        } finally {
            OxmqQueue<String> cleanQueue = OxmqQueue.<String>builder().name(queueName).connectionManager(connectionManager).build();
            cleanQueue.obliterate();
            cleanQueue.close();
        }
    }

    @Test
    @DisplayName("QueueEvents off removes registered event listener successfully")
    void testQueueEventsOff() throws Exception {
        String queueName = "test-events-off-" + System.currentTimeMillis();
        QueueEvents events = QueueEvents.builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .build()
                .start();
        eventsToClose.add(events);

        AtomicInteger invocationCount = new AtomicInteger(0);
        java.util.function.BiConsumer<String, Map<String, Object>> listener = (jobId, payload) -> {
            invocationCount.incrementAndGet();
        };

        events.on("completed", listener);
        // Remove immediately
        events.off("completed", listener);

        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .payloadClass(String.class)
                .build();
        queuesToClean.add(queue);

        CountDownLatch latch = new CountDownLatch(1);
        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .payloadClass(String.class)
                .processor(job -> {
                    latch.countDown();
                    return "ok";
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        queue.add("job1", "data");
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        Thread.sleep(300);

        // Listener was deregistered before job completed, so invocation count must be 0
        assertEquals(0, invocationCount.get());
    }
}
