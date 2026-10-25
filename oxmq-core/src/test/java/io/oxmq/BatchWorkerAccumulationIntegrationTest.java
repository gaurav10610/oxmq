package io.oxmq;

import io.oxmq.client.RedisConnectionManager;
import io.oxmq.model.Job;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Batch Worker Accumulation Buffer Integration Tests")
class BatchWorkerAccumulationIntegrationTest {

    private String queueName;
    private RedisConnectionManager connectionManager;
    private OxmqQueue<String> queue;
    private OxmqBatchWorker<String> batchWorker;

    @BeforeEach
    void setUp() {
        queueName = "batch-accum-" + UUID.randomUUID();
        connectionManager = new RedisConnectionManager("redis://localhost:6379");
        queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
    }

    @AfterEach
    void tearDown() {
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
    @DisplayName("Waits for minBatchSize to accumulate before dispatching batch")
    void testWaitsForMinBatchSize() throws Exception {
        CountDownLatch batchProcessedLatch = new CountDownLatch(1);
        AtomicInteger batchSizeObserved = new AtomicInteger(0);

        batchWorker = OxmqBatchWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .batchSize(10)
                .minBatchSize(3)
                .batchTimeout(Duration.ofMillis(800))
                .processor(jobs -> {
                    batchSizeObserved.set(jobs.size());
                    batchProcessedLatch.countDown();
                    return "ok";
                })
                .build();

        batchWorker.start();

        // Enqueue 1 job first
        queue.add("item-1", "data-1");

        // Wait 150ms (less than 800ms timeout), then add 2 more jobs so total = 3
        Thread.sleep(150);
        queue.add("item-2", "data-2");
        queue.add("item-3", "data-3");

        assertTrue(batchProcessedLatch.await(3, TimeUnit.SECONDS), "Batch should process");
        assertEquals(3, batchSizeObserved.get(), "Worker should have accumulated 3 jobs in a single batch");
    }

    @Test
    @DisplayName("Flushes partial batch when batchTimeout expires if fewer than minBatchSize exist")
    void testFlushesOnTimeoutWhenBelowMinBatchSize() throws Exception {
        CountDownLatch batchProcessedLatch = new CountDownLatch(1);
        AtomicInteger batchSizeObserved = new AtomicInteger(0);

        batchWorker = OxmqBatchWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .batchSize(10)
                .minBatchSize(5) // Requires 5, but we only supply 2
                .batchTimeout(Duration.ofMillis(400))
                .processor(jobs -> {
                    batchSizeObserved.set(jobs.size());
                    batchProcessedLatch.countDown();
                    return "ok";
                })
                .build();

        batchWorker.start();

        long startTime = System.currentTimeMillis();
        queue.add("item-1", "data-1");
        queue.add("item-2", "data-2");

        assertTrue(batchProcessedLatch.await(4, TimeUnit.SECONDS), "Batch should flush after timeout");
        long elapsed = System.currentTimeMillis() - startTime;

        assertTrue(elapsed >= 350, "Batch flush should wait approximately batchTimeout duration (elapsed: " + elapsed + " ms)");
        assertEquals(2, batchSizeObserved.get(), "Batch worker should flush the 2 accumulated jobs after timeout");
    }
}
