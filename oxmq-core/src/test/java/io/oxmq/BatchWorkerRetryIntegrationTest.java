package io.oxmq;

import io.oxmq.client.RedisConnectionManager;
import io.oxmq.model.BackoffStrategy;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BatchWorkerRetryIntegrationTest {

    private String queueName;
    private RedisConnectionManager connectionManager;
    private OxmqQueue<String> queue;
    private OxmqBatchWorker<String> batchWorker;

    @BeforeEach
    void setUp() {
        queueName = "batch-retry-" + UUID.randomUUID();
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
    @DisplayName("OxmqBatchWorker: retries failed job when attempts > 1 and respects backoff delay")
    void testBatchWorkerRetriesFailedJob() throws Exception {
        AtomicInteger attemptCounter = new AtomicInteger(0);
        CountDownLatch completeLatch = new CountDownLatch(1);

        batchWorker = OxmqBatchWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .batchSize(5)
                .processor(jobs -> {
                    for (Job<String> job : jobs) {
                        int attempt = attemptCounter.incrementAndGet();
                        if (attempt == 1) {
                            throw new RuntimeException("Transient batch failure attempt 1");
                        }
                    }
                    completeLatch.countDown();
                    return "retry-success";
                })
                .build();

        batchWorker.start();

        JobOptions retryOpts = JobOptions.builder()
                .attempts(2)
                .fixedBackoff(100)
                .build();

        Job<String> job = queue.add("test-retry-job", "payload-data", retryOpts);

        assertTrue(completeLatch.await(5, TimeUnit.SECONDS), "Job should succeed on retry attempt 2");
        assertEquals(2, attemptCounter.get(), "Batch processor should have executed twice for retried job");

        Thread.sleep(300);
        assertEquals(JobState.COMPLETED, queue.getJobState(job.getId()));
    }
}
