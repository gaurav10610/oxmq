package io.oxmq;

import io.oxmq.client.RedisConnectionManager;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Batch Worker Granular Item-Level Partial Failure Integration Tests")
class BatchWorkerPartialFailureIntegrationTest {

    private String queueName;
    private RedisConnectionManager connectionManager;
    private OxmqQueue<String> queue;
    private OxmqBatchWorker<String> batchWorker;

    @BeforeEach
    void setUp() {
        queueName = "batch-partial-" + UUID.randomUUID();
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
    @DisplayName("Batch with 5 jobs allows 2 marked failures while remaining 3 succeed")
    void testGranularItemLevelBatchFailures() throws Exception {
        CountDownLatch batchProcessedLatch = new CountDownLatch(1);

        batchWorker = OxmqBatchWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .batchSize(5)
                .processor(jobs -> {
                    assertEquals(5, jobs.size(), "Batch worker should receive 5 jobs");
                    for (Job<String> job : jobs) {
                        String payload = job.getData();
                        if (payload.contains("fail")) {
                            // Mark individual job as failed
                            job.setAsFailed(new IllegalArgumentException("Corrupt data: " + payload));
                        }
                    }
                    batchProcessedLatch.countDown();
                    return "batch-ok";
                })
                .build();

        batchWorker.start();

        List<Job<String>> submitted = new ArrayList<>();
        submitted.add(queue.add("item-1", "valid-1"));
        submitted.add(queue.add("item-2", "fail-corrupt-2"));
        submitted.add(queue.add("item-3", "valid-3"));
        submitted.add(queue.add("item-4", "fail-corrupt-4"));
        submitted.add(queue.add("item-5", "valid-5"));

        assertTrue(batchProcessedLatch.await(5, TimeUnit.SECONDS), "Batch should be processed within 5s");

        // Wait brief moment for Redis state transitions to settle
        Thread.sleep(400);

        // Verify that items 1, 3, 5 are COMPLETED
        assertEquals(JobState.COMPLETED, queue.getJobState(submitted.get(0).getId()));
        assertEquals(JobState.COMPLETED, queue.getJobState(submitted.get(2).getId()));
        assertEquals(JobState.COMPLETED, queue.getJobState(submitted.get(4).getId()));

        // Verify that items 2 and 4 are FAILED
        assertEquals(JobState.FAILED, queue.getJobState(submitted.get(1).getId()));
        assertEquals(JobState.FAILED, queue.getJobState(submitted.get(3).getId()));

        // Verify failure reason is recorded in the failed jobs
        Job<String> failedJob2 = queue.getJob(submitted.get(1).getId());
        assertNotNull(failedJob2.getFailedReason());
        assertTrue(failedJob2.getFailedReason().contains("Corrupt data: fail-corrupt-2"),
                "Failed reason should match exception message: " + failedJob2.getFailedReason());

        Job<String> failedJob4 = queue.getJob(submitted.get(3).getId());
        assertNotNull(failedJob4.getFailedReason());
        assertTrue(failedJob4.getFailedReason().contains("Corrupt data: fail-corrupt-4"));
    }
}
