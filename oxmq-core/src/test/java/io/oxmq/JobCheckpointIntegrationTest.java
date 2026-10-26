package io.oxmq;

import io.oxmq.client.RedisConnectionManager;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Resumable State Machines & Job Progress Checkpointing Integration Tests")
class JobCheckpointIntegrationTest {

    private String queueName;
    private RedisConnectionManager connectionManager;
    private OxmqQueue<String> queue;
    private OxmqWorker<String> worker;

    public static class WorkflowState {
        public boolean step1Done;
        public boolean step2Done;
        public int processedRecords;

        public WorkflowState() {}

        public WorkflowState(boolean s1, boolean s2, int records) {
            this.step1Done = s1;
            this.step2Done = s2;
            this.processedRecords = records;
        }
    }

    @BeforeEach
    void setUp() {
        queueName = "checkpoint-queue-" + UUID.randomUUID();
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
        if (queue != null) {
            try { queue.obliterate(); } catch (Exception ignored) {}
            try { queue.close(); } catch (Exception ignored) {}
        }
        if (connectionManager != null) {
            try { connectionManager.close(); } catch (Exception ignored) {}
        }
    }

    @Test
    @DisplayName("Failed job retries resume directly from previous checkpoint without re-running completed steps")
    void testResumableWorkflowFromCheckpoint() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        List<String> executedSteps = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch finishedLatch = new CountDownLatch(1);

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(1)
                .processor(job -> {
                    int attempt = attempts.incrementAndGet();
                    WorkflowState state = job.getCheckpoint(WorkflowState.class);
                    if (state == null) {
                        state = new WorkflowState();
                    }

                    // Step 1: Download
                    if (!state.step1Done) {
                        executedSteps.add("step-1-download");
                        state.step1Done = true;
                        state.processedRecords = 50;
                        job.checkpoint("download", state);
                    }

                    // Step 2: Transform
                    if (!state.step2Done) {
                        executedSteps.add("step-2-transform");
                        state.step2Done = true;
                        state.processedRecords = 100;
                        job.checkpoint("transform", state);
                    }

                    // Step 3: Upload (fail on first attempt to trigger retry)
                    if (attempt == 1) {
                        executedSteps.add("step-3-failed");
                        throw new RuntimeException("Simulated transient network drop in step 3");
                    }

                    executedSteps.add("step-3-upload-success");
                    finishedLatch.countDown();
                    return "all-done";
                })
                .build();

        worker.start();

        Job<String> job = queue.add("multi-stage-etl", "source-uri",
                JobOptions.builder().attempts(2).backoff(io.oxmq.model.BackoffStrategy.fixed(100)).build());

        assertTrue(finishedLatch.await(5, TimeUnit.SECONDS), "Workflow should finish on second attempt");

        // Verify that on attempt 2, step 1 and step 2 were NOT executed again!
        assertEquals(List.of("step-1-download", "step-2-transform", "step-3-failed", "step-3-upload-success"), executedSteps,
                "Retried attempt must resume from checkpoint and skip step 1 and step 2!");

        // Verify that the checkpoint in Redis contains step2 state
        Job<String> completedJob = queue.getJob(job.getId());
        assertNotNull(completedJob);
        assertEquals("transform", completedJob.getCheckpointStep());
        WorkflowState finalState = completedJob.getCheckpoint(WorkflowState.class);
        assertNotNull(finalState);
        assertTrue(finalState.step1Done);
        assertTrue(finalState.step2Done);
        assertEquals(100, finalState.processedRecords);
    }
}
