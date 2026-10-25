package io.oxmq;

import io.oxmq.client.RedisConnectionManager;
import io.oxmq.exception.JobTimeoutException;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("In-Flight Hard Execution Timeouts Integration Tests")
class JobTimeoutIntegrationTest {

    private String queueName;
    private RedisConnectionManager connectionManager;
    private OxmqQueue<String> queue;
    private OxmqWorker<String> worker;

    @BeforeEach
    void setUp() {
        queueName = "timeout-queue-" + UUID.randomUUID();
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
    @DisplayName("Watchdog interrupts hanging job when jobTimeout is exceeded and marks it FAILED")
    void testJobTimeoutInterruptsHangingJob() throws Exception {
        CountDownLatch failedLatch = new CountDownLatch(1);
        AtomicReference<Throwable> failureRef = new AtomicReference<>();
        AtomicBoolean interruptedRef = new AtomicBoolean(false);

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(1)
                .processor(job -> {
                    try {
                        // Simulate hanging operation (sleeping 10 seconds)
                        Thread.sleep(10_000);
                    } catch (InterruptedException e) {
                        interruptedRef.set(true);
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                    return "should-not-reach-here";
                })
                .onFailed((job, err) -> {
                    failureRef.set(err);
                    failedLatch.countDown();
                })
                .build();

        worker.start();

        // Enqueue job with 250ms hard timeout
        Job<String> job = queue.add("hanging-task", "payload", JobOptions.builder()
                .jobTimeout(Duration.ofMillis(250))
                .build());

        boolean failed = failedLatch.await(5, TimeUnit.SECONDS);
        assertTrue(failed, "Job should have failed due to timeout within 5 seconds");
        assertTrue(interruptedRef.get(), "Thread must have been interrupted by timeout watchdog");

        assertNotNull(failureRef.get(), "Failure error must be captured");
        assertTrue(failureRef.get() instanceof JobTimeoutException ||
                   failureRef.get().getMessage().toLowerCase().contains("timed out"),
                "Error should indicate timeout: " + failureRef.get());

        JobState state = queue.getJobState(job.getId());
        assertEquals(JobState.FAILED, state, "Job state in Redis must be FAILED");
    }

    @Test
    @DisplayName("Cooperative cancellation token throwIfCancelled detects timeout")
    void testCooperativeCancellationTokenDetectsTimeout() throws Exception {
        CountDownLatch failedLatch = new CountDownLatch(1);
        AtomicReference<Throwable> failureRef = new AtomicReference<>();

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(1)
                .processor(job -> {
                    long deadline = System.currentTimeMillis() + 5000;
                    while (System.currentTimeMillis() < deadline) {
                        job.cancellationToken().throwIfCancelled();
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            job.cancellationToken().throwIfCancelled();
                        }
                    }
                    return "done";
                })
                .onFailed((job, err) -> {
                    failureRef.set(err);
                    failedLatch.countDown();
                })
                .build();

        worker.start();

        Job<String> job = queue.add("cooperative-task", "payload", JobOptions.builder()
                .jobTimeout(Duration.ofMillis(200))
                .build());

        boolean failed = failedLatch.await(5, TimeUnit.SECONDS);
        assertTrue(failed, "Job should have failed via cooperative cancellation within 5 seconds");
        assertNotNull(failureRef.get());

        JobState state = queue.getJobState(job.getId());
        assertEquals(JobState.FAILED, state);
    }

    @Test
    @DisplayName("Job timeout with multiple retry attempts retries instead of immediate permanent failure")
    void testJobTimeoutWithRetriesScheduledForRetry() throws Exception {
        CountDownLatch firstAttemptTimeoutLatch = new CountDownLatch(1);
        CountDownLatch secondAttemptCompleteLatch = new CountDownLatch(1);
        AtomicBoolean firstAttempt = new AtomicBoolean(true);

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(1)
                .processor(job -> {
                    if (firstAttempt.compareAndSet(true, false)) {
                        // First attempt hangs and times out
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            firstAttemptTimeoutLatch.countDown();
                            Thread.currentThread().interrupt();
                            throw e;
                        }
                        return "hang";
                    }
                    // Second attempt succeeds quickly
                    secondAttemptCompleteLatch.countDown();
                    return "success-on-retry";
                })
                .build();

        worker.start();

        // Enqueue job with 200ms timeout and 2 attempts with 100ms fixed backoff
        Job<String> job = queue.add("retry-on-timeout", "payload", JobOptions.builder()
                .jobTimeout(Duration.ofMillis(200))
                .attempts(2)
                .fixedBackoff(Duration.ofMillis(100))
                .build());

        assertTrue(firstAttemptTimeoutLatch.await(4, TimeUnit.SECONDS), "First attempt should timeout");
        assertTrue(secondAttemptCompleteLatch.await(5, TimeUnit.SECONDS), "Second attempt should succeed after retry");

        // Wait brief moment for state transition to complete
        Thread.sleep(300);
        JobState state = queue.getJobState(job.getId());
        assertEquals(JobState.COMPLETED, state, "Job must eventually complete on retry");
    }
}
