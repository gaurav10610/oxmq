package io.oxmq;

import io.oxmq.client.RedisConnectionManager;
import io.oxmq.model.Job;
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

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Declarative Per-Job-Type Timeout Mappings on Worker Integration Tests")
class DeclarativeTimeoutIntegrationTest {

    private String queueName;
    private RedisConnectionManager connectionManager;
    private OxmqQueue<String> queue;
    private OxmqWorker<String> worker;

    @BeforeEach
    void setUp() {
        queueName = "decl-timeout-queue-" + UUID.randomUUID();
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
    @DisplayName("Worker applies specific declarative timeouts per task name")
    void testDeclarativeWorkerTimeoutsPerTaskName() throws Exception {
        CountDownLatch fastFailedLatch = new CountDownLatch(1);
        CountDownLatch slowSuccessLatch = new CountDownLatch(1);
        AtomicBoolean fastTimedOut = new AtomicBoolean(false);

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(2)
                .jobTimeout("fast-task", Duration.ofMillis(200))
                .jobTimeout("slow-task", Duration.ofSeconds(5))
                .processor(job -> {
                    try {
                        // Both tasks attempt to run for 500ms
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Task interrupted", e);
                    }
                    slowSuccessLatch.countDown();
                    return "done";
                })
                .onFailed((job, err) -> {
                    if ("fast-task".equals(job.getName())) {
                        if (err.getMessage() != null && err.getMessage().toLowerCase().contains("timed out")) {
                            fastTimedOut.set(true);
                        }
                        fastFailedLatch.countDown();
                    }
                })
                .build();

        worker.start();

        Job<String> fastJob = queue.add("fast-task", "fast-payload");
        Job<String> slowJob = queue.add("slow-task", "slow-payload");

        assertTrue(fastFailedLatch.await(5, TimeUnit.SECONDS), "fast-task should time out and fail");
        assertTrue(slowSuccessLatch.await(5, TimeUnit.SECONDS), "slow-task should complete successfully");
        assertTrue(fastTimedOut.get(), "fast-task should have failed with timeout exception");

        // Verify states in Redis
        Job<String> fetchedFast = queue.getJob(fastJob.getId());
        Job<String> fetchedSlow = queue.getJob(slowJob.getId());

        assertEquals(JobState.FAILED, fetchedFast.getState());
        assertEquals(JobState.COMPLETED, fetchedSlow.getState());
    }
}
