package io.oxmq;

import io.oxmq.client.RedisConnectionManager;
import io.oxmq.model.CancellationToken;
import io.oxmq.model.Job;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Cluster-Wide Distributed Job Cancellation Integration Tests")
class DistributedCancellationIntegrationTest {

    private String queueName;
    private RedisConnectionManager connectionManager;
    private OxmqQueue<String> queue;
    private OxmqWorker<String> worker;

    @BeforeEach
    void setUp() {
        queueName = "cancel-cluster-queue-" + UUID.randomUUID();
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
    @DisplayName("queue.cancelJob() broadcasts cancellation over Redis pub/sub and halts active virtual thread")
    void testClusterWideCancellationInterruptsRemoteWorker() throws Exception {
        CountDownLatch startedLatch = new CountDownLatch(1);
        CountDownLatch cancelledLatch = new CountDownLatch(1);
        AtomicBoolean tokenWasTripped = new AtomicBoolean(false);

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(1)
                .processor(job -> {
                    startedLatch.countDown();
                    CancellationToken token = job.getCancellationToken();
                    try {
                        for (int i = 0; i < 50; i++) {
                            token.throwIfCancelled();
                            Thread.sleep(100);
                        }
                    } catch (Exception e) {
                        if (token.isCancelled() || job.isCancelled()) {
                            tokenWasTripped.set(true);
                        }
                        cancelledLatch.countDown();
                        throw new RuntimeException("Job aborted by cancellation signal", e);
                    }
                    return "completed-unexpectedly";
                })
                .build();

        worker.start();

        Job<String> job = queue.add("long-running-task", "payload-data");
        assertTrue(startedLatch.await(5, TimeUnit.SECONDS), "Job should start executing");

        // Simulate an independent web controller or separate JVM cancelling the job via Queue
        boolean dispatched = queue.cancelJob(job.getId(), "Aborted by cluster admin");
        assertTrue(dispatched, "Cancellation dispatch should return true");

        assertTrue(cancelledLatch.await(5, TimeUnit.SECONDS), "Worker should abort execution upon receiving cancellation signal");
        assertTrue(tokenWasTripped.get(), "Cancellation token should have been marked cancelled");
    }
}
