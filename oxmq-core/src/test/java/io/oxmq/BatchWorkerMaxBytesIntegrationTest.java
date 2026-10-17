package io.oxmq;

import io.oxmq.client.RedisConnectionManager;
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

import static org.assertj.core.api.Assertions.assertThat;

public class BatchWorkerMaxBytesIntegrationTest {

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
            try {
                connectionManager.close();
            } catch (Exception ignored) {}
        }
    }

    @Test
    @DisplayName("maxBatchBytes splits large payloads into smaller batches before reaching batchSize")
    void testMaxBatchBytesBudget() throws Exception {
        String queueName = "batch-bytes-test-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        // Enqueue 6 jobs with ~400 characters payload each
        String chunk400 = "A".repeat(400);
        for (int i = 0; i < 6; i++) {
            queue.add("item-" + i, chunk400);
        }

        List<Integer> batchSizes = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch allProcessed = new CountDownLatch(6);

        // batchSize is 10, but maxBatchBytes is 1000.
        // Each job is > 400 bytes in JSON, so 3 jobs ~ 1200 bytes >= 1000 bytes.
        // Thus each batch should be capped at 3 jobs instead of 6 or 10.
        OxmqBatchWorker<String> batchWorker = OxmqBatchWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .batchSize(10)
                .maxBatchBytes(1000)
                .processor(jobs -> {
                    batchSizes.add(jobs.size());
                    for (int i = 0; i < jobs.size(); i++) {
                        allProcessed.countDown();
                    }
                    return "ok";
                })
                .build();
        workersToStop.add(batchWorker);
        batchWorker.start();

        assertThat(allProcessed.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(batchWorker.getMaxBatchBytes()).isEqualTo(1000);

        // Verify that batches were split by byte budget (expected [3, 3] rather than [6])
        assertThat(batchSizes).containsExactly(3, 3);
    }
}
