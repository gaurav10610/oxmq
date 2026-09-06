package io.oxmq;

import io.lettuce.core.RedisClient;
import io.oxmq.model.JobState;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class OxmqBatchWorkerIntegrationTest {

    private RedisClient redisClient;
    private String queueName;
    private OxmqQueue<String> queue;

    @BeforeEach
    void setUp() {
        String redisUri = System.getProperty("oxmq.redis.uri", "redis://localhost:6379");
        redisClient = RedisClient.create(redisUri);
        queueName = "test-batch-" + System.currentTimeMillis();
        queue = OxmqQueue.<String>builder()
                .name(queueName)
                .redisClient(redisClient)
                .payloadClass(String.class)
                .build();
    }

    @AfterEach
    void tearDown() {
        if (queue != null) {
            try { queue.obliterate(); queue.close(); } catch (Exception ignored) {}
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    @Test
    void testBatchDequeueAndCompletion() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger totalPopped = new AtomicInteger(0);

        OxmqBatchWorker<String> batchWorker = OxmqBatchWorker.<String>builder()
                .queueName(queueName)
                .redisClient(redisClient)
                .payloadClass(String.class)
                .batchSize(20)
                .pollIntervalMs(20)
                .processor(batch -> {
                    totalPopped.addAndGet(batch.size());
                    latch.countDown();
                    return "BATCH_SUCCESS";
                })
                .build();

        // Enqueue 15 jobs
        for (int i = 1; i <= 15; i++) {
            queue.add("item-" + i, "data-" + i);
        }

        assertThat(queue.count(JobState.WAITING)).isEqualTo(15);

        batchWorker.start();

        boolean completed = latch.await(4, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
        assertThat(totalPopped.get()).isEqualTo(15);

        TimeUnit.MILLISECONDS.sleep(200);
        assertThat(queue.count(JobState.COMPLETED)).isEqualTo(15);
        assertThat(queue.count(JobState.ACTIVE)).isEqualTo(0);

        batchWorker.close();
    }
}
