package io.oxmq.example.batch;

import io.lettuce.core.RedisClient;
import io.oxmq.OxmqBatchWorker;
import io.oxmq.OxmqQueue;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BatchIngestionStressIntegrationTest {

    private RedisClient redisClient;
    private OxmqQueue<BatchDatabaseIngestionExample.AuditLogEvent> queue;
    private OxmqBatchWorker<BatchDatabaseIngestionExample.AuditLogEvent> batchWorker;
    private String queueName;

    @BeforeEach
    void setUp() {
        queueName = "test-batch-ingestion-" + UUID.randomUUID().toString().substring(0, 8);
        redisClient = RedisClient.create("redis://localhost:6379");
        queue = OxmqQueue.<BatchDatabaseIngestionExample.AuditLogEvent>builder()
                .name(queueName)
                .redisClient(redisClient)
                .payloadClass(BatchDatabaseIngestionExample.AuditLogEvent.class)
                .build();
    }

    @AfterEach
    void tearDown() {
        if (batchWorker != null) {
            batchWorker.close();
        }
        if (queue != null) {
            queue.obliterate();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    @Test
    @DisplayName("Should batch pop and ingest 500 audit logs in chunks of up to 50 items")
    void testBulkBatchIngestion() throws InterruptedException {
        int totalEvents = 500;
        int batchSize = 50;

        CountDownLatch latch = new CountDownLatch(totalEvents);
        List<Integer> batchSizes = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger totalProcessed = new AtomicInteger(0);

        // 1. Enqueue 500 events
        for (int i = 0; i < totalEvents; i++) {
            queue.add("evt-" + i, new BatchDatabaseIngestionExample.AuditLogEvent(
                    "id_" + i, "user_" + (i % 20), "PURCHASE", "10.0.0." + (i % 255), Instant.now()
            ));
        }

        assertThat(queue.count(JobState.WAITING)).isEqualTo(totalEvents);

        // 2. Start Batch Worker
        batchWorker = OxmqBatchWorker.<BatchDatabaseIngestionExample.AuditLogEvent>builder()
                .queueName(queueName)
                .redisClient(redisClient)
                .payloadClass(BatchDatabaseIngestionExample.AuditLogEvent.class)
                .batchSize(batchSize)
                .pollIntervalMs(20)
                .processor(batch -> {
                    batchSizes.add(batch.size());
                    totalProcessed.addAndGet(batch.size());
                    for (int j = 0; j < batch.size(); j++) {
                        latch.countDown();
                    }
                    return "INSERTED_" + batch.size();
                })
                .build();

        batchWorker.start();

        // 3. Await processing
        boolean finished = latch.await(10, TimeUnit.SECONDS);
        assertThat(finished).as("All 500 events should be batch processed").isTrue();
        assertThat(totalProcessed.get()).isEqualTo(totalEvents);

        for (int size : batchSizes) {
            assertThat(size).isLessThanOrEqualTo(batchSize);
        }

        // 4. Verify Redis states
        TimeUnit.MILLISECONDS.sleep(200);
        assertThat(queue.count(JobState.WAITING)).isEqualTo(0);
        assertThat(queue.count(JobState.COMPLETED)).isEqualTo(totalEvents);
    }

    @Test
    @DisplayName("Should flush partial/odd batches (73 items with batchSize 50) without hanging")
    void testPartialBatchFlush() throws InterruptedException {
        int totalEvents = 73;
        int batchSize = 50;

        CountDownLatch latch = new CountDownLatch(totalEvents);
        AtomicInteger totalProcessed = new AtomicInteger(0);

        for (int i = 0; i < totalEvents; i++) {
            queue.add("odd-evt-" + i, new BatchDatabaseIngestionExample.AuditLogEvent(
                    "id_" + i, "user_1", "ACTION", "127.0.0.1", Instant.now()
            ));
        }

        batchWorker = OxmqBatchWorker.<BatchDatabaseIngestionExample.AuditLogEvent>builder()
                .queueName(queueName)
                .redisClient(redisClient)
                .payloadClass(BatchDatabaseIngestionExample.AuditLogEvent.class)
                .batchSize(batchSize)
                .pollIntervalMs(20)
                .processor(batch -> {
                    totalProcessed.addAndGet(batch.size());
                    for (int j = 0; j < batch.size(); j++) {
                        latch.countDown();
                    }
                    return "OK";
                })
                .build();

        batchWorker.start();

        boolean finished = latch.await(6, TimeUnit.SECONDS);
        assertThat(finished).as("Partial batches should flush cleanly").isTrue();
        assertThat(totalProcessed.get()).isEqualTo(totalEvents);

        TimeUnit.MILLISECONDS.sleep(200);
        assertThat(queue.count(JobState.WAITING)).isEqualTo(0);
        assertThat(queue.count(JobState.COMPLETED)).isEqualTo(totalEvents);
    }
}
