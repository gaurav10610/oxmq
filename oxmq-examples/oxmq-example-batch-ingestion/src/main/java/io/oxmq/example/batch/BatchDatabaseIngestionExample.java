package io.oxmq.example.batch;

import io.lettuce.core.RedisClient;
import io.oxmq.OxmqBatchWorker;
import io.oxmq.OxmqQueue;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real-World Showcase: High-Throughput Batch Dequeue &amp; Ingestion.
 * Ingest high-volume audit logs / telemetry events in bulk chunks into ClickHouse, Elasticsearch, or PostgreSQL.
 */
public class BatchDatabaseIngestionExample {

    private static final Logger log = LoggerFactory.getLogger(BatchDatabaseIngestionExample.class);

    public record AuditLogEvent(String eventId, String userId, String action, String ipAddress, Instant timestamp) {}

    public static void main(String[] args) throws Exception {
        String redisUri = System.getProperty("oxmq.redis.uri", "redis://localhost:6379");
        RedisClient redisClient = RedisClient.create(redisUri);

        String queueName = "audit-log-ingestion";

        // 1. Producer Queue
        OxmqQueue<AuditLogEvent> queue = OxmqQueue.<AuditLogEvent>builder()
                .name(queueName)
                .redisClient(redisClient)
                .payloadClass(AuditLogEvent.class)
                .build();

        // 2. High-Throughput Batch Worker (pops up to 50 items at once)
        OxmqBatchWorker<AuditLogEvent> batchWorker = OxmqBatchWorker.<AuditLogEvent>builder()
                .queueName(queueName)
                .redisClient(redisClient)
                .payloadClass(AuditLogEvent.class)
                .batchSize(50) // Pop chunks of up to 50 jobs per Redis roundtrip
                .processor(batch -> {
                    log.info(">>> Batch Worker received {} audit logs! Executing 1 bulk database insert...", batch.size());

                    // Simulate 1 single bulk insert into ClickHouse / Elasticsearch
                    Thread.sleep(80);

                    log.info("Bulk insert of {} records committed successfully.", batch.size());
                    return "BULK_COMMITTED_" + batch.size();
                })
                .build();

        batchWorker.start();

        // 3. Enqueue 100 events rapidly
        log.info("Enqueuing 100 audit events...");
        for (int i = 1; i <= 100; i++) {
            queue.add("log-event-" + i,
                    new AuditLogEvent("evt_" + i, "user_" + (i % 10), "USER_LOGIN", "192.168.1." + (i % 255), Instant.now()),
                    JobOptions.defaults()
            );
        }

        // Wait to observe batch worker popping chunks
        TimeUnit.SECONDS.sleep(3);

        batchWorker.close();
        queue.close();
        redisClient.shutdown();
        log.info("Batch database ingestion recipe completed.");
    }
}
