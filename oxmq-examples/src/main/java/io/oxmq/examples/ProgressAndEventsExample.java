package io.oxmq.examples;

import io.lettuce.core.RedisClient;
import io.oxmq.OxmqQueue;
import io.oxmq.OxmqWorker;
import io.oxmq.QueueEvents;
import io.oxmq.model.JobOptions;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real-World Recipe: Real-Time Progress &amp; Event Streaming (QueueEvents)
 * Use Case: Long-running AI fine-tuning / data migration streaming live 0-100% progress and step logs to Bull-Board.
 */
public class ProgressAndEventsExample {

    private static final Logger log = LoggerFactory.getLogger(ProgressAndEventsExample.class);

    public record BatchMigrationTask(String datasetId, int totalRecords) {}

    public static void main(String[] args) throws Exception {
        String redisUri = System.getProperty("oxmq.redis.uri", "redis://localhost:6379");
        RedisClient redisClient = RedisClient.create(redisUri);

        String queueName = "dataset-migrations";

        // 1. QueueEvents Pub/Sub Listener
        QueueEvents events = new QueueEvents(queueName, redisClient);
        events.onWaiting(jobId -> log.info("[EVENT: WAITING] Job {} entered queue", jobId));
        events.onProgress((jobId, progress) -> log.info("[EVENT: PROGRESS] Job {} -> {}%", jobId, progress));
        events.onCompleted((jobId, result) -> log.info("[EVENT: COMPLETED] Job {} finished with: {}", jobId, result));
        events.onFailed((jobId, reason) -> log.warn("[EVENT: FAILED] Job {} failed: {}", jobId, reason));
        events.start();

        // 2. Producer Queue
        OxmqQueue<BatchMigrationTask> queue = OxmqQueue.<BatchMigrationTask>builder()
                .name(queueName)
                .redisClient(redisClient)
                .payloadClass(BatchMigrationTask.class)
                .build();

        // 3. Worker with Step Logging & Progress Updates
        OxmqWorker<BatchMigrationTask> worker = OxmqWorker.<BatchMigrationTask>builder()
                .queueName(queueName)
                .redisClient(redisClient)
                .payloadClass(BatchMigrationTask.class)
                .concurrency(5)
                .processor(job -> {
                    BatchMigrationTask task = job.getData();
                    log.info("Starting migration for dataset {} ({} records)...", task.datasetId(), task.totalRecords());

                    job.log("Step 1: Validating schema...");
                    job.updateProgress(20);
                    Thread.sleep(200);

                    job.log("Step 2: Migrating records 1-5000...");
                    job.updateProgress(50);
                    Thread.sleep(200);

                    job.log("Step 3: Migrating records 5001-10000...");
                    job.updateProgress(80);
                    Thread.sleep(200);

                    job.log("Step 4: Indexing and verification complete.");
                    job.updateProgress(100);

                    return "MIGRATED_" + task.totalRecords() + "_ROWS";
                })
                .build();

        worker.start();

        // 4. Enqueue migration job
        queue.add("migrate-customer-data",
                new BatchMigrationTask("dataset_users_2026", 10_000),
                JobOptions.defaults()
        );

        // Wait for execution and event delivery
        TimeUnit.SECONDS.sleep(3);

        worker.close();
        events.close();
        queue.close();
        redisClient.shutdown();
        log.info("Progress and QueueEvents recipe completed.");
    }
}
