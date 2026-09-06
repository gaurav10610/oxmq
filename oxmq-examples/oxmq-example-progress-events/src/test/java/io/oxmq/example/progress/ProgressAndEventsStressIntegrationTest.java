package io.oxmq.example.progress;

import io.lettuce.core.RedisClient;
import io.oxmq.OxmqQueue;
import io.oxmq.OxmqWorker;
import io.oxmq.QueueEvents;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProgressAndEventsStressIntegrationTest {

    private RedisClient redisClient;
    private OxmqQueue<ProgressAndEventsExample.BatchMigrationTask> queue;
    private OxmqWorker<ProgressAndEventsExample.BatchMigrationTask> worker;
    private QueueEvents queueEvents;
    private String queueName;

    @BeforeEach
    void setUp() {
        queueName = "test-progress-events-" + UUID.randomUUID().toString().substring(0, 8);
        redisClient = RedisClient.create("redis://localhost:6379");
        queue = OxmqQueue.<ProgressAndEventsExample.BatchMigrationTask>builder()
                .name(queueName)
                .redisClient(redisClient)
                .payloadClass(ProgressAndEventsExample.BatchMigrationTask.class)
                .build();
    }

    @AfterEach
    void tearDown() {
        if (worker != null) {
            worker.close();
        }
        if (queueEvents != null) {
            queueEvents.close();
        }
        if (queue != null) {
            queue.obliterate();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    @Test
    @DisplayName("QueueEvents should reliably stream all progress updates (25%, 50%, 75%, 100%) and completion events across concurrent jobs")
    void testProgressAndPubSubEventsStreaming() throws InterruptedException {
        int totalJobs = 5;
        // Each job emits 4 progress events (25, 50, 75, 100)
        int expectedProgressEvents = totalJobs * 4;

        CountDownLatch progressLatch = new CountDownLatch(expectedProgressEvents);
        CountDownLatch completedLatch = new CountDownLatch(totalJobs);

        List<Integer> receivedProgressValues = Collections.synchronizedList(new ArrayList<>());
        List<String> receivedCompletedResults = Collections.synchronizedList(new ArrayList<>());

        // 1. Start QueueEvents Subscriber
        queueEvents = new QueueEvents(queueName, redisClient);
        queueEvents.onProgress((jobId, progress) -> {
            if (progress instanceof Number n) {
                receivedProgressValues.add(n.intValue());
            } else if (progress != null) {
                receivedProgressValues.add(Integer.parseInt(progress.toString()));
            }
            progressLatch.countDown();
        });
        queueEvents.onCompleted((jobId, result) -> {
            receivedCompletedResults.add(String.valueOf(result));
            completedLatch.countDown();
        });
        queueEvents.start();

        // 2. Start Worker
        worker = OxmqWorker.<ProgressAndEventsExample.BatchMigrationTask>builder()
                .queueName(queueName)
                .redisClient(redisClient)
                .payloadClass(ProgressAndEventsExample.BatchMigrationTask.class)
                .concurrency(5)
                .processor(job -> {
                    ProgressAndEventsExample.BatchMigrationTask task = job.getData();
                    job.log("Step 1: 25%");
                    job.updateProgress(25);
                    Thread.sleep(30);

                    job.log("Step 2: 50%");
                    job.updateProgress(50);
                    Thread.sleep(30);

                    job.log("Step 3: 75%");
                    job.updateProgress(75);
                    Thread.sleep(30);

                    job.log("Step 4: 100%");
                    job.updateProgress(100);

                    return "MIGRATION_COMPLETED_" + task.datasetId();
                })
                .build();

        worker.start();

        // 3. Enqueue 5 migration tasks
        for (int i = 0; i < totalJobs; i++) {
            queue.add("migrate-" + i,
                    new ProgressAndEventsExample.BatchMigrationTask("dataset_" + i, 1000),
                    JobOptions.defaults()
            );
        }

        // 4. Await streaming events
        boolean progressDone = progressLatch.await(8, TimeUnit.SECONDS);
        boolean completedDone = completedLatch.await(8, TimeUnit.SECONDS);

        assertThat(progressDone).as("All 20 progress events should be received via Pub/Sub").isTrue();
        assertThat(completedDone).as("All 5 completion events should be received").isTrue();

        assertThat(receivedProgressValues).hasSize(expectedProgressEvents);
        assertThat(receivedCompletedResults).hasSize(totalJobs);

        TimeUnit.MILLISECONDS.sleep(200);
        assertThat(queue.count(JobState.COMPLETED)).isEqualTo(totalJobs);
        assertThat(queue.count(JobState.WAITING)).isEqualTo(0);
    }
}
