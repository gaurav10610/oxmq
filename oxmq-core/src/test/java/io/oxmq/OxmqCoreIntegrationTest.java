package io.oxmq;

import io.lettuce.core.RedisClient;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class OxmqCoreIntegrationTest {

    private RedisClient redisClient;
    private String queueName;
    private OxmqQueue<String> queue;

    @BeforeEach
    void setUp() {
        String redisUri = System.getProperty("oxmq.redis.uri", "redis://localhost:6379");
        redisClient = RedisClient.create(redisUri);
        queueName = "test-core-" + System.currentTimeMillis();
        queue = OxmqQueue.<String>builder()
                .name(queueName)
                .redisClient(redisClient)
                .payloadClass(String.class)
                .build();
    }

    @AfterEach
    void tearDown() {
        if (queue != null) {
            try {
                queue.obliterate();
                queue.close();
            } catch (Exception ignored) {}
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    @Test
    void testEnqueueAndExecuteWithVirtualThreads() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> processedData = new AtomicReference<>();
        AtomicBoolean isVirtualThread = new AtomicBoolean(false);

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .redisClient(redisClient)
                .payloadClass(String.class)
                .concurrency(5)
                .pollIntervalMs(20)
                .processor(job -> {
                    processedData.set(job.getData());
                    isVirtualThread.set(Thread.currentThread().isVirtual());
                    job.updateProgress(50);
                    job.log("Worker execution completed");
                    latch.countDown();
                    return "OK";
                })
                .build();

        worker.start();

        Job<String> enqueued = queue.add("test-job", "hello-world");
        assertThat(enqueued.getId()).isNotNull();

        boolean completed = latch.await(5, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
        assertThat(processedData.get()).isEqualTo("hello-world");
        assertThat(isVirtualThread.get()).isTrue();

        // Check Redis state
        TimeUnit.MILLISECONDS.sleep(200);
        assertThat(queue.count(JobState.COMPLETED)).isEqualTo(1);
        assertThat(queue.count(JobState.ACTIVE)).isEqualTo(0);

        worker.close();
    }

    @Test
    void testDelayedJobExecution() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        long startTime = System.currentTimeMillis();

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .redisClient(redisClient)
                .payloadClass(String.class)
                .pollIntervalMs(20)
                .processor(job -> {
                    latch.countDown();
                    return "DONE";
                })
                .build();

        worker.start();

        // Schedule with 1000ms delay
        queue.add("delayed-job", "delayed-payload", JobOptions.builder().delay(Duration.ofMillis(1000)).build());

        assertThat(queue.count(JobState.DELAYED)).isEqualTo(1);

        boolean completed = latch.await(4, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
        long duration = System.currentTimeMillis() - startTime;
        assertThat(duration).isGreaterThanOrEqualTo(950);

        worker.close();
    }

    @Test
    void testCustomJobIdDeduplication() {
        String customId = "unique-order-12345";

        Job<String> job1 = queue.add("order", "payload-1", JobOptions.builder().jobId(customId).build());
        Job<String> job2 = queue.add("order", "payload-2", JobOptions.builder().jobId(customId).build());

        assertThat(job1.getId()).isEqualTo(customId);
        assertThat(job2.getId()).isEqualTo(customId);
        assertThat(queue.count(JobState.WAITING)).isEqualTo(1);
    }
}
