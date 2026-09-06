package io.oxmq.example.standalone;

import io.lettuce.core.RedisClient;
import io.oxmq.OxmqQueue;
import io.oxmq.OxmqWorker;
import io.oxmq.model.JobState;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StandaloneLoadIntegrationTest {

    private RedisClient redisClient;
    private OxmqQueue<StandaloneQuickstartApplication.EmailPayload> queue;
    private OxmqWorker<StandaloneQuickstartApplication.EmailPayload> worker;
    private String queueName;

    @BeforeEach
    void setUp() {
        queueName = "test-load-standalone-" + UUID.randomUUID().toString().substring(0, 8);
        redisClient = RedisClient.create("redis://localhost:6379");
        queue = OxmqQueue.<StandaloneQuickstartApplication.EmailPayload>builder()
                .name(queueName)
                .redisClient(redisClient)
                .payloadClass(StandaloneQuickstartApplication.EmailPayload.class)
                .build();
    }

    @AfterEach
    void tearDown() {
        if (worker != null) {
            worker.close();
        }
        if (queue != null) {
            queue.obliterate();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    @Test
    @DisplayName("Should process 1,000 jobs concurrently with 50 Virtual Threads without thread starvation or leaks")
    void testHighConcurrencyBurst() throws InterruptedException {
        int totalJobs = 1000;
        CountDownLatch latch = new CountDownLatch(totalJobs);
        AtomicInteger processedCount = new AtomicInteger(0);

        // 1. Enqueue 1,000 jobs
        for (int i = 0; i < totalJobs; i++) {
            queue.add("email-task-" + i, new StandaloneQuickstartApplication.EmailPayload(
                    "user" + i + "@example.com",
                    "Welcome Notification " + i,
                    "Hello user " + i,
                    Instant.now()
            ));
        }

        assertThat(queue.count(JobState.WAITING)).isEqualTo(totalJobs);

        // 2. Start Worker with 50 Virtual Threads
        long startTime = System.currentTimeMillis();
        worker = OxmqWorker.<StandaloneQuickstartApplication.EmailPayload>builder()
                .queueName(queueName)
                .redisClient(redisClient)
                .payloadClass(StandaloneQuickstartApplication.EmailPayload.class)
                .concurrency(50)
                .useVirtualThreads(true)
                .processor(job -> {
                    assertThat(Thread.currentThread().isVirtual()).isTrue();
                    processedCount.incrementAndGet();
                    latch.countDown();
                    return "SENT";
                })
                .build();

        worker.start();

        // 3. Await completion
        boolean finished = latch.await(15, TimeUnit.SECONDS);
        long duration = System.currentTimeMillis() - startTime;

        assertThat(finished).as("All 1,000 jobs should complete within 15s").isTrue();
        assertThat(processedCount.get()).isEqualTo(totalJobs);

        // 4. Verify Redis state invariants
        TimeUnit.MILLISECONDS.sleep(200);
        assertThat(queue.count(JobState.WAITING)).isEqualTo(0);
        assertThat(queue.count(JobState.ACTIVE)).isEqualTo(0);
        assertThat(queue.count(JobState.COMPLETED)).isEqualTo(totalJobs);
        assertThat(queue.count(JobState.FAILED)).isEqualTo(0);
    }
}
