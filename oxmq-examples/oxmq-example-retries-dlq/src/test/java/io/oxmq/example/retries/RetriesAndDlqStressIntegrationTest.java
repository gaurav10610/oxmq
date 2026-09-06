package io.oxmq.example.retries;

import io.lettuce.core.RedisClient;
import io.oxmq.OxmqQueue;
import io.oxmq.OxmqWorker;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RetriesAndDlqStressIntegrationTest {

    private RedisClient redisClient;
    private OxmqQueue<RetriesAndDlqExample.PaymentWebhook> queue;
    private OxmqWorker<RetriesAndDlqExample.PaymentWebhook> worker;
    private String queueName;

    @BeforeEach
    void setUp() {
        queueName = "test-retries-dlq-" + UUID.randomUUID().toString().substring(0, 8);
        redisClient = RedisClient.create("redis://localhost:6379");
        queue = OxmqQueue.<RetriesAndDlqExample.PaymentWebhook>builder()
                .name(queueName)
                .redisClient(redisClient)
                .payloadClass(RetriesAndDlqExample.PaymentWebhook.class)
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
    @DisplayName("Should handle mixed batch of direct success (25), retry recoveries (10), and permanent DLQ failures (5)")
    void testRetriesAndDeadLetterQueueRouting() throws InterruptedException {
        int normalCount = 25;
        int retryRecoverCount = 10;
        int permanentFailCount = 5;
        int totalJobs = normalCount + retryRecoverCount + permanentFailCount;

        Map<String, AtomicInteger> attemptTrackers = new ConcurrentHashMap<>();
        CountDownLatch completedLatch = new CountDownLatch(normalCount + retryRecoverCount);
        CountDownLatch dlqLatch = new CountDownLatch(permanentFailCount);

        // 1. Start Worker
        worker = OxmqWorker.<RetriesAndDlqExample.PaymentWebhook>builder()
                .queueName(queueName)
                .redisClient(redisClient)
                .payloadClass(RetriesAndDlqExample.PaymentWebhook.class)
                .concurrency(10)
                .pollIntervalMs(20)
                .processor(job -> {
                    RetriesAndDlqExample.PaymentWebhook payment = job.getData();
                    AtomicInteger attempts = attemptTrackers.computeIfAbsent(payment.eventId(), k -> new AtomicInteger(0));
                    int currentAttempt = attempts.incrementAndGet();

                    if (payment.eventId().startsWith("perm_fail_")) {
                        if (currentAttempt >= 3) {
                            dlqLatch.countDown();
                        }
                        throw new IllegalStateException("Permanent Gateway Error for " + payment.eventId());
                    } else if (payment.eventId().startsWith("retry_recover_")) {
                        if (currentAttempt < 2) {
                            throw new RuntimeException("Transient Gateway Timeout (HTTP 504)");
                        }
                        completedLatch.countDown();
                        return "RETRY_SUCCEEDED";
                    } else {
                        completedLatch.countDown();
                        return "SUCCESS_IMMEDIATE";
                    }
                })
                .build();

        worker.start();

        // 2. Enqueue 25 Normal Jobs
        for (int i = 0; i < normalCount; i++) {
            queue.add("charge.normal",
                    new RetriesAndDlqExample.PaymentWebhook("normal_" + i, "cust_" + i, 1000.0),
                    JobOptions.builder().attempts(3).exponentialBackoff(Duration.ofMillis(50)).build()
            );
        }

        // 3. Enqueue 10 Retry-Recovery Jobs
        for (int i = 0; i < retryRecoverCount; i++) {
            queue.add("charge.retry",
                    new RetriesAndDlqExample.PaymentWebhook("retry_recover_" + i, "cust_" + i, 2000.0),
                    JobOptions.builder().attempts(3).exponentialBackoff(Duration.ofMillis(50)).build()
            );
        }

        // 4. Enqueue 5 Permanent Fail Jobs
        for (int i = 0; i < permanentFailCount; i++) {
            queue.add("charge.fail",
                    new RetriesAndDlqExample.PaymentWebhook("perm_fail_" + i, "cust_" + i, 9999.0),
                    JobOptions.builder().attempts(3).exponentialBackoff(Duration.ofMillis(50)).build()
            );
        }

        // 5. Await completions & DLQ arrivals
        boolean completedDone = completedLatch.await(10, TimeUnit.SECONDS);
        boolean dlqDone = dlqLatch.await(10, TimeUnit.SECONDS);

        assertThat(completedDone).as("35 jobs should eventually succeed").isTrue();
        assertThat(dlqDone).as("5 jobs should exhaust retries and reach DLQ").isTrue();

        TimeUnit.MILLISECONDS.sleep(300);
        assertThat(queue.count(JobState.COMPLETED)).isEqualTo(normalCount + retryRecoverCount);
        assertThat(queue.count(JobState.FAILED)).isEqualTo(permanentFailCount);
        assertThat(queue.count(JobState.WAITING)).isEqualTo(0);
        assertThat(queue.count(JobState.ACTIVE)).isEqualTo(0);
    }
}
