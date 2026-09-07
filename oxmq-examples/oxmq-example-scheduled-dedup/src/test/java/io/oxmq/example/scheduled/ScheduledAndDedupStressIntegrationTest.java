package io.oxmq.example.scheduled;

import io.lettuce.core.RedisClient;
import io.oxmq.OxmqQueue;
import io.oxmq.OxmqWorker;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ScheduledAndDedupStressIntegrationTest {

    private RedisClient redisClient;
    private OxmqQueue<ScheduledAndDedupExample.CartReminder> queue;
    private OxmqWorker<ScheduledAndDedupExample.CartReminder> worker;
    private String queueName;

    @BeforeEach
    void setUp() {
        queueName = "test-sched-dedup-" + UUID.randomUUID().toString().substring(0, 8);
        redisClient = RedisClient.create("redis://localhost:6379");
        queue = OxmqQueue.<ScheduledAndDedupExample.CartReminder>builder()
                .name(queueName)
                .redisClient(redisClient)
                .payloadClass(ScheduledAndDedupExample.CartReminder.class)
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
    @DisplayName("20 concurrent threads submitting identical jobId should yield exactly 1 active/waiting job (idempotent)")
    void testConcurrentDeduplicationBlitz() throws InterruptedException {
        int threadCount = 20;
        String sharedJobId = "dedup_order_target_8888";
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threadCount);
        List<String> returnedJobIds = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    Job<ScheduledAndDedupExample.CartReminder> job = queue.add("order-job",
                            new ScheduledAndDedupExample.CartReminder("cart_" + index, "user@test.com", 100.0),
                            JobOptions.builder().jobId(sharedJobId).build()
                    );
                    returnedJobIds.add(job.getId());
                } catch (Exception e) {
                    // Ignore
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        // Trigger all threads simultaneously
        startLatch.countDown();
        boolean finished = finishLatch.await(5, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(finished).isTrue();
        assertThat(returnedJobIds).hasSize(threadCount);
        for (String id : returnedJobIds) {
            assertThat(id).isEqualTo(sharedJobId);
        }

        // Only 1 job must exist in Redis
        assertThat(queue.count(JobState.WAITING)).isEqualTo(1);
    }

    @Test
    @DisplayName("Delayed jobs should mature and execute accurately after their delay threshold")
    void testSubSecondDelayedExecution() throws InterruptedException {
        int jobCount = 3;
        CountDownLatch latch = new CountDownLatch(jobCount);
        List<Long> completionDelays = Collections.synchronizedList(new ArrayList<>());
        long startTime = System.currentTimeMillis();

        worker = OxmqWorker.<ScheduledAndDedupExample.CartReminder>builder()
                .queueName(queueName)
                .redisClient(redisClient)
                .payloadClass(ScheduledAndDedupExample.CartReminder.class)
                .concurrency(5)
                .pollIntervalMs(20)
                .processor(job -> {
                    long delayElapsed = System.currentTimeMillis() - startTime;
                    completionDelays.add(delayElapsed);
                    latch.countDown();
                    return "DONE";
                })
                .build();

        worker.start();

        // Enqueue 3 jobs with delays: 200ms, 400ms, 600ms
        queue.add("delay-200", new ScheduledAndDedupExample.CartReminder("c1", "u1", 10.0),
                JobOptions.builder().delay(Duration.ofMillis(200)).build());
        queue.add("delay-400", new ScheduledAndDedupExample.CartReminder("c2", "u2", 20.0),
                JobOptions.builder().delay(Duration.ofMillis(400)).build());
        queue.add("delay-600", new ScheduledAndDedupExample.CartReminder("c3", "u3", 30.0),
                JobOptions.builder().delay(Duration.ofMillis(600)).build());

        boolean allDone = latch.await(5, TimeUnit.SECONDS);
        assertThat(allDone).as("All 3 delayed jobs should complete").isTrue();

        assertThat(completionDelays).hasSize(3);
        assertThat(completionDelays.get(0)).isGreaterThanOrEqualTo(180);
        assertThat(completionDelays.get(1)).isGreaterThanOrEqualTo(380);
        assertThat(completionDelays.get(2)).isGreaterThanOrEqualTo(580);
    }
}
