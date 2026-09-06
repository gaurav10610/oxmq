package io.oxmq.example.ratelimit;

import io.lettuce.core.RedisClient;
import io.oxmq.OxmqQueue;
import io.oxmq.OxmqWorker;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import java.time.Duration;
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

class RateLimitingLoadIntegrationTest {

    private RedisClient redisClient;
    private OxmqQueue<RateLimitingExample.OpenAiPromptRequest> queue;
    private OxmqWorker<RateLimitingExample.OpenAiPromptRequest> worker;
    private String queueName;

    @BeforeEach
    void setUp() {
        queueName = "test-rate-limit-" + UUID.randomUUID().toString().substring(0, 8);
        redisClient = RedisClient.create("redis://localhost:6379");
        queue = OxmqQueue.<RateLimitingExample.OpenAiPromptRequest>builder()
                .name(queueName)
                .redisClient(redisClient)
                .payloadClass(RateLimitingExample.OpenAiPromptRequest.class)
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
    @DisplayName("Should strictly enforce sliding-window rate limit without dropping or delaying jobs indefinitely")
    void testSlidingWindowThrottling() throws InterruptedException {
        int totalJobs = 15;
        int maxPerWindow = 5;
        long windowDurationMs = 1000;

        CountDownLatch latch = new CountDownLatch(totalJobs);
        List<Long> executionTimestamps = Collections.synchronizedList(new ArrayList<>());

        // 1. Enqueue 15 rapid jobs
        for (int i = 0; i < totalJobs; i++) {
            queue.add("prompt-" + i,
                    new RateLimitingExample.OpenAiPromptRequest("req-" + i, "gpt-4o", "Query " + i),
                    JobOptions.defaults()
            );
        }

        // 2. Start Worker with 5 jobs per 1,000ms
        long startTime = System.currentTimeMillis();
        worker = OxmqWorker.<RateLimitingExample.OpenAiPromptRequest>builder()
                .queueName(queueName)
                .redisClient(redisClient)
                .payloadClass(RateLimitingExample.OpenAiPromptRequest.class)
                .concurrency(10)
                .pollIntervalMs(20)
                .rateLimit(maxPerWindow, Duration.ofMillis(windowDurationMs))
                .processor(job -> {
                    executionTimestamps.add(System.currentTimeMillis());
                    latch.countDown();
                    return "DONE";
                })
                .build();

        worker.start();

        // 3. Await completion
        boolean finished = latch.await(10, TimeUnit.SECONDS);
        assertThat(finished).as("All rate-limited jobs should finish").isTrue();
        assertThat(executionTimestamps).hasSize(totalJobs);

        // 4. Verify sliding window invariant: in any 1,000ms window, count <= 5
        Collections.sort(executionTimestamps);
        for (int i = 0; i < executionTimestamps.size(); i++) {
            long windowStart = executionTimestamps.get(i);
            long windowEnd = windowStart + windowDurationMs;
            long countInWindow = executionTimestamps.stream()
                    .filter(t -> t >= windowStart && t <= windowEnd)
                    .count();
            assertThat(countInWindow)
                    .as("No more than %d jobs should execute in any %d ms window", maxPerWindow, windowDurationMs)
                    .isLessThanOrEqualTo(maxPerWindow + 1); // +1 accounts for millisecond boundary rounding
        }

        // 5. Verify final queue state
        TimeUnit.MILLISECONDS.sleep(200);
        assertThat(queue.count(JobState.WAITING)).isEqualTo(0);
        assertThat(queue.count(JobState.COMPLETED)).isEqualTo(totalJobs);
    }
}
