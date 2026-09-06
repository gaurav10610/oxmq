package io.oxmq.example.ratelimit;

import io.lettuce.core.RedisClient;
import io.oxmq.OxmqQueue;
import io.oxmq.OxmqWorker;
import io.oxmq.model.JobOptions;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real-World Showcase: OpenAI / Stripe API Rate Limiting.
 * Protects downstream APIs by strictly enforcing sliding-window rate limits (max 5 requests per 2 seconds).
 */
public class RateLimitingExample {

    private static final Logger log = LoggerFactory.getLogger(RateLimitingExample.class);

    public record OpenAiPromptRequest(String promptId, String model, String userQuery) {}

    public static void main(String[] args) throws Exception {
        String redisUri = System.getProperty("oxmq.redis.uri", "redis://localhost:6379");
        RedisClient redisClient = RedisClient.create(redisUri);

        String queueName = "openai-generation-requests";

        // 1. Producer Queue
        OxmqQueue<OpenAiPromptRequest> queue = OxmqQueue.<OpenAiPromptRequest>builder()
                .name(queueName)
                .redisClient(redisClient)
                .payloadClass(OpenAiPromptRequest.class)
                .build();

        // 2. Rate-Limited Worker (Max 5 requests per 2 seconds)
        OxmqWorker<OpenAiPromptRequest> worker = OxmqWorker.<OpenAiPromptRequest>builder()
                .queueName(queueName)
                .redisClient(redisClient)
                .payloadClass(OpenAiPromptRequest.class)
                .concurrency(20) // 20 Virtual Threads
                .rateLimit(5, Duration.ofSeconds(2)) // Strict sliding window token bucket
                .processor(job -> {
                    OpenAiPromptRequest req = job.getData();
                    log.info("[{}] Dispatching prompt to OpenAI: '{}' (Virtual Thread: {})",
                            Instant.now(), req.userQuery(), Thread.currentThread().isVirtual());

                    Thread.sleep(100); // Simulate API latency
                    return "COMPLETED_PROMPT_" + req.promptId();
                })
                .build();

        worker.start();

        // 3. Enqueue 15 rapid jobs
        log.info("Enqueuing 15 prompts simultaneously...");
        for (int i = 1; i <= 15; i++) {
            queue.add("generate-summary-" + i,
                    new OpenAiPromptRequest("prompt-" + i, "gpt-4o", "Summarize article #" + i),
                    JobOptions.defaults()
            );
        }

        // Wait to observe rate-limited execution
        TimeUnit.SECONDS.sleep(8);

        worker.close();
        queue.close();
        redisClient.shutdown();
        log.info("Rate limiting example finished successfully.");
    }
}
