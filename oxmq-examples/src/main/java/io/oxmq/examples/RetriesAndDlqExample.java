package io.oxmq.examples;

import io.lettuce.core.RedisClient;
import io.oxmq.OxmqQueue;
import io.oxmq.OxmqWorker;
import io.oxmq.model.JobOptions;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real-World Recipe: Automatic Retries, Exponential Backoff &amp; Dead-Letter Queue (DLQ)
 * Use Case: Critical payment webhook dispatch handling transient 503 errors and archiving permanent failures.
 */
public class RetriesAndDlqExample {

    private static final Logger log = LoggerFactory.getLogger(RetriesAndDlqExample.class);

    public record PaymentWebhook(String eventId, String customerId, double amountCents) {}

    public static void main(String[] args) throws Exception {
        String redisUri = System.getProperty("oxmq.redis.uri", "redis://localhost:6379");
        RedisClient redisClient = RedisClient.create(redisUri);

        String queueName = "payment-webhooks";

        OxmqQueue<PaymentWebhook> queue = OxmqQueue.<PaymentWebhook>builder()
                .name(queueName)
                .redisClient(redisClient)
                .payloadClass(PaymentWebhook.class)
                .build();

        AtomicInteger attemptCounter = new AtomicInteger(0);

        OxmqWorker<PaymentWebhook> worker = OxmqWorker.<PaymentWebhook>builder()
                .queueName(queueName)
                .redisClient(redisClient)
                .payloadClass(PaymentWebhook.class)
                .concurrency(10)
                .processor(job -> {
                    PaymentWebhook payment = job.getData();
                    int currentAttempt = attemptCounter.incrementAndGet();

                    log.warn("Processing payment webhook {} (Attempt #{})", payment.eventId(), currentAttempt);

                    // Simulate transient downstream outage on first 2 attempts
                    if (currentAttempt <= 2) {
                        job.log("Failed to connect to gateway: HTTP 503 Service Unavailable");
                        throw new RuntimeException("Downstream Payment Gateway Timeout (HTTP 503)");
                    }

                    job.log("Gateway returned HTTP 200 OK");
                    log.info("Payment webhook {} succeeded on attempt #{}!", payment.eventId(), currentAttempt);
                    return "PAYMENT_CONFIRMED";
                })
                .build();

        worker.start();

        // Enqueue job with 3 max attempts and exponential backoff starting at 500ms
        queue.add("charge.succeeded",
                new PaymentWebhook("evt_9981", "cust_alice", 4999.0),
                JobOptions.builder()
                        .attempts(3)
                        .exponentialBackoff(Duration.ofMillis(500), Duration.ofSeconds(5))
                        .build()
        );

        // Allow retry sequence to complete
        TimeUnit.SECONDS.sleep(5);

        worker.close();
        queue.close();
        redisClient.shutdown();
        log.info("Retries and DLQ recipe completed.");
    }
}
