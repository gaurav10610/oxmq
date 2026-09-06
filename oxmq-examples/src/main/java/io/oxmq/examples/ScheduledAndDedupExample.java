package io.oxmq.examples;

import io.lettuce.core.RedisClient;
import io.oxmq.OxmqQueue;
import io.oxmq.OxmqWorker;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real-World Recipe: Scheduled Delays &amp; Deduplication
 * Use Case: Abandoned cart reminders with delay + Idempotent order processing preventing duplicate executions.
 */
public class ScheduledAndDedupExample {

    private static final Logger log = LoggerFactory.getLogger(ScheduledAndDedupExample.class);

    public record CartReminder(String cartId, String customerEmail, double cartValue) {}

    public static void main(String[] args) throws Exception {
        String redisUri = System.getProperty("oxmq.redis.uri", "redis://localhost:6379");
        RedisClient redisClient = RedisClient.create(redisUri);

        String queueName = "cart-reminders";

        OxmqQueue<CartReminder> queue = OxmqQueue.<CartReminder>builder()
                .name(queueName)
                .redisClient(redisClient)
                .payloadClass(CartReminder.class)
                .build();

        OxmqWorker<CartReminder> worker = OxmqWorker.<CartReminder>builder()
                .queueName(queueName)
                .redisClient(redisClient)
                .payloadClass(CartReminder.class)
                .concurrency(10)
                .processor(job -> {
                    CartReminder cart = job.getData();
                    log.info("Executing scheduled reminder for cart {} ({}) [Delayed by {} ms]",
                            cart.cartId(), cart.customerEmail(), job.getOpts().getDelayMs());
                    return "REMINDER_SENT";
                })
                .build();

        worker.start();

        // 1. Scheduled Job with 2-second delay
        log.info("Enqueuing scheduled job with 2000ms delay...");
        queue.add("send-cart-reminder",
                new CartReminder("cart_8801", "alice@example.com", 149.00),
                JobOptions.builder().delay(Duration.ofSeconds(2)).build()
        );

        // 2. Deduplication Test: Submit the same custom jobId twice
        String dedupJobId = "order_confirmation_order_9901";
        log.info("Submitting first job with custom jobId: {}", dedupJobId);
        Job<CartReminder> job1 = queue.add("order-confirm",
                new CartReminder("cart_9901", "bob@example.com", 299.00),
                JobOptions.builder().jobId(dedupJobId).build()
        );

        log.info("Submitting duplicate job with same jobId: {}", dedupJobId);
        Job<CartReminder> job2 = queue.add("order-confirm",
                new CartReminder("cart_9901", "bob@example.com", 299.00),
                JobOptions.builder().jobId(dedupJobId).build()
        );

        log.info("Deduplication check: job1 ID = {}, job2 ID = {}", job1.getId(), job2.getId());

        // Wait for execution
        TimeUnit.SECONDS.sleep(4);

        worker.close();
        queue.close();
        redisClient.shutdown();
        log.info("Scheduled & Deduplication recipe finished.");
    }
}
