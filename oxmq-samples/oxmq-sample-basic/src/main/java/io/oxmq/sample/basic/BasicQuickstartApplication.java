package io.oxmq.sample.basic;

import io.lettuce.core.RedisClient;
import io.oxmq.OxmqQueue;
import io.oxmq.OxmqWorker;
import io.oxmq.metrics.OxmqMetrics;
import io.oxmq.model.JobOptions;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal Setup Quickstart for OxMQ with Java 21 Virtual Threads and Micrometer telemetry.
 */
public class BasicQuickstartApplication {

    private static final Logger log = LoggerFactory.getLogger(BasicQuickstartApplication.class);

    public record EmailPayload(String to, String subject, String body, Instant sentAt) {}

    public static void main(String[] args) throws Exception {
        String redisUri = System.getProperty("oxmq.redis.uri", "redis://localhost:6379");
        log.info("Starting OxMQ Basic Quickstart using Redis at: {}", redisUri);

        RedisClient redisClient = RedisClient.create(redisUri);
        OxmqMetrics metrics = new OxmqMetrics();

        // 1. Create Producer Queue
        OxmqQueue<EmailPayload> emailQueue = OxmqQueue.<EmailPayload>builder()
                .name("email-notifications")
                .redisClient(redisClient)
                .metrics(metrics)
                .payloadClass(EmailPayload.class)
                .build();

        // 2. Create Virtual Thread Worker
        OxmqWorker<EmailPayload> worker = OxmqWorker.<EmailPayload>builder()
                .queueName("email-notifications")
                .redisClient(redisClient)
                .metrics(metrics)
                .payloadClass(EmailPayload.class)
                .concurrency(100) // 100 Virtual Threads!
                .processor(job -> {
                    EmailPayload email = job.getData();
                    log.info("Worker [{}] processing email to: {} [Subject: {}]", Thread.currentThread(), email.to(), email.subject());

                    job.updateProgress(25);
                    job.log("Connecting to SMTP server...");
                    Thread.sleep(100); // Simulated I/O call - carrier thread is not blocked!

                    job.updateProgress(75);
                    job.log("Dispatching email payload...");
                    Thread.sleep(100);

                    job.updateProgress(100);
                    job.log("Email delivered successfully.");
                    return "SENT_200_OK";
                })
                .build();

        // 3. Start worker
        worker.start();

        // 4. Produce sample jobs
        log.info("Enqueuing 5 sample jobs...");
        for (int i = 1; i <= 5; i++) {
            emailQueue.add("send-welcome-" + i,
                    new EmailPayload("user" + i + "@example.com", "Welcome to OxMQ #" + i, "Enjoy ultra-fast Virtual Thread job processing!", Instant.now()),
                    JobOptions.builder()
                            .attempts(3)
                            .exponentialBackoff(Duration.ofMillis(500))
                            .build()
            );
        }

        // Wait for worker to complete jobs
        TimeUnit.SECONDS.sleep(3);

        log.info("Done! Active jobs: {}, Waiting: {}, Completed count check",
                emailQueue.count(io.oxmq.model.JobState.ACTIVE),
                emailQueue.count(io.oxmq.model.JobState.WAITING));

        // Graceful shutdown
        worker.close();
        emailQueue.close();
        redisClient.shutdown();
        log.info("OxMQ Quickstart application finished successfully.");
    }
}
