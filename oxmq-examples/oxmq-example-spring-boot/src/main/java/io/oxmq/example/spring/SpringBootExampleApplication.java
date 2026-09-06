package io.oxmq.example.spring;

import io.lettuce.core.RedisClient;
import io.oxmq.OxmqQueue;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.spring.annotation.EnableOxmq;
import io.oxmq.spring.annotation.OxmqListener;
import java.time.Duration;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Real-World Showcase: Spring Boot 3 Declarative Webhook Dispatcher
 * REST microservice with @OxmqListener, rate limiting, and Actuator metrics export.
 */
@SpringBootApplication
@EnableOxmq
public class SpringBootExampleApplication {

    private static final Logger log = LoggerFactory.getLogger(SpringBootExampleApplication.class);

    public record WebhookNotification(String endpointUrl, String eventType, Map<String, Object> data) {}

    public static void main(String[] args) {
        SpringApplication.run(SpringBootExampleApplication.class, args);
    }

    @Bean
    public OxmqQueue<WebhookNotification> webhookQueue(RedisClient redisClient) {
        return OxmqQueue.<WebhookNotification>builder()
                .name("outgoing-webhooks")
                .redisClient(redisClient)
                .payloadClass(WebhookNotification.class)
                .build();
    }

    @RestController
    @RequestMapping("/api/webhooks")
    public static class WebhookApiController {

        private final OxmqQueue<WebhookNotification> queue;

        public WebhookApiController(OxmqQueue<WebhookNotification> queue) {
            this.queue = queue;
        }

        @PostMapping("/dispatch")
        public Map<String, Object> dispatch(@RequestBody WebhookNotification payload,
                                            @RequestParam(defaultValue = "0") long delayMs) {
            Job<WebhookNotification> job = queue.add("dispatch-webhook", payload,
                    JobOptions.builder()
                            .delay(Duration.ofMillis(delayMs))
                            .attempts(3)
                            .exponentialBackoff(Duration.ofSeconds(1))
                            .build()
            );

            return Map.of(
                    "status", "ENQUEUED",
                    "jobId", job.getId(),
                    "queue", queue.getName()
            );
        }

        @GetMapping("/stats")
        public Map<String, Object> getQueueStats() {
            return Map.of(
                    "waiting", queue.count(io.oxmq.model.JobState.WAITING),
                    "active", queue.count(io.oxmq.model.JobState.ACTIVE),
                    "delayed", queue.count(io.oxmq.model.JobState.DELAYED),
                    "completed", queue.count(io.oxmq.model.JobState.COMPLETED),
                    "failed", queue.count(io.oxmq.model.JobState.FAILED)
            );
        }
    }

    @RestController
    public static class WebhookWorkerComponent {

        @OxmqListener(queue = "outgoing-webhooks", concurrency = 50, rateLimitMax = 100, rateLimitDurationMs = 60000)
        public String processWebhook(Job<WebhookNotification> job) throws InterruptedException {
            WebhookNotification webhook = job.getData();
            log.info("Processing webhook [jobId: {}] to URL: {} (VirtualThread: {})",
                    job.getId(), webhook.endpointUrl(), Thread.currentThread().isVirtual());

            job.updateProgress(50);
            Thread.sleep(50); // Simulating HTTP webhook dispatch

            job.updateProgress(100);
            job.log("Webhook delivered successfully to " + webhook.endpointUrl());
            return "HTTP_200_OK";
        }
    }
}
