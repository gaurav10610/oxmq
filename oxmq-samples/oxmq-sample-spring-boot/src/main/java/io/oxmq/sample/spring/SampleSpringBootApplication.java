package io.oxmq.sample.spring;

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

@SpringBootApplication
@EnableOxmq
public class SampleSpringBootApplication {

    private static final Logger log = LoggerFactory.getLogger(SampleSpringBootApplication.class);

    public record WebhookRequest(String url, String eventType, Map<String, Object> payload) {}

    public static void main(String[] args) {
        SpringApplication.run(SampleSpringBootApplication.class, args);
    }

    @Bean
    public OxmqQueue<WebhookRequest> webhookQueue(RedisClient redisClient) {
        return OxmqQueue.<WebhookRequest>builder()
                .name("webhooks")
                .redisClient(redisClient)
                .payloadClass(WebhookRequest.class)
                .build();
    }

    @RestController
    @RequestMapping("/api/webhooks")
    public static class WebhookController {

        private final OxmqQueue<WebhookRequest> queue;

        public WebhookController(OxmqQueue<WebhookRequest> queue) {
            this.queue = queue;
        }

        @PostMapping("/dispatch")
        public Map<String, Object> dispatch(@RequestBody WebhookRequest request,
                                            @RequestParam(defaultValue = "0") long delayMs) {
            Job<WebhookRequest> job = queue.add("dispatch-event", request,
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
        public Map<String, Object> getStats() {
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

        @OxmqListener(queue = "webhooks", concurrency = 50, rateLimitMax = 100, rateLimitDurationMs = 60000)
        public String handleWebhook(Job<WebhookRequest> job) throws InterruptedException {
            WebhookRequest req = job.getData();
            log.info("Processing webhook [jobId: {}] for URL: {} (VirtualThread: {})",
                    job.getId(), req.url(), Thread.currentThread().isVirtual());

            job.updateProgress(50);
            Thread.sleep(50); // Simulating HTTP webhook dispatch

            job.updateProgress(100);
            job.log("Dispatched webhook successfully to " + req.url());
            return "HTTP_200_OK";
        }
    }
}
