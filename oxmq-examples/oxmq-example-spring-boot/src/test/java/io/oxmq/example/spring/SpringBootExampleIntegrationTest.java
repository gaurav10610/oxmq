package io.oxmq.example.spring;

import io.oxmq.OxmqQueue;
import io.oxmq.model.JobState;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SpringBootExampleIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private OxmqQueue<SpringBootExampleApplication.WebhookNotification> webhookQueue;

    @Test
    @DisplayName("Should dispatch webhook via REST endpoint and have @OxmqListener consume it")
    void testWebhookDispatchAndProcessing() throws InterruptedException {
        String url = "http://localhost:" + port + "/api/webhooks/dispatch";

        var payload = new SpringBootExampleApplication.WebhookNotification(
                "https://api.merchant.com/webhook",
                "payment.succeeded",
                Map.of("amount", 9900, "currency", "USD")
        );

        ResponseEntity<Map> response = restTemplate.postForEntity(url, payload, Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().get("status")).isEqualTo("ENQUEUED");
        String jobId = (String) response.getBody().get("jobId");
        assertThat(jobId).isNotBlank();

        // Wait for @OxmqListener Virtual Thread worker to process
        long start = System.currentTimeMillis();
        boolean processed = false;
        while (System.currentTimeMillis() - start < 5000) {
            long completed = webhookQueue.count(JobState.COMPLETED);
            if (completed >= 1) {
                processed = true;
                break;
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }

        assertThat(processed).isTrue();

        // Verify stats endpoint
        ResponseEntity<Map> statsResp = restTemplate.getForEntity("http://localhost:" + port + "/api/webhooks/stats", Map.class);
        assertThat(statsResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(statsResp.getBody()).isNotNull();
        assertThat(((Number) statsResp.getBody().get("completed")).longValue()).isGreaterThanOrEqualTo(1);

        // Verify Actuator health endpoint
        ResponseEntity<Map> healthResp = restTemplate.getForEntity("http://localhost:" + port + "/actuator/health", Map.class);
        assertThat(healthResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(healthResp.getBody()).isNotNull();
        assertThat(healthResp.getBody().get("status")).isEqualTo("UP");
    }
}
