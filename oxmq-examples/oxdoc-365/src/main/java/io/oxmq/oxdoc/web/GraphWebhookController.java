package io.oxmq.oxdoc.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.oxmq.model.BackoffStrategy;
import io.oxmq.model.DeduplicationOptions;
import io.oxmq.model.JobOptions;
import io.oxmq.oxdoc.model.DriveDeltaSyncPayload;
import io.oxmq.oxdoc.model.ScanOrigin;
import io.oxmq.oxdoc.queue.OxDocQueueConstants;
import io.oxmq.spring.OxmqTemplate;
import io.oxmq.tracing.TraceContext;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Ingestion controller for Microsoft Graph Webhooks.
 * Handles validation token handshakes and converts invalidation pings into Priority 1 Forward Scan jobs.
 */
@RestController
@RequestMapping("/api/v1/webhooks/graph")
public class GraphWebhookController {

    private static final Logger log = LoggerFactory.getLogger(GraphWebhookController.class);

    private final OxmqTemplate oxmqTemplate;
    private final ObjectMapper objectMapper;
    private final String expectedClientState;

    public GraphWebhookController(
            OxmqTemplate oxmqTemplate,
            ObjectMapper objectMapper,
            @Value("${oxdoc.webhook.client-state:oxdoc-secret-state-token}") String expectedClientState
    ) {
        this.oxmqTemplate = oxmqTemplate;
        this.objectMapper = objectMapper;
        this.expectedClientState = expectedClientState;
    }

    /**
     * Microsoft Graph Webhook Lifecycle Handshake.
     * When registering a subscription, Graph sends a validation request with `validationToken`.
     * The endpoint must reply within 10s with HTTP 200 and the plain-text token.
     */
    @GetMapping(produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> validateGet(@RequestParam(value = "validationToken", required = false) String validationToken) {
        if (validationToken != null) {
            log.info("Microsoft Graph Webhook GET Validation Handshake successful");
            return ResponseEntity.ok(validationToken);
        }
        return ResponseEntity.badRequest().body("Missing validationToken parameter");
    }

    /**
     * Ingestion endpoint receiving change notifications for SharePoint Libraries & OneDrives.
     * Decouples synchronous HTTP delivery by immediately enqueuing DriveDeltaSyncPayload at Priority 1.
     */
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> handleNotification(@RequestBody String payloadString) {
        log.info("Received Graph Webhook Notification: {}", payloadString);

        try {
            JsonNode root = objectMapper.readTree(payloadString);
            JsonNode valueArray = root.get("value");
            if (valueArray == null || !valueArray.isArray()) {
                return ResponseEntity.badRequest().body("Missing 'value' array in Graph notification");
            }

            for (JsonNode item : valueArray) {
                // Verify clientState secret token to prevent spoofing
                String clientState = item.has("clientState") ? item.get("clientState").asText() : "";
                if (!expectedClientState.equals(clientState)) {
                    log.warn("Unauthorized Graph Webhook ping: clientState [{}] did not match expected secret", clientState);
                    continue;
                }

                String tenantId = item.has("tenantId") ? item.get("tenantId").asText() : "tenant-mid-2500";
                String resource = item.has("resource") ? item.get("resource").asText() : "";
                String driveId = extractDriveIdFromResource(resource);

                if (driveId != null) {
                    log.info("Dispatching Priority 1 Forward Sync for Drive [{}] (Tenant: {})", driveId, tenantId);

                    TraceContext traceCtx = TraceContext.fromMdc() != null ? TraceContext.fromMdc().createChildSpan() : TraceContext.generateNew();

                    // Forward Scans: Priority 1 with 15s debounce deduplication
                    JobOptions options = JobOptions.builder()
                            .priority(1)
                            .attempts(3)
                            .backoff(new BackoffStrategy.Exponential(1000L))
                            .traceparent(traceCtx.asTraceparent())
                            .deduplication(DeduplicationOptions.builder()
                                    .id("webhook-delta-" + driveId)
                                    .ttl(Duration.ofSeconds(15))
                                    .keepLastIfActive(true)
                                    .build())
                            .build();

                    DriveDeltaSyncPayload deltaPayload = new DriveDeltaSyncPayload(tenantId, driveId, ScanOrigin.FORWARD);

                    oxmqTemplate.send(
                            OxDocQueueConstants.QUEUE_DRIVE_DELTA,
                            "forward-sync-" + driveId,
                            deltaPayload,
                            options
                    );
                }
            }
        } catch (Exception e) {
            log.error("Failed processing Graph Webhook notification payload: {}", e.getMessage(), e);
            return ResponseEntity.badRequest().body("Invalid notification JSON");
        }

        return ResponseEntity.accepted().body("Notifications accepted");
    }

    private String extractDriveIdFromResource(String resource) {
        if (resource == null) return null;
        if (resource.contains("drives/")) {
            String[] parts = resource.split("drives/");
            if (parts.length > 1) {
                String sub = parts[1];
                int slash = sub.indexOf('/');
                return (slash != -1) ? sub.substring(0, slash) : sub;
            }
        }
        return null;
    }
}
