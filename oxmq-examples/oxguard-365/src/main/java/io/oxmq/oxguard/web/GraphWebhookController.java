package io.oxmq.oxguard.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.oxmq.model.DeduplicationOptions;
import io.oxmq.model.JobOptions;
import io.oxmq.oxguard.model.DriveDeltaSyncPayload;
import io.oxmq.oxguard.model.ScanOrigin;
import io.oxmq.oxguard.queue.OxGuardQueueConstants;
import io.oxmq.spring.OxmqTemplate;
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
            @Value("${oxguard.webhook.client-state:oxguard-secret-state-token}") String expectedClientState
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

    @PostMapping(produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> handleNotification(
            @RequestParam(value = "validationToken", required = false) String validationToken,
            @RequestBody(required = false) String payload
    ) {
        // Validation handshake can also be sent via POST
        if (validationToken != null) {
            log.info("Microsoft Graph Webhook POST Validation Handshake successful");
            return ResponseEntity.ok(validationToken);
        }

        if (payload == null || payload.isBlank()) {
            return ResponseEntity.badRequest().body("Empty payload");
        }

        try {
            JsonNode root = objectMapper.readTree(payload);
            JsonNode values = root.get("value");
            if (values != null && values.isArray()) {
                for (JsonNode notification : values) {
                    String clientState = notification.has("clientState") ? notification.get("clientState").asText() : "";
                    if (!expectedClientState.equals(clientState) && !clientState.isEmpty()) {
                        log.warn("Invalid clientState token in Graph notification: {}", clientState);
                        continue;
                    }

                    String resource = notification.has("resource") ? notification.get("resource").asText() : "";
                    String tenantId = notification.has("tenantId") ? notification.get("tenantId").asText() : "contoso-enterprise-tenant";

                    // Extract driveId from resource path (e.g., "drives/{driveId}/root" or "/drives/{driveId}")
                    String driveId = extractDriveIdFromResource(resource);
                    if (driveId == null || driveId.isBlank()) {
                        driveId = "sp-drive-1"; // Fallback to primary drive
                    }

                    log.info("Received Microsoft Graph Webhook notification for drive [{}] (tenant={}). Dispatching Priority 1 Forward Scan...",
                            driveId, tenantId);

                    // Forward Scans: Priority 1 with 15s debounce deduplication
                    JobOptions options = JobOptions.builder()
                            .priority(1)
                            .attempts(3)
                            .deduplication(DeduplicationOptions.builder()
                                    .id("webhook-delta-" + driveId)
                                    .ttl(Duration.ofSeconds(15))
                                    .keepLastIfActive(true)
                                    .build())
                            .build();

                    DriveDeltaSyncPayload deltaPayload = new DriveDeltaSyncPayload(tenantId, driveId, ScanOrigin.FORWARD);

                    oxmqTemplate.send(
                            OxGuardQueueConstants.QUEUE_DRIVE_DELTA,
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
