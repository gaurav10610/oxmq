package io.oxmq.example.java17.controller;

import io.oxmq.example.java17.model.AuditLogEvent;
import io.oxmq.example.java17.model.InvoiceRequest;
import io.oxmq.example.java17.model.NotificationEvent;
import io.oxmq.model.Job;
import io.oxmq.spring.OxmqTemplate;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller demonstrating production usage of {@link OxmqTemplate}.
 */
@RestController
@RequestMapping("/api")
public class InvoiceController {

    private final OxmqTemplate oxmqTemplate;

    @Value("${app.queues.invoices:java17-invoices}")
    private String invoiceQueue;

    @Value("${app.queues.audit:java17-audit-logs}")
    private String auditQueue;

    @Value("${app.queues.notifications:java17-notifications}")
    private String notificationQueue;

    public InvoiceController(OxmqTemplate oxmqTemplate) {
        this.oxmqTemplate = oxmqTemplate;
    }

    @PostMapping("/invoices")
    public ResponseEntity<Map<String, Object>> submitInvoice(@RequestBody InvoiceRequest request) {
        Job<InvoiceRequest> job = oxmqTemplate.send(invoiceQueue, "process-invoice", request);
        return ResponseEntity.ok(Map.of(
                "jobId", job.getId(),
                "queue", invoiceQueue,
                "invoiceId", request.invoiceId(),
                "status", "QUEUED"
        ));
    }

    @PostMapping("/audit/bulk")
    public ResponseEntity<Map<String, Object>> submitAuditBatch(@RequestBody List<AuditLogEvent> events) {
        for (AuditLogEvent event : events) {
            oxmqTemplate.send(auditQueue, "audit-entry", event);
        }
        return ResponseEntity.ok(Map.of(
                "count", events.size(),
                "queue", auditQueue,
                "status", "SUBMITTED"
        ));
    }

    @PostMapping("/notifications")
    public ResponseEntity<Map<String, Object>> sendNotification(
            @RequestParam String type,
            @RequestBody NotificationEvent event
    ) {
        Job<NotificationEvent> job = oxmqTemplate.send(notificationQueue, type, event);
        return ResponseEntity.ok(Map.of(
                "jobId", job.getId(),
                "channel", type,
                "recipient", event.recipient()
        ));
    }

    @PostMapping("/queues/{queueName}/pause")
    public ResponseEntity<Map<String, String>> pauseQueue(@PathVariable String queueName) {
        oxmqTemplate.pause(queueName);
        return ResponseEntity.ok(Map.of("queue", queueName, "state", "PAUSED"));
    }

    @PostMapping("/queues/{queueName}/resume")
    public ResponseEntity<Map<String, String>> resumeQueue(@PathVariable String queueName) {
        oxmqTemplate.resume(queueName);
        return ResponseEntity.ok(Map.of("queue", queueName, "state", "ACTIVE"));
    }

    @GetMapping("/jobs/{queueName}/{jobId}")
    public ResponseEntity<?> getJob(@PathVariable String queueName, @PathVariable String jobId) {
        Job<?> job = oxmqTemplate.getJob(queueName, jobId);
        return job != null ? ResponseEntity.ok(job) : ResponseEntity.notFound().build();
    }
}
