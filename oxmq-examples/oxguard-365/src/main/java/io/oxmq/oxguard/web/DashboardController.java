package io.oxmq.oxguard.web;

import io.oxmq.model.DeduplicationOptions;
import io.oxmq.model.JobOptions;
import io.oxmq.oxguard.connector.GraphClient;
import io.oxmq.oxguard.connector.MockGraphClient;
import io.oxmq.oxguard.model.DriveDeltaSyncPayload;
import io.oxmq.oxguard.model.DriveItem;
import io.oxmq.oxguard.model.ExposureLevel;
import io.oxmq.oxguard.model.ScanOrigin;
import io.oxmq.oxguard.model.ScannedFileRecord;
import io.oxmq.oxguard.model.TenantDiscoveryPayload;
import io.oxmq.oxguard.queue.OxGuardQueueConstants;
import io.oxmq.oxguard.service.FileInventoryRepository;
import io.oxmq.oxguard.service.LiveMetricsService;
import io.oxmq.spring.OxmqTemplate;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * REST API Controller for OxGuard 365 DSPM Dashboard and real-time operations.
 */
@RestController
@RequestMapping("/api/v1/dashboard")
public class DashboardController {

    private static final Logger log = LoggerFactory.getLogger(DashboardController.class);

    private final FileInventoryRepository repository;
    private final LiveMetricsService metricsService;
    private final OxmqTemplate oxmqTemplate;
    private final io.oxmq.oxguard.service.TenantCooldownService cooldownService;

    @Autowired(required = false)
    private MockGraphClient mockGraphClient;

    @Autowired(required = false)
    private io.oxmq.oxguard.mockserver.MockOffice365Server mockOffice365Server;

    public DashboardController(
            FileInventoryRepository repository,
            LiveMetricsService metricsService,
            OxmqTemplate oxmqTemplate,
            io.oxmq.oxguard.service.TenantCooldownService cooldownService
    ) {
        this.repository = repository;
        this.metricsService = metricsService;
        this.oxmqTemplate = oxmqTemplate;
        this.cooldownService = cooldownService;
    }

    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> getMetrics() {
        return ResponseEntity.ok(repository.getMetrics());
    }

    @GetMapping("/hierarchy")
    public ResponseEntity<Map<String, Object>> getHierarchy() {
        return ResponseEntity.ok(repository.getHierarchy());
    }

    @GetMapping("/quota")
    public ResponseEntity<Map<String, Object>> getQuotaTelemetry(
            @RequestParam(value = "tenantId", defaultValue = "contoso-enterprise-tenant") String tenantId
    ) {
        return ResponseEntity.ok(cooldownService.getQuotaTelemetry(tenantId));
    }

    @PostMapping("/ratelimit")
    public ResponseEntity<Map<String, Object>> updateRateLimit(
            @RequestParam(value = "limit", defaultValue = "50") int limit
    ) {
        cooldownService.setMaxRequestsPerSecond(limit);
        if (mockOffice365Server != null) {
            mockOffice365Server.setRateLimit(limit);
        }
        return ResponseEntity.ok(Map.of(
                "status", "UPDATED",
                "maxRequestsPerSecond", limit,
                "message", "Rate limit adjusted to " + limit + " req/s"
        ));
    }

    @PostMapping("/simulate-429")
    public ResponseEntity<Map<String, Object>> simulate429Burst(
            @RequestParam(value = "enabled", defaultValue = "true") boolean enabled,
            @RequestParam(value = "burstCount", defaultValue = "20") int burstCount
    ) {
        if (mockOffice365Server != null) {
            mockOffice365Server.trigger429Burst(burstCount);
            mockOffice365Server.setSimulate429Burst(enabled);
            return ResponseEntity.ok(Map.of(
                    "status", "TRIGGERED",
                    "burstCount", burstCount,
                    "enabled", enabled,
                    "message", "Triggered " + burstCount + " HTTP 429 burst responses from Mock M365 Server"
            ));
        }
        return ResponseEntity.badRequest().body(Map.of("error", "MockOffice365Server not active"));
    }

    @GetMapping("/files")
    public ResponseEntity<List<ScannedFileRecord>> getFiles(
            @RequestParam(value = "exposure", required = false) String exposure,
            @RequestParam(value = "threat", required = false) String threat,
            @RequestParam(value = "origin", required = false) String origin
    ) {
        List<ScannedFileRecord> files = repository.getAllFiles();

        if (exposure != null && !exposure.isBlank()) {
            files = files.stream()
                    .filter(f -> f.exposureLevel().name().equalsIgnoreCase(exposure))
                    .toList();
        }

        if (threat != null && !threat.isBlank()) {
            files = files.stream()
                    .filter(f -> f.threatVerdict().name().equalsIgnoreCase(threat))
                    .toList();
        }

        if (origin != null && !origin.isBlank()) {
            files = files.stream()
                    .filter(f -> f.scanOrigin().name().equalsIgnoreCase(origin))
                    .toList();
        }

        return ResponseEntity.ok(files);
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamEvents() {
        return metricsService.subscribe();
    }

    @PostMapping("/seed")
    public ResponseEntity<Map<String, Object>> seedAdditionalFiles(
            @RequestParam(value = "count", defaultValue = "1000") int count
    ) {
        if (mockOffice365Server != null) {
            mockOffice365Server.seedAdditionalFiles(count);
            return ResponseEntity.ok(Map.of(
                    "status", "SEEDED",
                    "totalFiles", mockOffice365Server.getTotalFiles(),
                    "message", "Successfully seeded files up to " + count
            ));
        }
        return ResponseEntity.badRequest().body(Map.of("error", "MockOffice365Server is not active"));
    }

    @PostMapping("/trigger/reset")
    public ResponseEntity<Map<String, Object>> resetDatabase() {
        log.info("Resetting scanned files database and metrics");
        repository.clear();
        return ResponseEntity.ok(Map.of(
                "status", "RESET",
                "message", "PostgreSQL database inventory reset successfully"
        ));
    }

    /**
     * Triggers a historical backward baseline crawl across all tenant document libraries.
     */
    @PostMapping("/trigger/crawl")
    public ResponseEntity<Map<String, Object>> triggerBaselineCrawl(
            @RequestParam(value = "tenantId", defaultValue = "contoso-enterprise-tenant") String tenantId
    ) {
        log.info("Manually triggering tenant discovery and backward crawl for [{}]", tenantId);

        JobOptions options = JobOptions.builder()
                .priority(5) // Discovery itself runs at normal priority 5, dispatches delta jobs at 10
                .attempts(3)
                .build();

        oxmqTemplate.send(
                OxGuardQueueConstants.QUEUE_TENANT_DISCOVERY,
                "discovery-" + tenantId + "-" + System.currentTimeMillis(),
                new TenantDiscoveryPayload(tenantId),
                options
        );

        return ResponseEntity.ok(Map.of(
                "status", "DISPATCHED",
                "message", "Baseline historical discovery job queued for tenant: " + tenantId,
                "timestamp", System.currentTimeMillis()
        ));
    }

    /**
     * Simulates a real-time Forward upload (Priority 1) with auto-save debouncing.
     */
    @PostMapping("/trigger/upload")
    public ResponseEntity<Map<String, Object>> simulateForwardUpload(
            @RequestParam(value = "driveId", defaultValue = "sp-drive-finance") String driveId,
            @RequestParam(value = "fileName", defaultValue = "Quarterly_Board_Deck_Draft.pptx") String fileName,
            @RequestParam(value = "exposure", defaultValue = "PUBLIC_ANONYMOUS") String exposure,
            @RequestParam(value = "threatType", defaultValue = "CLEAN") String threatType
    ) {
        log.info("Simulating real-time forward upload: file=[{}], drive=[{}], exposure=[{}], threatType=[{}]",
                fileName, driveId, exposure, threatType);

        byte[] fileBytes;
        if ("EICAR".equalsIgnoreCase(threatType)) {
            fileBytes = MockGraphClient.EICAR_TEST_STRING.getBytes(StandardCharsets.UTF_8);
            fileName = "eicar_infected_" + fileName;
        } else if ("ENTROPY".equalsIgnoreCase(threatType)) {
            byte[] highEntropy = new byte[8192];
            new java.util.Random().nextBytes(highEntropy);
            fileBytes = highEntropy;
            fileName = "ransomware_encrypted_" + fileName;
        } else if ("MIME_MISMATCH".equalsIgnoreCase(threatType)) {
            // PE Executable MZ header disguised as Office Document
            fileBytes = new byte[4096];
            fileBytes[0] = 0x4D;
            fileBytes[1] = 0x5A;
            fileName = "fake_invoice_" + (fileName.endsWith(".docx") ? fileName : fileName + ".docx");
        } else {
            fileBytes = ("Enterprise presentation deck generated at: " + System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8);
        }

        ExposureLevel exposureLevel;
        try {
            exposureLevel = ExposureLevel.valueOf(exposure.toUpperCase());
        } catch (Exception e) {
            exposureLevel = ExposureLevel.PUBLIC_ANONYMOUS;
        }

        if (mockGraphClient != null) {
            mockGraphClient.addMockForwardFile(driveId, fileName, exposureLevel, fileBytes);
        }
        if (mockOffice365Server != null) {
            String scope = "anonymous";
            if (exposureLevel == ExposureLevel.EXTERNAL_GUEST) scope = "users";
            else if (exposureLevel == ExposureLevel.ORGANIZATION_WIDE) scope = "organization";
            else if (exposureLevel == ExposureLevel.PRIVATE) scope = "private";
            mockOffice365Server.saveForwardFile(driveId, fileName, scope, fileBytes, "application/octet-stream");
        }

        // Dispatch Priority 1 Forward Scan job with 15s burst debounce
        JobOptions options = JobOptions.builder()
                .priority(1)
                .attempts(3)
                .deduplication(DeduplicationOptions.builder()
                        .id("forward-upload-" + driveId)
                        .ttl(Duration.ofSeconds(15))
                        .keepLastIfActive(true)
                        .build())
                .build();

        DriveDeltaSyncPayload payload = new DriveDeltaSyncPayload("contoso-enterprise-tenant", driveId, ScanOrigin.FORWARD);

        oxmqTemplate.send(
                OxGuardQueueConstants.QUEUE_DRIVE_DELTA,
                "forward-event-" + driveId + "-" + System.currentTimeMillis(),
                payload,
                options
        );

        return ResponseEntity.ok(Map.of(
                "status", "ENQUEUED",
                "message", "Simulated forward file upload queued with Priority 1 and 15s debounce",
                "fileName", fileName,
                "exposure", exposureLevel.name()
        ));
    }
}
