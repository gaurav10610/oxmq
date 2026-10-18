package io.oxmq.oxguard.web;

import io.oxmq.model.DeduplicationOptions;
import io.oxmq.model.JobOptions;
import io.oxmq.oxguard.connector.MockGraphClient;
import io.oxmq.oxguard.model.DriveDeltaSyncPayload;
import io.oxmq.oxguard.model.ExposureLevel;
import io.oxmq.oxguard.model.ScanOrigin;
import io.oxmq.oxguard.model.ScannedFileRecord;
import io.oxmq.oxguard.model.TenantDiscoveryPayload;
import io.oxmq.oxguard.model.TenantProfile;
import io.oxmq.oxguard.queue.OxGuardQueueConstants;
import io.oxmq.oxguard.service.FileInventoryRepository;
import io.oxmq.oxguard.service.LiveMetricsService;
import io.oxmq.oxguard.service.TenantCooldownService;
import io.oxmq.spring.OxmqTemplate;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 * REST API Controller for OxGuard 365 DSPM Dashboard and multi-tenant operations.
 */
@RestController
@RequestMapping("/api/v1/dashboard")
public class DashboardController {

    private static final Logger log = LoggerFactory.getLogger(DashboardController.class);

    private final FileInventoryRepository repository;
    private final LiveMetricsService metricsService;
    private final OxmqTemplate oxmqTemplate;
    private final TenantCooldownService cooldownService;

    @Autowired(required = false)
    private MockGraphClient mockGraphClient;

    @Autowired(required = false)
    private io.oxmq.oxguard.mockserver.MockOffice365Server mockOffice365Server;

    public DashboardController(
            FileInventoryRepository repository,
            LiveMetricsService metricsService,
            OxmqTemplate oxmqTemplate,
            TenantCooldownService cooldownService
    ) {
        this.repository = repository;
        this.metricsService = metricsService;
        this.oxmqTemplate = oxmqTemplate;
        this.cooldownService = cooldownService;
    }

    /**
     * Lists all 5 onboarded enterprise tenants and their license tiers.
     */
    @GetMapping("/tenants")
    public ResponseEntity<List<Map<String, Object>>> getTenants() {
        List<Map<String, Object>> list = new ArrayList<>();
        for (TenantProfile p : TenantProfile.ALL_PROFILES) {
            Map<String, Object> t = new LinkedHashMap<>();
            t.put("tenantId", p.tenantId());
            t.put("organizationName", p.organizationName());
            t.put("tier", p.tier().name());
            t.put("tierDisplayName", p.tier().getDisplayName());
            t.put("seatCount", p.seatCount());
            t.put("dailyRuBudget", p.dailyRuBudget());
            t.put("burstLimitRUsPerSec", cooldownService.getTenantBurstLimit(p.tenantId()));
            t.put("connected", true);
            list.add(t);
        }
        return ResponseEntity.ok(list);
    }

    /**
     * Exposes platform and JVM system metrics.
     */
    @GetMapping("/system-metrics")
    public ResponseEntity<Map<String, Object>> getSystemMetrics() {
        Runtime rt = Runtime.getRuntime();
        long totalMem = rt.totalMemory();
        long freeMem = rt.freeMemory();
        long maxMem = rt.maxMemory();
        long usedMem = totalMem - freeMem;

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("jvmUsedMemoryBytes", usedMem);
        map.put("jvmUsedMemoryMB", Math.round(usedMem / (1024.0 * 1024.0) * 10.0) / 10.0);
        map.put("jvmTotalMemoryMB", Math.round(totalMem / (1024.0 * 1024.0) * 10.0) / 10.0);
        map.put("jvmMaxMemoryMB", Math.round(maxMem / (1024.0 * 1024.0) * 10.0) / 10.0);
        map.put("activeThreadCount", Thread.activeCount());
        map.put("javaVersion", System.getProperty("java.version"));
        map.put("osName", System.getProperty("os.name"));

        Map<String, Object> metrics = repository.getMetrics();
        map.put("totalFilesScanned", metrics.get("totalFiles"));
        map.put("totalThreatsFound", metrics.get("totalThreats"));
        map.put("totalDrivesMonitored", metrics.get("totalDrives"));
        return ResponseEntity.ok(map);
    }

    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> getMetrics(
            @RequestParam(value = "tenantId", required = false) String tenantId
    ) {
        return ResponseEntity.ok(repository.getMetrics(tenantId));
    }

    @GetMapping("/hierarchy")
    public ResponseEntity<Map<String, Object>> getHierarchy(
            @RequestParam(value = "tenantId", required = false) String tenantId
    ) {
        return ResponseEntity.ok(repository.getHierarchy(tenantId));
    }

    @GetMapping("/quota")
    public ResponseEntity<Map<String, Object>> getQuotaTelemetry(
            @RequestParam(value = "tenantId", required = false) String tenantId
    ) {
        if (tenantId == null || tenantId.isBlank() || "all".equalsIgnoreCase(tenantId)) {
            return ResponseEntity.ok(cooldownService.getAllTenantsQuotaTelemetry());
        }
        return ResponseEntity.ok(cooldownService.getQuotaTelemetry(tenantId));
    }

    @PostMapping("/ratelimit")
    public ResponseEntity<Map<String, Object>> updateRateLimit(
            @RequestParam(value = "tenantId", required = false) String tenantId,
            @RequestParam(value = "limit", defaultValue = "50") int limit
    ) {
        if (tenantId != null && !tenantId.isBlank() && !"all".equalsIgnoreCase(tenantId)) {
            cooldownService.setMaxRequestsPerSecond(tenantId, limit);
            if (mockOffice365Server != null) {
                mockOffice365Server.setRateLimit(tenantId, limit);
            }
            return ResponseEntity.ok(Map.of(
                    "status", "UPDATED",
                    "tenantId", tenantId,
                    "maxRequestsPerSecond", limit,
                    "message", "Rate limit adjusted for " + tenantId + " to " + limit + " req/s"
            ));
        } else {
            cooldownService.setMaxRequestsPerSecond(limit);
            if (mockOffice365Server != null) {
                mockOffice365Server.setRateLimit(limit);
            }
            return ResponseEntity.ok(Map.of(
                    "status", "UPDATED_ALL",
                    "maxRequestsPerSecond", limit,
                    "message", "Rate limit adjusted globally to " + limit + " req/s"
            ));
        }
    }

    @PostMapping("/simulate-429")
    public ResponseEntity<Map<String, Object>> simulate429Burst(
            @RequestParam(value = "tenantId", defaultValue = "tenant-smb-250") String tenantId,
            @RequestParam(value = "enabled", defaultValue = "true") boolean enabled,
            @RequestParam(value = "burstCount", defaultValue = "20") int burstCount
    ) {
        if (mockOffice365Server != null) {
            mockOffice365Server.trigger429Burst(tenantId, burstCount);
            mockOffice365Server.setSimulate429Burst(tenantId, enabled);
            return ResponseEntity.ok(Map.of(
                    "status", "TRIGGERED",
                    "tenantId", tenantId,
                    "burstCount", burstCount,
                    "enabled", enabled,
                    "message", "Triggered " + burstCount + " HTTP 429 burst responses on tenant: " + tenantId
            ));
        }
        return ResponseEntity.badRequest().body(Map.of("error", "MockOffice365Server not active"));
    }

    @GetMapping("/files")
    public ResponseEntity<List<ScannedFileRecord>> getFiles(
            @RequestParam(value = "tenantId", required = false) String tenantId,
            @RequestParam(value = "exposure", required = false) String exposure,
            @RequestParam(value = "threat", required = false) String threat,
            @RequestParam(value = "origin", required = false) String origin
    ) {
        List<ScannedFileRecord> files = repository.getFilesByTenant(tenantId);

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
            @RequestParam(value = "count", defaultValue = "5000") int count
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
     * Triggers a historical backward baseline crawl for a specific tenant.
     */
    @PostMapping("/trigger/crawl")
    public ResponseEntity<Map<String, Object>> triggerBaselineCrawl(
            @RequestParam(value = "tenantId", defaultValue = "tenant-mid-2500") String tenantId
    ) {
        log.info("Manually triggering tenant discovery and backward crawl for [{}]", tenantId);

        JobOptions options = JobOptions.builder()
                .priority(5)
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
                "tenantId", tenantId,
                "message", "Baseline historical discovery job queued for tenant: " + tenantId,
                "timestamp", System.currentTimeMillis()
        ));
    }

    /**
     * Triggers concurrent baseline crawl across ALL 5 official Microsoft 365 tenant accounts!
     */
    @PostMapping("/trigger/crawl-all")
    public ResponseEntity<Map<String, Object>> triggerCrawlAll() {
        log.info("Triggering concurrent baseline crawl across all 5 official Microsoft 365 tenant accounts");
        List<String> dispatched = new ArrayList<>();
        for (TenantProfile profile : TenantProfile.ALL_PROFILES) {
            JobOptions options = JobOptions.builder()
                    .priority(5)
                    .attempts(3)
                    .build();

            oxmqTemplate.send(
                    OxGuardQueueConstants.QUEUE_TENANT_DISCOVERY,
                    "discovery-" + profile.tenantId() + "-" + System.currentTimeMillis(),
                    new TenantDiscoveryPayload(profile.tenantId()),
                    options
            );
            dispatched.add(profile.tenantId());
        }

        return ResponseEntity.ok(Map.of(
                "status", "ALL_DISPATCHED",
                "tenants", dispatched,
                "totalTenants", dispatched.size(),
                "message", "Dispatched concurrent discovery across all 5 enterprise tenant accounts (5,000 files)"
        ));
    }

    /**
     * Backward-compatible overload for 4-parameter simulation.
     */
    public ResponseEntity<Map<String, Object>> simulateForwardUpload(
            String driveId,
            String fileName,
            String exposure,
            String threatType
    ) {
        return simulateForwardUpload("tenant-mid-2500", driveId, fileName, exposure, threatType);
    }

    /**
     * Simulates a real-time Forward upload (Priority 1) with auto-save debouncing.
     */
    @PostMapping("/trigger/upload")
    public ResponseEntity<Map<String, Object>> simulateForwardUpload(
            @RequestParam(value = "tenantId", defaultValue = "tenant-mid-2500") String tenantId,
            @RequestParam(value = "driveId", defaultValue = "drive-mid-sp-logistics") String driveId,
            @RequestParam(value = "fileName", defaultValue = "Quarterly_Board_Deck_Draft.pptx") String fileName,
            @RequestParam(value = "exposure", defaultValue = "PUBLIC_ANONYMOUS") String exposure,
            @RequestParam(value = "threatType", defaultValue = "CLEAN") String threatType
    ) {

        log.info("Simulating real-time forward upload for tenant [{}]: file=[{}], drive=[{}], exposure=[{}], threatType=[{}]",
                tenantId, fileName, driveId, exposure, threatType);

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

        JobOptions options = JobOptions.builder()
                .priority(1)
                .attempts(3)
                .deduplication(DeduplicationOptions.builder()
                        .id("forward-upload-" + driveId)
                        .ttl(Duration.ofSeconds(15))
                        .keepLastIfActive(true)
                        .build())
                .build();

        DriveDeltaSyncPayload payload = new DriveDeltaSyncPayload(tenantId, driveId, ScanOrigin.FORWARD);

        oxmqTemplate.send(
                OxGuardQueueConstants.QUEUE_DRIVE_DELTA,
                "forward-event-" + driveId + "-" + System.currentTimeMillis(),
                payload,
                options
        );

        return ResponseEntity.ok(Map.of(
                "status", "ENQUEUED",
                "tenantId", tenantId,
                "message", "Simulated forward file upload queued with Priority 1 and 15s debounce",
                "fileName", fileName,
                "exposure", exposureLevel.name()
        ));
    }
}
