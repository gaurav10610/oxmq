package io.oxmq.oxdoc.web;

import io.oxmq.model.BackoffStrategy;
import io.oxmq.model.DeduplicationOptions;
import io.oxmq.model.JobOptions;
import io.oxmq.oxdoc.connector.MockAdminClient;
import io.oxmq.oxdoc.connector.MockGraphClient;
import io.oxmq.oxdoc.model.AccessScope;
import io.oxmq.oxdoc.model.DriveDeltaSyncPayload;
import io.oxmq.oxdoc.model.DriveType;
import io.oxmq.oxdoc.model.IndexedDocumentRecord;
import io.oxmq.oxdoc.model.MonitoredDrive;
import io.oxmq.oxdoc.model.ScanOrigin;
import io.oxmq.oxdoc.model.TenantDiscoveryPayload;
import io.oxmq.oxdoc.model.TenantProfile;
import io.oxmq.oxdoc.model.TenantTier;
import io.oxmq.oxdoc.queue.OxDocQueueConstants;
import io.oxmq.oxdoc.service.BackpressureService;
import io.oxmq.oxdoc.service.DocumentCatalogRepository;
import io.oxmq.oxdoc.service.LiveMetricsService;
import io.oxmq.oxdoc.service.TenantCooldownService;
import io.oxmq.oxdoc.service.TenantSubscriptionService;
import io.oxmq.spring.OxmqTemplate;
import io.oxmq.tracing.TraceContext;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * REST API Controller for OxDoc 365 Content Intelligence Dashboard and multi-tenant operations.
 */
@RestController
@RequestMapping("/api/v1/dashboard")
public class DashboardController {

    private static final Logger log = LoggerFactory.getLogger(DashboardController.class);

    private final DocumentCatalogRepository repository;
    private final LiveMetricsService metricsService;
    private final OxmqTemplate oxmqTemplate;
    private final TenantCooldownService cooldownService;
    private final MockAdminClient mockAdminClient;
    private final TenantSubscriptionService tenantSubscriptionService;

    @Autowired(required = false)
    private MockGraphClient mockGraphClient;

    @Autowired(required = false)
    private BackpressureService backpressureService;

    @Autowired
    public DashboardController(
            DocumentCatalogRepository repository,
            LiveMetricsService metricsService,
            OxmqTemplate oxmqTemplate,
            TenantCooldownService cooldownService,
            MockAdminClient mockAdminClient,
            TenantSubscriptionService tenantSubscriptionService,
            @Autowired(required = false) BackpressureService backpressureService
    ) {
        this.repository = repository;
        this.metricsService = metricsService;
        this.oxmqTemplate = oxmqTemplate;
        this.cooldownService = cooldownService;
        this.mockAdminClient = mockAdminClient;
        this.tenantSubscriptionService = tenantSubscriptionService;
        this.backpressureService = backpressureService;
    }

    public DashboardController(
            DocumentCatalogRepository repository,
            LiveMetricsService metricsService,
            OxmqTemplate oxmqTemplate,
            TenantCooldownService cooldownService,
            MockAdminClient mockAdminClient,
            TenantSubscriptionService tenantSubscriptionService
    ) {
        this(repository, metricsService, oxmqTemplate, cooldownService, mockAdminClient, tenantSubscriptionService, null);
    }

    /**
     * Lists all 5 onboarded enterprise tenants, license tiers, and adaptive tuning state.
     */
    @GetMapping("/tenants")
    public ResponseEntity<List<Map<String, Object>>> getTenants() {
        List<Map<String, Object>> list = new ArrayList<>();
        for (TenantProfile p : tenantSubscriptionService.getAllTenants()) {
            Map<String, Object> t = new LinkedHashMap<>();
            t.put("tenantId", p.tenantId());
            t.put("organizationName", p.organizationName());
            t.put("tier", p.tier().name());
            t.put("tierDisplayName", p.tier().getDisplayName());
            t.put("seatCount", p.seatCount());
            t.put("dailyRuBudget", p.dailyRuBudget());
            t.put("burstLimitRUsPerSec", cooldownService.getTenantBurstLimit(p.tenantId()));
            t.put("tierSource", p.tierSource().name());
            t.put("isOverridePinned", p.isOverridePinned());
            t.put("upgradeCount", p.upgradeCount());
            t.put("downgradeCount", p.downgradeCount());
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
        map.put("totalDocumentsIndexed", metrics.get("totalDocuments"));
        map.put("totalDrivesMonitored", metrics.get("totalDrives"));

        if (backpressureService != null) {
            map.put("redisUsedMemoryBytes", backpressureService.getRedisUsedMemoryBytes());
            map.put("redisUsedMemoryMB", backpressureService.getRedisUsedMemoryMB());
            map.put("redisMaxMemoryCeilingMB", backpressureService.getRedisMaxMemoryCeilingMB());
            map.put("isBackpressureActive", backpressureService.isBackpressureActive());
            map.put("backpressureYields", backpressureService.getTotalYields());
            map.put("downstreamBacklogJobs", backpressureService.getDownstreamBacklogJobs());
            map.put("lastThrottleReason", backpressureService.getLastThrottleReason());
        } else {
            map.put("redisUsedMemoryBytes", 0L);
            map.put("redisUsedMemoryMB", 0.0);
            map.put("redisMaxMemoryCeilingMB", 200.0);
            map.put("isBackpressureActive", false);
            map.put("backpressureYields", 0L);
            map.put("downstreamBacklogJobs", 0L);
            map.put("lastThrottleReason", "NONE");
        }
        return ResponseEntity.ok(map);
    }

    /**
     * Exposes real-time Redis memory ceiling and queue backpressure telemetry.
     */
    @GetMapping("/backpressure")
    public ResponseEntity<Map<String, Object>> getBackpressureTelemetry() {
        if (backpressureService != null) {
            return ResponseEntity.ok(backpressureService.getTelemetry());
        }
        return ResponseEntity.ok(Map.of("enabled", false, "message", "BackpressureService not configured"));
    }

    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> getMetrics(
            @RequestParam(value = "tenantId", required = false) String tenantId
    ) {
        return ResponseEntity.ok(repository.getMetrics(tenantId));
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
            mockAdminClient.setRateLimit(tenantId, limit);
            return ResponseEntity.ok(Map.of(
                    "status", "UPDATED",
                    "tenantId", tenantId,
                    "maxRequestsPerSecond", limit,
                    "message", "Rate limit adjusted for " + tenantId + " to " + limit + " req/s"
            ));
        } else {
            cooldownService.setMaxRequestsPerSecond(limit);
            mockAdminClient.setGlobalRateLimit(limit);
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
        boolean ok = mockAdminClient.trigger429Burst(tenantId, burstCount, enabled);
        if (ok) {
            return ResponseEntity.ok(Map.of(
                    "status", "TRIGGERED",
                    "tenantId", tenantId,
                    "burstCount", burstCount,
                    "enabled", enabled,
                    "message", "Triggered " + burstCount + " HTTP 429 burst responses on tenant: " + tenantId
            ));
        }
        return ResponseEntity.badRequest().body(Map.of("error", "Mock Office 365 Server not reachable on port 8081"));
    }

    @GetMapping("/files")
    public ResponseEntity<List<IndexedDocumentRecord>> getFiles(
            @RequestParam(value = "tenantId", required = false) String tenantId,
            @RequestParam(value = "scope", required = false) String scope,
            @RequestParam(value = "exposure", required = false) String exposure,
            @RequestParam(value = "verdict", required = false) String verdict,
            @RequestParam(value = "threat", required = false) String threat,
            @RequestParam(value = "origin", required = false) String origin
    ) {
        List<IndexedDocumentRecord> files = repository.getRecentDocuments(tenantId, 100);

        String effectiveScope = (scope != null && !scope.isBlank()) ? scope : exposure;
        if (effectiveScope != null && !effectiveScope.isBlank() && !"all".equalsIgnoreCase(effectiveScope)) {
            files = files.stream()
                    .filter(f -> f.accessScope().name().equalsIgnoreCase(effectiveScope))
                    .toList();
        }

        String effectiveVerdict = (verdict != null && !verdict.isBlank()) ? verdict : threat;
        if (effectiveVerdict != null && !effectiveVerdict.isBlank() && !"all".equalsIgnoreCase(effectiveVerdict)) {
            files = files.stream()
                    .filter(f -> f.indexingVerdict().name().equalsIgnoreCase(effectiveVerdict))
                    .toList();
        }

        if (origin != null && !origin.isBlank() && !"all".equalsIgnoreCase(origin)) {
            files = files.stream()
                    .filter(f -> f.scanOrigin().name().equalsIgnoreCase(origin))
                    .toList();
        }

        return ResponseEntity.ok(files);
    }

    /**
     * Returns the structured multi-tenant hierarchy (Tenants -> Drives -> Folders -> Files)
     * for interactive exploration in the OxDoc 365 dashboard.
     */
    @GetMapping("/hierarchy")
    public ResponseEntity<Map<String, Object>> getHierarchy(
            @RequestParam(value = "tenantId", required = false) String tenantId
    ) {
        boolean filterTenant = tenantId != null && !tenantId.isBlank() && !"all".equalsIgnoreCase(tenantId);
        List<TenantProfile> profiles = filterTenant
                ? Collections.singletonList(tenantSubscriptionService.getTenantProfile(tenantId))
                : tenantSubscriptionService.getAllTenants();

        List<Map<String, Object>> tenantList = new ArrayList<>();
        for (TenantProfile p : profiles) {
            if (p == null) continue;
            Map<String, Object> tMap = new LinkedHashMap<>();
            tMap.put("tenantId", p.tenantId());
            tMap.put("organizationName", p.organizationName());
            tMap.put("tier", p.tier().name());
            tMap.put("tierDisplayName", p.tier().getDisplayName());
            tMap.put("seatCount", p.seatCount());
            tMap.put("burstLimitRUsPerSec", cooldownService.getTenantBurstLimit(p.tenantId()));

            Map<String, Object> tenantMetrics = repository.getMetrics(p.tenantId());
            long docCount = tenantMetrics.get("totalDocuments") instanceof Number n ? n.longValue() : 0L;
            List<MonitoredDrive> drives = repository.getDrivesByTenantId(p.tenantId());
            tMap.put("totalDocuments", docCount);
            tMap.put("totalDrives", drives.size());

            List<IndexedDocumentRecord> recentDocs = repository.getRecentDocuments(p.tenantId(), 200);

            List<Map<String, Object>> spSites = new ArrayList<>();
            List<Map<String, Object>> userODs = new ArrayList<>();

            for (MonitoredDrive drive : drives) {
                Map<String, Object> driveMap = new LinkedHashMap<>();
                driveMap.put("driveId", drive.driveId());
                driveMap.put("name", drive.name());
                driveMap.put("driveType", drive.driveType().name());

                List<IndexedDocumentRecord> driveDocs = recentDocs.stream()
                        .filter(d -> drive.driveId().equals(d.driveId()))
                        .toList();
                driveMap.put("totalFiles", driveDocs.size());

                long publicCount = driveDocs.stream()
                        .filter(d -> d.accessScope() == AccessScope.PUBLIC_ANONYMOUS)
                        .count();
                driveMap.put("publicExposureCount", publicCount);

                Map<String, List<IndexedDocumentRecord>> byFolder = driveDocs.stream()
                        .collect(Collectors.groupingBy(d -> {
                            String pth = d.filePath();
                            if (pth == null || pth.isBlank() || "/".equals(pth)) return "Root Library";
                            int lastSlash = pth.lastIndexOf('/');
                            return lastSlash > 0 ? pth.substring(0, lastSlash) : pth;
                        }, LinkedHashMap::new, Collectors.toList()));

                if (byFolder.isEmpty()) {
                    byFolder.put("Root Library", Collections.emptyList());
                }

                List<Map<String, Object>> folderList = new ArrayList<>();
                for (Map.Entry<String, List<IndexedDocumentRecord>> entry : byFolder.entrySet()) {
                    Map<String, Object> fMap = new LinkedHashMap<>();
                    fMap.put("folderPath", entry.getKey());
                    fMap.put("files", entry.getValue());
                    folderList.add(fMap);
                }
                driveMap.put("folders", folderList);

                if (drive.driveType() == DriveType.SHAREPOINT_LIBRARY) {
                    spSites.add(driveMap);
                } else {
                    userODs.add(driveMap);
                }
            }

            tMap.put("sharepointSites", spSites);
            tMap.put("oneDrives", userODs);
            tenantList.add(tMap);
        }

        return ResponseEntity.ok(Map.of("tenants", tenantList));
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamEvents() {
        return metricsService.subscribe();
    }

    @GetMapping(value = "/sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamSse() {
        return metricsService.subscribe();
    }

    @PostMapping("/seed")
    public ResponseEntity<Map<String, Object>> seedAdditionalFiles(
            @RequestParam(value = "count", defaultValue = "5000") int count
    ) {
        boolean ok = mockAdminClient.seedAdditionalFiles(count);
        if (ok) {
            Map<String, Object> stats = mockAdminClient.getStats();
            return ResponseEntity.ok(Map.of(
                    "status", "SEEDED",
                    "totalFiles", stats.getOrDefault("totalFiles", count),
                    "message", "Successfully seeded files up to " + count
            ));
        }
        return ResponseEntity.badRequest().body(Map.of("error", "Mock Office 365 Server is not reachable on port 8081"));
    }

    /**
     * Triggers a historical backward baseline crawl for a specific tenant.
     */
    @PostMapping("/trigger/crawl")
    public ResponseEntity<Map<String, Object>> triggerBaselineCrawl(
            @RequestParam(value = "tenantId", defaultValue = "tenant-mid-2500") String tenantId
    ) {
        log.info("Manually triggering tenant discovery and backward crawl for [{}]", tenantId);

        TraceContext traceCtx = TraceContext.fromMdc() != null ? TraceContext.fromMdc().createChildSpan() : TraceContext.generateNew();
        JobOptions options = JobOptions.builder()
                .priority(5)
                .attempts(3)
                .backoff(new BackoffStrategy.Exponential(1000L))
                .traceparent(traceCtx.asTraceparent())
                .build();

        oxmqTemplate.send(
                OxDocQueueConstants.QUEUE_TENANT_DISCOVERY,
                "discovery-" + tenantId + "-" + System.currentTimeMillis(),
                new TenantDiscoveryPayload(tenantId),
                options
        );

        return ResponseEntity.ok(Map.of(
                "status", "DISPATCHED",
                "tenantId", tenantId,
                "traceId", traceCtx.traceId(),
                "message", "Baseline historical discovery job queued for tenant: " + tenantId,
                "timestamp", System.currentTimeMillis()
        ));
    }

    /**
     * Triggers concurrent baseline crawl across ALL 5 official Microsoft 365 tenant accounts!
     */
    @PostMapping("/trigger/crawl-all")
    public ResponseEntity<Map<String, Object>> triggerCrawlAll() {
        log.info("Triggering concurrent baseline crawl across all official Microsoft 365 tenant accounts");
        List<String> dispatched = new ArrayList<>();
        for (TenantProfile profile : tenantSubscriptionService.getAllTenants()) {
            TraceContext traceCtx = TraceContext.fromMdc() != null ? TraceContext.fromMdc().createChildSpan() : TraceContext.generateNew();
            JobOptions.Builder optBuilder = JobOptions.builder()
                    .priority(5)
                    .attempts(3)
                    .backoff(new BackoffStrategy.Exponential(1000L))
                    .removeOnComplete(true);
            traceCtx.inject(optBuilder);

            oxmqTemplate.send(
                    OxDocQueueConstants.QUEUE_TENANT_DISCOVERY,
                    "discovery-" + profile.tenantId() + "-" + System.currentTimeMillis(),
                    new TenantDiscoveryPayload(profile.tenantId(), traceCtx.traceId()),
                    optBuilder.build()
            );
            dispatched.add(profile.tenantId());
        }

        return ResponseEntity.ok(Map.of(
                "status", "ALL_DISPATCHED",
                "tenants", dispatched,
                "totalTenants", dispatched.size(),
                "message", "Dispatched concurrent discovery across all 5 enterprise tenant accounts (231,000+ files)"
        ));
    }

    public ResponseEntity<Map<String, Object>> simulateForwardUpload(
            String driveId,
            String fileName,
            String scope,
            String docFormat
    ) {
        return simulateForwardUpload("tenant-mid-2500", driveId, fileName, scope, docFormat);
    }

    /**
     * Simulates a real-time Forward upload (Priority 1) with auto-save debouncing.
     */
    @PostMapping("/trigger/upload")
    public ResponseEntity<Map<String, Object>> simulateForwardUpload(
            @RequestParam(value = "tenantId", defaultValue = "tenant-mid-2500") String tenantId,
            @RequestParam(value = "driveId", defaultValue = "drive-mid-sp-logistics") String driveId,
            @RequestParam(value = "fileName", defaultValue = "Quarterly_Strategic_Roadmap.docx") String fileName,
            @RequestParam(value = "scope", defaultValue = "ORGANIZATION_WIDE") String scope,
            @RequestParam(value = "docFormat", defaultValue = "DOCUMENT") String docFormat
    ) {
        log.info("Simulating real-time forward document upload for tenant [{}]: file=[{}], drive=[{}], scope=[{}], docFormat=[{}]",
                tenantId, fileName, driveId, scope, docFormat);

        byte[] fileBytes;
        if ("SPREADSHEET".equalsIgnoreCase(docFormat)) {
            fileBytes = "Category,Quarter,Budget,Actual\nAI_Infrastructure,Q1,1200000,950000\nCloud_Compute,Q1,850000,810000".getBytes(StandardCharsets.UTF_8);
            if (!fileName.endsWith(".xlsx") && !fileName.endsWith(".csv")) fileName += ".xlsx";
        } else if ("DATASET".equalsIgnoreCase(docFormat)) {
            fileBytes = new byte[8192];
            new java.util.Random().nextBytes(fileBytes);
            if (!fileName.endsWith(".bin")) fileName += ".bin";
        } else {
            fileBytes = ("OxDoc 365 Enterprise Content Intelligence Document: " + fileName + "\nGenerated at: " + System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8);
            if (!fileName.endsWith(".docx") && !fileName.endsWith(".pdf") && !fileName.endsWith(".txt") && !fileName.endsWith(".xlsx") && !fileName.endsWith(".csv") && !fileName.endsWith(".bin")) {
                fileName += ".docx";
            }
        }

        AccessScope accessScope;
        try {
            accessScope = AccessScope.valueOf(scope.toUpperCase());
        } catch (Exception e) {
            accessScope = AccessScope.ORGANIZATION_WIDE;
        }

        if (mockGraphClient != null) {
            mockGraphClient.addMockForwardFile(driveId, fileName, accessScope, fileBytes);
        }
        if (repository != null && repository.getDrive(driveId).isEmpty()) {
            repository.saveDrive(new MonitoredDrive(driveId, tenantId, DriveType.SHAREPOINT_LIBRARY, "Real-Time Forward Drive (" + tenantId + ")", "admin@" + tenantId + ".com", "delta-v1-0", null));
        }
        String scopeStr = "organization";
        if (accessScope == AccessScope.PUBLIC_ANONYMOUS) scopeStr = "anonymous";
        else if (accessScope == AccessScope.EXTERNAL_GUEST) scopeStr = "users";
        else if (accessScope == AccessScope.PRIVATE) scopeStr = "private";
        mockAdminClient.uploadForwardFile(driveId, fileName, scopeStr, fileBytes, "application/octet-stream");

        TraceContext traceCtx = TraceContext.fromMdc() != null ? TraceContext.fromMdc().createChildSpan() : TraceContext.generateNew();
        JobOptions.Builder optBuilder = JobOptions.builder()
                .priority(1)
                .attempts(3)
                .removeOnComplete(true)
                .deduplication(DeduplicationOptions.builder()
                                .id("forward-upload-" + driveId + "-" + fileName)
                                .ttl(Duration.ofSeconds(15))
                                .keepLastIfActive(true)
                                .build());
        traceCtx.inject(optBuilder);

        DriveDeltaSyncPayload payload = new DriveDeltaSyncPayload(tenantId, driveId, ScanOrigin.FORWARD, null, traceCtx.traceId());

        oxmqTemplate.send(
                OxDocQueueConstants.QUEUE_DRIVE_DELTA,
                "forward-event-" + driveId + "-" + System.currentTimeMillis(),
                payload,
                optBuilder.build()
        );

        return ResponseEntity.ok(Map.of(
                "status", "ENQUEUED",
                "tenantId", tenantId,
                "traceId", traceCtx.traceId(),
                "message", "Simulated forward file upload queued with Priority 1 and 15s debounce",
                "fileName", fileName,
                "scope", accessScope.name()
        ));
    }

    /**
     * Pins a static tier override on a tenant account, bypassing dynamic discovery and adaptive tuning.
     */
    @PostMapping("/tenants/{tenantId}/override-tier")
    public ResponseEntity<Map<String, Object>> setTenantOverride(
            @PathVariable String tenantId,
            @RequestParam String tier
    ) {
        log.info("API: Setting static tier override [{}] on tenant [{}]", tier, tenantId);
        TenantTier targetTier = TenantTier.valueOf(tier.trim().toUpperCase());
        TenantProfile updated = tenantSubscriptionService.setStaticOverride(tenantId, targetTier);
        return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "tenantId", tenantId,
                "tier", updated.tier().name(),
                "burstLimitRUsPerSec", updated.burstLimitRUsPerSec(),
                "isOverridePinned", updated.isOverridePinned(),
                "tierSource", updated.tierSource().name()
        ));
    }

    /**
     * Clears a static tier override on a tenant account, re-enabling dynamic discovery and adaptive tuning.
     */
    @DeleteMapping("/tenants/{tenantId}/override-tier")
    public ResponseEntity<Map<String, Object>> clearTenantOverride(@PathVariable String tenantId) {
        log.info("API: Clearing static tier override on tenant [{}]", tenantId);
        TenantProfile updated = tenantSubscriptionService.clearStaticOverride(tenantId);
        return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "tenantId", tenantId,
                "tier", updated.tier().name(),
                "burstLimitRUsPerSec", updated.burstLimitRUsPerSec(),
                "isOverridePinned", updated.isOverridePinned(),
                "tierSource", updated.tierSource().name()
        ));
    }

    /**
     * Manually triggers an adaptive tier evaluation cycle for a tenant account.
     */
    @PostMapping("/tenants/{tenantId}/evaluate-tier")
    public ResponseEntity<Map<String, Object>> evaluateTenantTier(@PathVariable String tenantId) {
        log.info("API: Evaluating adaptive tier tuning for tenant [{}]", tenantId);
        TenantProfile updated = tenantSubscriptionService.evaluateTenantTier(tenantId);
        return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "tenantId", tenantId,
                "tier", updated.tier().name(),
                "burstLimitRUsPerSec", updated.burstLimitRUsPerSec(),
                "tierSource", updated.tierSource().name(),
                "isOverridePinned", updated.isOverridePinned(),
                "upgradeCount", updated.upgradeCount(),
                "downgradeCount", updated.downgradeCount(),
                "consecutiveCleanWindows", tenantSubscriptionService.getConsecutiveCleanWindows(tenantId)
        ));
    }
}
