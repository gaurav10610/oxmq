package io.oxmq.oxguard.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.oxmq.model.Job;
import io.oxmq.oxguard.model.EffectiveExposure;
import io.oxmq.oxguard.model.ExposureLevel;
import io.oxmq.oxguard.model.FileAggregatePayload;
import io.oxmq.oxguard.model.ScannedFileRecord;
import io.oxmq.oxguard.model.ThreatAnalysisResult;
import io.oxmq.oxguard.model.ThreatVerdict;
import io.oxmq.oxguard.service.FileInventoryRepository;
import io.oxmq.oxguard.service.LiveMetricsService;
import io.oxmq.spring.annotation.OxmqListener;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Parent DAG Aggregator Worker:
 * Activates automatically when BOTH Pillar 1 (Permissions) and Pillar 2 (Malware) child jobs complete.
 * Merges exposure classifications and malware analysis verdicts into the persistent DSPM inventory.
 */
@Component
public class FileAggregateWorker {

    private static final Logger log = LoggerFactory.getLogger(FileAggregateWorker.class);

    private final FileInventoryRepository repository;
    private final LiveMetricsService metricsService;
    private final ObjectMapper objectMapper;

    public FileAggregateWorker(
            FileInventoryRepository repository,
            LiveMetricsService metricsService,
            ObjectMapper objectMapper
    ) {
        this.repository = repository;
        this.metricsService = metricsService;
        this.objectMapper = objectMapper;
    }

    @OxmqListener(queue = OxGuardQueueConstants.QUEUE_FILE_AGGREGATE, concurrency = 15)
    public ScannedFileRecord processAggregate(Job<FileAggregatePayload> job) {
        FileAggregatePayload payload = (FileAggregatePayload) job.getData();
        Map<String, Object> childrenValues = job.getChildrenValues();

        log.info("Parent DAG Aggregator triggered for file [{}] (jobId={}). Merging child inspection verdicts...",
                payload.itemName(), job.getId());

        EffectiveExposure exposure = null;
        ThreatAnalysisResult threat = null;

        if (childrenValues != null) {
            for (Map.Entry<String, Object> entry : childrenValues.entrySet()) {
                String childKey = entry.getKey();
                Object val = entry.getValue();

                if (childKey.contains(OxGuardQueueConstants.QUEUE_FILE_PERMISSION) || childKey.contains("perm-")) {
                    exposure = convertValue(val, EffectiveExposure.class);
                } else if (childKey.contains(OxGuardQueueConstants.QUEUE_FILE_MALWARE) || childKey.contains("malware-")) {
                    threat = convertValue(val, ThreatAnalysisResult.class);
                }
            }
        }

        // Fallback defaults if a child failed with failParentOnFailure=false
        if (exposure == null) {
            exposure = new EffectiveExposure(ExposureLevel.PRIVATE, false, java.util.List.of());
        }
        if (threat == null) {
            threat = ThreatAnalysisResult.clean("sha256-uncomputed", 0.0);
        }

        ScannedFileRecord record = new ScannedFileRecord(
                payload.itemId(),
                payload.driveId(),
                payload.tenantId(),
                payload.itemName(),
                payload.itemPath(),
                payload.fileSize(),
                payload.mimeType(),
                exposure.level(),
                exposure.hasUniquePermissions(),
                threat.verdict(),
                threat.details(),
                threat.sha256(),
                payload.scanOrigin(),
                System.currentTimeMillis()
        );

        // Save to repository
        repository.saveFile(record);

        // Broadcast to live dashboard via SSE
        metricsService.broadcastFileScanned(record);

        log.info("DSPM Security Audit Completed for [{}]: Exposure=[{}], Threat=[{}], Origin=[{}]",
                record.fileName(), record.exposureLevel(), record.threatVerdict(), record.scanOrigin());

        return record;
    }

    private <T> T convertValue(Object val, Class<T> clazz) {
        if (val == null) return null;
        if (clazz.isInstance(val)) return clazz.cast(val);
        try {
            return objectMapper.convertValue(val, clazz);
        } catch (Exception e) {
            log.debug("Could not convert child result to {}: {}", clazz.getSimpleName(), e.getMessage());
            return null;
        }
    }
}
