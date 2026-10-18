package io.oxmq.oxguard.queue;

import io.oxmq.exception.DelayedException;
import io.oxmq.oxguard.connector.GraphClient;
import io.oxmq.oxguard.connector.GraphThrottlingException;
import io.oxmq.oxguard.engine.ExposureEngine;
import io.oxmq.oxguard.model.EffectiveExposure;
import io.oxmq.oxguard.model.FileScanPayload;
import io.oxmq.oxguard.model.ScanOrigin;
import io.oxmq.oxguard.service.TenantCooldownService;
import io.oxmq.spring.annotation.Header;
import io.oxmq.spring.annotation.OxmqListener;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Pillar 1 Worker: Resolves effective file permissions and Access Control Lists.
 * Implements Approach 5 resilience (Sentinel Check + Dynamic 429 Deferral).
 */
@Component
public class FilePermissionWorker {

    private static final Logger log = LoggerFactory.getLogger(FilePermissionWorker.class);

    private final GraphClient graphClient;
    private final ExposureEngine exposureEngine;
    private final TenantCooldownService cooldownService;

    public FilePermissionWorker(
            GraphClient graphClient,
            ExposureEngine exposureEngine,
            TenantCooldownService cooldownService
    ) {
        this.graphClient = graphClient;
        this.exposureEngine = exposureEngine;
        this.cooldownService = cooldownService;
    }

    @OxmqListener(queue = OxGuardQueueConstants.QUEUE_FILE_PERMISSION, concurrency = 30)
    public EffectiveExposure processPermissionScan(
            FileScanPayload payload,
            @Header("jobId") String jobId
    ) {
        // 1. Approach 5: Pre-check tenant cooldown sentinel before making external API call
        long cooldownRemaining = cooldownService.getRemainingCooldownMs(payload.tenantId());
        if (cooldownRemaining > 0) {
            log.info("Tenant [{}] is throttled. Deferring permission check for file [{}] for {}ms without HTTP call",
                    payload.tenantId(), payload.itemName(), cooldownRemaining);
            throw new DelayedException(Duration.ofMillis(cooldownRemaining));
        }

        // 2. Backward Scan Yielding: Check available tenant token headroom (< 25%)
        if (payload.scanOrigin() == ScanOrigin.BACKWARD) {
            if (cooldownService.checkHeadroomYield(payload.tenantId())) {
                log.info("Backward scan yielding permission check for [{}] to prioritize forward events. Deferring 2s",
                        payload.itemName());
                throw new DelayedException(Duration.ofSeconds(2));
            }
        }

        try {
            cooldownService.recordTenantApiCall(payload.tenantId(), payload.scanOrigin());
            EffectiveExposure exposure = graphClient.fetchPermissions(payload.tenantId(), payload.driveId(), payload.itemId());
            EffectiveExposure evaluated = exposureEngine.evaluate(exposure.permissions());

            log.debug("Evaluated permissions for [{}]: exposure={}", payload.itemName(), evaluated.level());
            return evaluated;

        } catch (GraphThrottlingException gte) {
            cooldownService.record429Hit(payload.tenantId());
            log.warn("Permission API throttled (HTTP 429) for tenant [{}]. Setting cooldown for {}s",
                    payload.tenantId(), gte.getRetryAfterSeconds());
            cooldownService.setCooldown(payload.tenantId(), gte.getRetryAfterMillis());
            throw new DelayedException(Duration.ofMillis(gte.getRetryAfterMillis()));
        }

    }
}
