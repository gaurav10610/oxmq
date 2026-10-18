package io.oxmq.oxguard.queue;

import io.oxmq.oxguard.connector.GraphClient;
import io.oxmq.oxguard.engine.ExposureEngine;
import io.oxmq.oxguard.model.EffectiveExposure;
import io.oxmq.oxguard.model.FileScanPayload;
import io.oxmq.oxguard.model.ScanOrigin;
import io.oxmq.oxguard.service.TenantCooldownService;
import io.oxmq.spring.annotation.Header;
import io.oxmq.spring.annotation.OxmqListener;
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
        cooldownService.recordTenantApiCall(payload.tenantId(), payload.scanOrigin());
        EffectiveExposure exposure = graphClient.fetchPermissions(payload.tenantId(), payload.driveId(), payload.itemId());
        EffectiveExposure evaluated = exposureEngine.evaluate(exposure.permissions());

        log.debug("Evaluated permissions for [{}]: exposure={}", payload.itemName(), evaluated.level());
        return evaluated;

    }
}
