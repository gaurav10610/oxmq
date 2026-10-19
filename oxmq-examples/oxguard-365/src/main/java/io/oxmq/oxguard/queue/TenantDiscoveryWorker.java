package io.oxmq.oxguard.queue;

import io.oxmq.model.JobOptions;
import io.oxmq.oxguard.connector.GraphClient;
import io.oxmq.oxguard.model.DriveDeltaSyncPayload;
import io.oxmq.oxguard.model.MonitoredDrive;
import io.oxmq.oxguard.model.ScanOrigin;
import io.oxmq.oxguard.model.TenantDiscoveryPayload;
import io.oxmq.oxguard.service.FileInventoryRepository;
import io.oxmq.spring.OxmqTemplate;
import io.oxmq.spring.annotation.Header;
import io.oxmq.spring.annotation.OxmqListener;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Discovers SharePoint Document Libraries and User OneDrives for a tenant.
 * Dispatches historical baseline crawl jobs at Priority 10.
 */
@Component
public class TenantDiscoveryWorker {

    private static final Logger log = LoggerFactory.getLogger(TenantDiscoveryWorker.class);

    private final GraphClient graphClient;
    private final FileInventoryRepository repository;
    private final OxmqTemplate oxmqTemplate;
    private final io.oxmq.oxguard.service.TenantSubscriptionService tenantSubscriptionService;

    public TenantDiscoveryWorker(
            GraphClient graphClient,
            FileInventoryRepository repository,
            OxmqTemplate oxmqTemplate,
            io.oxmq.oxguard.service.TenantSubscriptionService tenantSubscriptionService
    ) {
        this.graphClient = graphClient;
        this.repository = repository;
        this.oxmqTemplate = oxmqTemplate;
        this.tenantSubscriptionService = tenantSubscriptionService;
    }

    @OxmqListener(queue = OxGuardQueueConstants.QUEUE_TENANT_DISCOVERY, concurrency = 5)
    public int discoverTenant(
            TenantDiscoveryPayload payload,
            @Header("jobId") String jobId
    ) {
        log.info("Starting Tenant Discovery for tenant [{}] (jobId={})", payload.tenantId(), jobId);

        // Dynamically discover and register tenant subscription & license profile from Microsoft Graph
        tenantSubscriptionService.discoverAndRegister(payload.tenantId());

        List<MonitoredDrive> allDrives = new ArrayList<>();
        allDrives.addAll(graphClient.discoverSharePointDrives(payload.tenantId()));
        allDrives.addAll(graphClient.discoverUserOneDrives(payload.tenantId()));

        log.info("Discovered {} document libraries/drives for tenant [{}]", allDrives.size(), payload.tenantId());

        int enqueuedCount = 0;
        for (MonitoredDrive drive : allDrives) {
            repository.saveDrive(drive);

            // Backward historical baseline crawl dispatched at Priority 10 with exponential backoff
            JobOptions options = JobOptions.builder()
                    .priority(10)
                    .attempts(3)
                    .backoff(new io.oxmq.model.BackoffStrategy.Exponential(1000L))
                    .build();

            DriveDeltaSyncPayload deltaPayload = new DriveDeltaSyncPayload(
                    payload.tenantId(),
                    drive.driveId(),
                    ScanOrigin.BACKWARD
            );

            oxmqTemplate.send(
                    OxGuardQueueConstants.QUEUE_DRIVE_DELTA,
                    "delta-sync-" + drive.driveId(),
                    deltaPayload,
                    options
            );
            enqueuedCount++;
        }

        return enqueuedCount;
    }
}
