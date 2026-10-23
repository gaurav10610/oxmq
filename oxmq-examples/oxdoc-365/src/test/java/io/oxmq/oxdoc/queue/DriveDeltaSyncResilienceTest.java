package io.oxmq.oxdoc.queue;

import io.oxmq.FlowProducer;
import io.oxmq.model.BackoffStrategy;
import io.oxmq.model.FlowJob;
import io.oxmq.oxdoc.connector.DeltaSyncResult;
import io.oxmq.oxdoc.connector.GraphClient;
import io.oxmq.oxdoc.model.DocumentCatalogPayload;
import io.oxmq.oxdoc.model.DriveDeltaSyncPayload;
import io.oxmq.oxdoc.model.DriveItem;
import io.oxmq.oxdoc.model.DriveType;
import io.oxmq.oxdoc.model.MonitoredDrive;
import io.oxmq.oxdoc.model.ScanOrigin;
import io.oxmq.oxdoc.service.DocumentCatalogRepository;
import io.oxmq.oxdoc.service.TenantCooldownService;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DriveDeltaSyncResilienceTest {

    @Mock
    private GraphClient graphClient;

    @Mock
    private TenantCooldownService cooldownService;

    @Mock
    private FlowProducer flowProducer;

    @Mock
    private DocumentCatalogRepository documentCatalogRepository;

    @Test
    @DisplayName("Verify delta token checkpointing and retry backoff on parent and child jobs")
    void testDeltaTokenCheckpointingAndJobRetryConfiguration() {
        DriveDeltaSyncWorker worker = new DriveDeltaSyncWorker(
                graphClient, cooldownService, flowProducer, documentCatalogRepository);

        String tenantId = "tenant-mid-2500";
        String driveId = "drive-101";
        String existingToken = "checkpoint-token-v1";
        String newToken = "checkpoint-token-v2";

        // 1. Existing drive has checkpoint token v1
        MonitoredDrive drive = new MonitoredDrive(driveId, tenantId, DriveType.SHAREPOINT_LIBRARY,
                "Finance Library", "admin@contoso.com", existingToken, "sub-1");
        when(documentCatalogRepository.getDrive(driveId)).thenReturn(Optional.of(drive));

        DriveItem fileItem = new DriveItem("item-1", driveId, "Quarterly_Report.docx", "/Finance/Quarterly_Report.docx",
                1024L, "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "https://sharepoint.com/file1", "https://storage.blob.core.windows.net/test/file1",
                false, "etag-1", "ctag-1");

        DeltaSyncResult deltaResult = new DeltaSyncResult(List.of(fileItem), List.of(), newToken);
        when(graphClient.fetchDeltaChanges(tenantId, driveId, existingToken)).thenReturn(deltaResult);

        // 2. Process delta sync
        DriveDeltaSyncPayload payload = new DriveDeltaSyncPayload(tenantId, driveId, ScanOrigin.FORWARD);
        int dispatched = worker.processDeltaSync(payload, "job-sync-1");
        assertEquals(1, dispatched);

        // 3. Verify delta token checkpoint was saved to repository
        verify(documentCatalogRepository).updateDriveDeltaToken(driveId, newToken);

        // 4. Capture dispatched FlowJob and verify retry & backoff on parent and children
        @SuppressWarnings("unchecked")
        ArgumentCaptor<FlowJob<DocumentCatalogPayload>> flowCaptor = ArgumentCaptor.forClass(FlowJob.class);
        verify(flowProducer).add(flowCaptor.capture());

        FlowJob<DocumentCatalogPayload> parent = flowCaptor.getValue();
        assertNotNull(parent);
        assertEquals(3, parent.getOpts().getAttempts());
        assertTrue(parent.getOpts().getBackoff() instanceof BackoffStrategy.Exponential);

        assertEquals(2, parent.getChildren().size());
        for (FlowJob<?> child : parent.getChildren()) {
            assertEquals(3, child.getOpts().getAttempts(), "Child job must have 3 retry attempts");
            assertTrue(child.getOpts().getBackoff() instanceof BackoffStrategy.Exponential, "Child job must use exponential backoff");
        }
    }
}
