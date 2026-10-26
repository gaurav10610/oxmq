package io.oxmq.oxdoc.service;

import io.oxmq.FlowProducer;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.model.JobOptions;
import io.oxmq.oxdoc.connector.DeltaSyncResult;
import io.oxmq.oxdoc.connector.GraphClient;
import io.oxmq.oxdoc.model.DriveDeltaSyncPayload;
import io.oxmq.oxdoc.model.DriveItem;
import io.oxmq.oxdoc.model.ScanOrigin;
import io.oxmq.oxdoc.queue.DriveDeltaSyncWorker;
import io.oxmq.oxdoc.queue.OxDocQueueConstants;
import io.oxmq.plugin.CheckpointStatePlugin;
import io.oxmq.spring.OxmqTemplate;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("OxDoc 365: BackpressureService & Strict 200MB Memory Ceiling Test")
public class BackpressureServiceIntegrationTest {

    @Mock
    private RedisConnectionManager connectionManager;

    @Mock
    private OxmqTemplate oxmqTemplate;

    @Mock
    private GraphClient graphClient;

    @Mock
    private FlowProducer flowProducer;

    @Mock
    private DocumentCatalogRepository catalogRepository;

    @Mock
    private CheckpointStatePlugin checkpointStatePlugin;

    private BackpressureService backpressureService;

    @BeforeEach
    void setUp() {
        backpressureService = new BackpressureService(
                connectionManager,
                oxmqTemplate,
                null,
                true,
                209715200L, // 200 MB
                0.85,       // 85% = ~170 MB
                0.60,       // 60% = ~120 MB
                35000L,     // 35k jobs
                10000L,     // 10k jobs
                2500L,      // 2500ms backoff
                1000L
        );
    }

    @Test
    @DisplayName("Verify telemetry accurately reflects strict 200 MB Redis memory ceiling")
    void testCeilingAndThresholdsConfiguration() {
        assertThat(backpressureService.getMaxRedisMemoryBytes()).isEqualTo(209715200L);
        assertThat(backpressureService.getRedisMaxMemoryCeilingMB()).isEqualTo(200.0);

        Map<String, Object> telemetry = backpressureService.getTelemetry();
        assertThat(telemetry.get("enabled")).isEqualTo(true);
        assertThat(telemetry.get("redisMaxMemoryCeilingMB")).isEqualTo(200.0);
        assertThat(telemetry.get("highMemoryThresholdMB")).isEqualTo(170L);
        assertThat(telemetry.get("lowMemoryThresholdMB")).isEqualTo(120L);
        assertThat(telemetry.get("highQueueWatermark")).isEqualTo(35000L);
        assertThat(telemetry.get("lowQueueWatermark")).isEqualTo(10000L);
        assertThat(telemetry.get("backoffDelayMs")).isEqualTo(2500L);
    }

    @Test
    @DisplayName("Verify memory threshold hysteresis: activates at 85% (170MB) and holds until 60% (120MB)")
    void testMemoryThresholdHysteresis() {
        // 1. Initial healthy state (50 MB)
        backpressureService.setSimulatedUsedMemoryBytes(50L * 1024 * 1024);
        backpressureService.setSimulatedQueueBacklog(1000L);
        assertThat(backpressureService.shouldThrottle()).isFalse();
        assertThat(backpressureService.isBackpressureActive()).isFalse();

        // 2. Memory spikes past 85% (180 MB > 170 MB trigger)
        backpressureService.setSimulatedUsedMemoryBytes(180L * 1024 * 1024);
        assertThat(backpressureService.shouldThrottle()).isTrue();
        assertThat(backpressureService.isBackpressureActive()).isTrue();
        assertThat(backpressureService.getLastThrottleReason()).contains("REDIS_MEM_EXCEEDED");
        assertThat(backpressureService.getTotalYields()).isEqualTo(1L);

        // 3. Memory drops to 150 MB (in between 60% and 85%) -> Hysteresis MUST hold backpressure active!
        backpressureService.setSimulatedUsedMemoryBytes(150L * 1024 * 1024);
        assertThat(backpressureService.shouldThrottle()).isTrue();
        assertThat(backpressureService.isBackpressureActive()).isTrue();

        // 4. Memory drops to 110 MB (< 60% low watermark) -> Backpressure releases
        backpressureService.setSimulatedUsedMemoryBytes(110L * 1024 * 1024);
        assertThat(backpressureService.shouldThrottle()).isFalse();
        assertThat(backpressureService.isBackpressureActive()).isFalse();
        assertThat(backpressureService.getLastThrottleReason()).isEqualTo("NONE");
    }

    @Test
    @DisplayName("Verify queue backlog threshold hysteresis: activates at 35k and holds until 10k")
    void testQueueBacklogThresholdHysteresis() {
        backpressureService.setSimulatedUsedMemoryBytes(50L * 1024 * 1024);

        // 1. Moderate queue depth (15,000 jobs)
        backpressureService.setSimulatedQueueBacklog(15000L);
        assertThat(backpressureService.shouldThrottle()).isFalse();

        // 2. Queue backlog exceeds 35,000 high watermark
        backpressureService.setSimulatedQueueBacklog(38000L);
        assertThat(backpressureService.shouldThrottle()).isTrue();
        assertThat(backpressureService.isBackpressureActive()).isTrue();
        assertThat(backpressureService.getLastThrottleReason()).contains("QUEUE_BACKLOG_EXCEEDED");

        // 3. Queue drains partially to 20,000 (between 10k and 35k) -> Hysteresis holds
        backpressureService.setSimulatedQueueBacklog(20000L);
        assertThat(backpressureService.shouldThrottle()).isTrue();

        // 4. Queue drains below 10,000 low watermark -> Releases
        backpressureService.setSimulatedQueueBacklog(8000L);
        assertThat(backpressureService.shouldThrottle()).isFalse();
        assertThat(backpressureService.isBackpressureActive()).isFalse();
    }

    @Test
    @DisplayName("Verify DriveDeltaSyncWorker defers backward crawl when backpressured, but protects forward uploads")
    void testDriveDeltaSyncWorkerBackpressureDeferral() {
        DriveDeltaSyncWorker worker = new DriveDeltaSyncWorker(
                graphClient,
                flowProducer,
                catalogRepository,
                checkpointStatePlugin,
                oxmqTemplate,
                backpressureService
        );

        String tenantId = "tenant-mid-2500";
        String driveId = "drive-101";

        // Activate backpressure
        backpressureService.setSimulatedUsedMemoryBytes(185L * 1024 * 1024);
        assertThat(backpressureService.shouldThrottle()).isTrue();

        // Case 1: Backward baseline crawl arrives during backpressure
        DriveDeltaSyncPayload backwardPayload = new DriveDeltaSyncPayload(tenantId, driveId, ScanOrigin.BACKWARD);
        int dispatchedBackward = worker.processDeltaSync(backwardPayload, "job-back-1");

        // Must yield 0 DAGs dispatched and reschedule itself with a backoff delay
        assertThat(dispatchedBackward).isEqualTo(0);
        verify(graphClient, never()).fetchDeltaChanges(anyString(), anyString(), any());

        ArgumentCaptor<JobOptions> optCaptor = ArgumentCaptor.forClass(JobOptions.class);
        verify(oxmqTemplate).send(
                eq(OxDocQueueConstants.QUEUE_DRIVE_DELTA),
                anyString(),
                eq(backwardPayload),
                optCaptor.capture()
        );
        JobOptions capturedOpts = optCaptor.getValue();
        assertThat(capturedOpts.getPriority()).isEqualTo(10);
        assertThat(capturedOpts.getDelayMs()).isEqualTo(2500L);

        // Case 2: Priority 1 Forward Scan (real-time user upload) arrives during backpressure
        // Forward scans must NEVER be blocked by memory backpressure unless hard OOM
        DriveItem urgentFile = new DriveItem("item-fwd", driveId, "Critical_Board_Minutes.docx", "/Docs/Critical.docx",
                2048L, "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "https://sharepoint.com/file", "https://blob.core.windows.net/file", false, "e1", "c1");

        when(graphClient.fetchDeltaChanges(tenantId, driveId, null))
                .thenReturn(new DeltaSyncResult(List.of(urgentFile), List.of(), "token-v2"));

        DriveDeltaSyncPayload forwardPayload = new DriveDeltaSyncPayload(tenantId, driveId, ScanOrigin.FORWARD);
        int dispatchedForward = worker.processDeltaSync(forwardPayload, "job-fwd-1");

        // Forward upload must proceed immediately (1 DAG dispatched)
        assertThat(dispatchedForward).isEqualTo(1);
        verify(graphClient, times(1)).fetchDeltaChanges(tenantId, driveId, null);
        verify(flowProducer, times(1)).add(any());
    }
}
