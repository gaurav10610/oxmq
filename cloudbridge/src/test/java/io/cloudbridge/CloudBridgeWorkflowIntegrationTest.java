package io.cloudbridge;

import io.cloudbridge.dto.SyncRequest;
import io.cloudbridge.service.SyncWorkflowService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
public class CloudBridgeWorkflowIntegrationTest {

    @Autowired
    private SyncWorkflowService workflowService;

    @Test
    void testFullSyncWorkflowExecution() throws Exception {
        SyncRequest request = new SyncRequest();
        request.setSource("github:gaurav10610/oxmq");
        request.setDestination("dropbox:/backups/oxmq");
        request.setUseVirtualThreads(true);

        String syncId = workflowService.triggerSync(request);
        assertThat(syncId).isNotNull().startsWith("sync-");

        // Wait up to 10 seconds for 8 child file transfers to complete and parent to compile manifest
        boolean completed = false;
        Map<String, Object> details = null;

        for (int i = 0; i < 40; i++) {
            Thread.sleep(250);
            details = workflowService.getSyncDetails(syncId);
            if (details != null && Boolean.TRUE.equals(details.get("isCompleted"))) {
                completed = true;
                break;
            }
        }

        assertThat(completed).isTrue();
        assertThat(details).isNotNull();
        assertThat(details.get("status")).isEqualTo("COMPLETED");
        assertThat(details.get("completedFiles")).isEqualTo(8);
        assertThat(details.get("progress")).isEqualTo(100);

        Map<String, Long> stats = workflowService.getQueueStats();
        assertThat(stats).isNotNull();
    }
}
