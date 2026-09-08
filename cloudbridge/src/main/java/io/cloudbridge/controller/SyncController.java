package io.cloudbridge.controller;

import io.cloudbridge.dto.SyncRequest;
import io.cloudbridge.service.SyncWorkflowService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class SyncController {

    private final SyncWorkflowService workflowService;

    public SyncController(SyncWorkflowService workflowService) {
        this.workflowService = workflowService;
    }

    @PostMapping("/sync")
    public ResponseEntity<Map<String, Object>> triggerSync(@RequestBody(required = false) SyncRequest request) {
        if (request == null) {
            request = new SyncRequest();
        }
        String syncId = workflowService.triggerSync(request);

        Map<String, Object> resp = new HashMap<>();
        resp.put("syncId", syncId);
        resp.put("status", "DISPATCHED");
        resp.put("source", request.getSource());
        resp.put("destination", request.getDestination());
        resp.put("statusUrl", "/api/sync/" + syncId);
        return ResponseEntity.ok(resp);
    }

    @GetMapping("/sync/{syncId}")
    public ResponseEntity<Map<String, Object>> getStatus(@PathVariable String syncId) {
        Map<String, Object> details = workflowService.getSyncDetails(syncId);
        if (details == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(details);
    }

    @GetMapping("/queues")
    public ResponseEntity<Map<String, Long>> getQueueStats() {
        return ResponseEntity.ok(workflowService.getQueueStats());
    }
}
