package io.oxmq.example.java17.listener;

import io.oxmq.example.java17.model.AuditLogEvent;
import io.oxmq.model.Job;
import io.oxmq.spring.annotation.OxmqBatchListener;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Demonstrates high-throughput batch consumption via {@link OxmqBatchListener}.
 * Batches multiple incoming jobs into a single transaction execution.
 */
@Component
public class AuditBatchListener {

    private static final Logger log = LoggerFactory.getLogger(AuditBatchListener.class);

    private final List<List<Job<AuditLogEvent>>> receivedBatches = new CopyOnWriteArrayList<>();

    @OxmqBatchListener(
            queue = "${app.queues.audit:java17-audit-logs}",
            batchSize = 5
    )
    public void processAuditBatch(List<Job<AuditLogEvent>> batch) {
        log.info("Received batch of {} audit logs for bulk ingestion", batch.size());
        for (Job<AuditLogEvent> job : batch) {
            AuditLogEvent event = job.getData();
            log.debug("  -> Audit log [{}]: action={}, principal={}",
                    event.eventId(), event.action(), event.principal());
        }
        receivedBatches.add(batch);
    }

    public List<List<Job<AuditLogEvent>>> getReceivedBatches() {
        return receivedBatches;
    }

    public void clear() {
        receivedBatches.clear();
    }
}
