package io.oxmq;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class QueueEventsTest {

    @Test
    void testQueueEventsInitialization() {
        QueueEvents events = new QueueEvents("test-queue", "redis://localhost:6379");
        assertThat(events.getQueueName()).isEqualTo("test-queue");
        assertThat(events.isRunning()).isFalse();

        events.onWaiting(jobId -> {});
        events.onCompleted((jobId, result) -> {});
        events.onFailed((jobId, reason) -> {});
        events.onProgress((jobId, progress) -> {});
        events.onRetried(jobId -> {});
    }
}
