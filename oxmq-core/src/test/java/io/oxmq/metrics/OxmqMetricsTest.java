package io.oxmq.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class OxmqMetricsTest {

    @Test
    void testMetricsRecording() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OxmqMetrics metrics = new OxmqMetrics(registry);

        metrics.recordJobEnqueued("emails");
        metrics.recordJobEnqueued("emails");
        metrics.recordJobCompleted("emails", Duration.ofMillis(120));
        metrics.recordJobFailed("emails", Duration.ofMillis(50), "NullPointerException");
        metrics.recordJobRetried("emails");
        metrics.recordJobStalled("emails");
        metrics.recordJobWaitTime("emails", Duration.ofMillis(30));

        assertThat(registry.get("oxmq.jobs.enqueued").tag("queue", "emails").counter().count()).isEqualTo(2.0);
        assertThat(registry.get("oxmq.jobs.completed").tag("queue", "emails").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("oxmq.jobs.failed").tag("queue", "emails").tag("error", "NullPointerException").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("oxmq.jobs.retried").tag("queue", "emails").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("oxmq.jobs.stalled").tag("queue", "emails").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("oxmq.job.duration").tag("queue", "emails").timer().count()).isEqualTo(2L);
        assertThat(registry.get("oxmq.job.wait_time").tag("queue", "emails").timer().count()).isEqualTo(1L);
    }
}
