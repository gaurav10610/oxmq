package io.oxmq.tracing;

import io.oxmq.interceptor.InterceptorDecision;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import static org.assertj.core.api.Assertions.assertThat;

public class TracingJobInterceptorTest {

    @Test
    void testMdcBindingAndCleanup() {
        TraceContext ctx = TraceContext.generateNew();
        JobOptions opts = JobOptions.builder()
                .traceparent(ctx.asTraceparent())
                .tracestate("vendor=oxmq")
                .build();

        Job<String> job = new Job<>();
        job.setId("job-trace-123");
        job.setName("test-traced-job");
        job.setOpts(opts);

        assertThat(job.getTraceContext()).isNotNull();
        assertThat(job.getTraceContext().traceId()).isEqualTo(ctx.traceId());
        assertThat(job.getTraceContext().spanId()).isEqualTo(ctx.spanId());

        TracingJobInterceptor interceptor = new TracingJobInterceptor();

        // 1. Before process -> binds to MDC
        InterceptorDecision decision = interceptor.beforeProcess(job);
        assertThat(decision.isProceed()).isTrue();
        assertThat(MDC.get("traceId")).isEqualTo(ctx.traceId());
        assertThat(MDC.get("spanId")).isEqualTo(ctx.spanId());

        // 2. After process -> cleans up MDC
        interceptor.afterProcess(job, "result", Duration.ofMillis(25));
        assertThat(MDC.get("traceId")).isNull();
        assertThat(MDC.get("spanId")).isNull();

        // 3. On error -> also cleans up MDC
        interceptor.beforeProcess(job);
        assertThat(MDC.get("traceId")).isEqualTo(ctx.traceId());
        interceptor.onError(job, new RuntimeException("simulated"), Duration.ofMillis(10));
        assertThat(MDC.get("traceId")).isNull();
        assertThat(MDC.get("spanId")).isNull();
    }
}
