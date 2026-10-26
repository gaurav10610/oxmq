package io.oxmq.tracing;

import io.oxmq.interceptor.ErrorDecision;
import io.oxmq.interceptor.InterceptorDecision;
import io.oxmq.interceptor.JobInterceptor;
import io.oxmq.model.Job;
import java.time.Duration;
import org.slf4j.MDC;

/**
 * Standard distributed tracing interceptor for OxMQ workers.
 * Automatically extracts W3C TraceContext from incoming jobs, binds traceId and spanId
 * to SLF4J MDC during processing, and cleans up context upon completion.
 */
public class TracingJobInterceptor implements JobInterceptor {

    public static final String DEFAULT_MDC_TRACE_ID_KEY = "traceId";
    public static final String DEFAULT_MDC_SPAN_ID_KEY = "spanId";

    private final String traceIdKey;
    private final String spanIdKey;
    private final boolean recordJobLog;

    public TracingJobInterceptor() {
        this(DEFAULT_MDC_TRACE_ID_KEY, DEFAULT_MDC_SPAN_ID_KEY, false);
    }

    public TracingJobInterceptor(String traceIdKey, String spanIdKey) {
        this(traceIdKey, spanIdKey, false);
    }

    public TracingJobInterceptor(String traceIdKey, String spanIdKey, boolean recordJobLog) {
        this.traceIdKey = traceIdKey != null ? traceIdKey : DEFAULT_MDC_TRACE_ID_KEY;
        this.spanIdKey = spanIdKey != null ? spanIdKey : DEFAULT_MDC_SPAN_ID_KEY;
        this.recordJobLog = recordJobLog;
    }

    @Override
    public InterceptorDecision beforeProcess(Job<?> job) {
        TraceContext ctx = job.getTraceContext();
        if (ctx != null) {
            MDC.put(traceIdKey, ctx.traceId());
            MDC.put(spanIdKey, ctx.spanId());
            if (recordJobLog) {
                try {
                    job.log("W3C OTel Trace: traceId=" + ctx.traceId() + ", spanId=" + ctx.spanId() + " (traceparent=" + ctx.asTraceparent() + ")");
                } catch (Exception ignored) {}
            }
        }
        return InterceptorDecision.proceed();
    }

    @Override
    public void afterProcess(Job<?> job, Object result, Duration executionDuration) {
        MDC.remove(traceIdKey);
        MDC.remove(spanIdKey);
    }

    @Override
    public ErrorDecision onError(Job<?> job, Throwable error, Duration duration) {
        MDC.remove(traceIdKey);
        MDC.remove(spanIdKey);
        return ErrorDecision.bubbleUp();
    }
}
