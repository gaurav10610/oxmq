package io.oxmq.tracing;

import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * W3C Trace Context representation (traceparent and tracestate) matching BullMQ OpenTelemetry distributed tracing standards.
 * Standard format: version(2 hex)-trace_id(32 hex)-parent_id/span_id(16 hex)-trace_flags(2 hex)
 * E.g., "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
 */
public record TraceContext(String version, String traceId, String spanId, String traceFlags, String tracestate) {

    public TraceContext {
        version = version != null ? version : "00";
        traceId = Objects.requireNonNull(traceId, "traceId must not be null");
        spanId = Objects.requireNonNull(spanId, "spanId must not be null");
        traceFlags = traceFlags != null ? traceFlags : "01";
    }

    /**
     * Formats this context into a standard W3C 'traceparent' header string.
     */
    public String asTraceparent() {
        return version + "-" + traceId + "-" + spanId + "-" + traceFlags;
    }

    /**
     * Checks if the sampled flag (01) is set.
     */
    public boolean isSampleed() {
        return "01".equals(traceFlags);
    }

    /**
     * Creates a new child TraceContext with a freshly generated 16-hex-character span ID,
     * preserving the current traceId and flags.
     */
    public TraceContext createChildSpan() {
        byte[] spanBytes = new byte[8];
        ThreadLocalRandom.current().nextBytes(spanBytes);
        String newSpanId = HexFormat.of().formatHex(spanBytes);
        return new TraceContext(this.version, this.traceId, newSpanId, this.traceFlags, this.tracestate);
    }

    /**
     * Generates a completely new root TraceContext with 128-bit trace ID and 64-bit span ID.
     */
    public static TraceContext generateNew() {
        byte[] traceBytes = new byte[16];
        byte[] spanBytes = new byte[8];
        ThreadLocalRandom current = ThreadLocalRandom.current();
        current.nextBytes(traceBytes);
        current.nextBytes(spanBytes);
        String traceId = HexFormat.of().formatHex(traceBytes);
        String spanId = HexFormat.of().formatHex(spanBytes);
        return new TraceContext("00", traceId, spanId, "01", null);
    }

    /**
     * Parses a W3C traceparent header string (e.g. "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").
     * Returns null if string is malformed or invalid.
     */
    public static TraceContext parse(String traceparent, String tracestate) {
        if (traceparent == null || traceparent.isBlank()) {
            return null;
        }
        String[] parts = traceparent.trim().split("-");
        if (parts.length != 4) {
            return null;
        }
        String version = parts[0];
        String traceId = parts[1];
        String spanId = parts[2];
        String flags = parts[3];
        if (traceId.length() != 32 || spanId.length() != 16) {
            return null;
        }
        return new TraceContext(version, traceId, spanId, flags, tracestate);
    }

    /**
     * Extracts TraceContext from a Job's options or metadata.
     */
    public static TraceContext extract(Job<?> job) {
        if (job == null) return null;
        String tp = job.getTraceparent();
        String ts = job.getTracestate();
        if (tp != null && !tp.isBlank()) {
            TraceContext parsed = parse(tp, ts);
            if (parsed != null) return parsed;
        }
        // Fallback: check if telemetry map has traceId or traceparent directly
        if (job.getOpts() != null && job.getOpts().getTelemetry() != null) {
            Object tpObj = job.getOpts().getTelemetry().get("traceparent");
            if (tpObj != null && !tpObj.toString().isBlank()) {
                TraceContext parsed = parse(tpObj.toString().trim(), ts);
                if (parsed != null) return parsed;
            }
            Object traceIdObj = job.getOpts().getTelemetry().get("traceId");
            if (traceIdObj != null && !traceIdObj.toString().isBlank()) {
                String tid = traceIdObj.toString().trim();
                Object spanIdObj = job.getOpts().getTelemetry().get("spanId");
                String sid = (spanIdObj != null && !spanIdObj.toString().isBlank()) ? spanIdObj.toString().trim() : generateNew().spanId();
                return new TraceContext("00", tid, sid, "01", ts);
            }
        }
        // Fallback: check if job data has traceId
        if (job.getData() instanceof Map<?, ?> map) {
            Object tid = map.get("traceId");
            if (tid != null && !tid.toString().isBlank()) {
                return new TraceContext("00", tid.toString().trim(), generateNew().spanId(), "01", ts);
            }
        }
        return null;
    }

    /**
     * Extracts TraceContext from SLF4J MDC using the default keys ("traceId", "spanId").
     * If traceId is present, creates a valid TraceContext. If spanId is missing, generates a fresh span ID.
     */
    public static TraceContext fromMdc() {
        return fromMdc("traceId", "spanId");
    }

    /**
     * Extracts TraceContext from SLF4J MDC using custom keys.
     */
    public static TraceContext fromMdc(String traceIdKey, String spanIdKey) {
        String traceId = org.slf4j.MDC.get(traceIdKey != null ? traceIdKey : "traceId");
        if (traceId == null || traceId.isBlank()) {
            return null;
        }
        traceId = traceId.trim();
        String spanId = org.slf4j.MDC.get(spanIdKey != null ? spanIdKey : "spanId");
        if (spanId != null && !spanId.isBlank()) {
            spanId = spanId.trim();
        } else {
            byte[] spanBytes = new byte[8];
            ThreadLocalRandom.current().nextBytes(spanBytes);
            spanId = HexFormat.of().formatHex(spanBytes);
        }
        return new TraceContext("00", traceId, spanId, "01", null);
    }

    /**
     * Injects this TraceContext into a JobOptions.Builder.
     */
    public void inject(JobOptions.Builder builder) {
        if (builder != null) {
            builder.traceparent(asTraceparent());
            if (tracestate != null) {
                builder.tracestate(tracestate);
            }
            Map<String, Object> tel = new java.util.HashMap<>();
            tel.put("traceparent", asTraceparent());
            tel.put("traceId", traceId());
            tel.put("spanId", spanId());
            if (tracestate != null) {
                tel.put("tracestate", tracestate);
            }
            builder.telemetry(tel);
        }
    }

    /**
     * Injects this TraceContext into a JobOptions instance.
     */
    public void inject(JobOptions options) {
        if (options != null) {
            options.setTraceparent(asTraceparent());
            if (tracestate != null) {
                options.setTracestate(tracestate);
            }
            Map<String, Object> tel = new java.util.HashMap<>();
            tel.put("traceparent", asTraceparent());
            tel.put("traceId", traceId());
            tel.put("spanId", spanId());
            if (tracestate != null) {
                tel.put("tracestate", tracestate);
            }
            options.setTelemetry(tel);
        }
    }
}
