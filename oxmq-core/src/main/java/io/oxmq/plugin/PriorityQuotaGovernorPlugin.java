package io.oxmq.plugin;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.oxmq.interceptor.InterceptorDecision;
import io.oxmq.interceptor.JobInterceptor;
import io.oxmq.model.Job;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Priority Quota Governor plugin for OxMQ.
 * <p>
 * Ensures real-time, interactive, or critical tasks (e.g. Priority 1 user actions) are never
 * starved by bulk background jobs (e.g. Priority 10 batch crawlers).
 * <p>
 * When remaining quota headroom for a group drops below a configured threshold (e.g. 25%),
 * lower-priority jobs automatically yield execution via non-destructive delay, preserving
 * the remaining quota headroom strictly for high-priority jobs.
 */
public class PriorityQuotaGovernorPlugin implements JobInterceptor {

    private static final Logger log = LoggerFactory.getLogger(PriorityQuotaGovernorPlugin.class);

    private final GroupRateLimiterPlugin rateLimiter;
    private final Function<Job<?>, String> groupKeyResolver;
    private final double headroomYieldThreshold;
    private final int priorityThreshold;
    private final long yieldDelayMs;
    private final MeterRegistry meterRegistry;

    private final ConcurrentMap<String, AtomicLong> yieldCounters = new ConcurrentHashMap<>();

    public PriorityQuotaGovernorPlugin(
            GroupRateLimiterPlugin rateLimiter,
            Function<Job<?>, String> groupKeyResolver,
            double headroomYieldThreshold,
            int priorityThreshold,
            long yieldDelayMs
    ) {
        this(rateLimiter, groupKeyResolver, headroomYieldThreshold, priorityThreshold, yieldDelayMs, Metrics.globalRegistry);
    }

    public PriorityQuotaGovernorPlugin(
            GroupRateLimiterPlugin rateLimiter,
            Function<Job<?>, String> groupKeyResolver,
            double headroomYieldThreshold,
            int priorityThreshold,
            long yieldDelayMs,
            MeterRegistry meterRegistry
    ) {
        this.rateLimiter = Objects.requireNonNull(rateLimiter, "rateLimiter must not be null");
        this.groupKeyResolver = Objects.requireNonNull(groupKeyResolver, "groupKeyResolver must not be null");
        this.headroomYieldThreshold = Math.max(0.01, Math.min(0.99, headroomYieldThreshold));
        this.priorityThreshold = priorityThreshold;
        this.yieldDelayMs = Math.max(20, yieldDelayMs);
        this.meterRegistry = meterRegistry != null ? meterRegistry : Metrics.globalRegistry;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public InterceptorDecision beforeProcess(Job<?> job) {
        int jobPriority = job.getOpts() != null && job.getOpts().getPriority() > 0 ? job.getOpts().getPriority() : 10;

        // High priority jobs (priority <= threshold) are exempt from yielding!
        if (jobPriority <= priorityThreshold) {
            return InterceptorDecision.proceed();
        }

        String groupKey;
        try {
            groupKey = groupKeyResolver.apply(job);
        } catch (Exception e) {
            return InterceptorDecision.proceed();
        }

        if (groupKey == null || groupKey.isBlank() || "all".equalsIgnoreCase(groupKey)) {
            return InterceptorDecision.proceed();
        }

        double headroomFraction = rateLimiter.getRemainingHeadroomPercent(groupKey) / 100.0;
        if (headroomFraction < headroomYieldThreshold) {
            yieldCounters.computeIfAbsent(groupKey, k -> new AtomicLong(0)).incrementAndGet();
            if (meterRegistry != null) {
                try {
                    Counter.builder("oxmq.plugin.governor.yields")
                            .description("Low-priority jobs yielded to protect high-priority quota headroom")
                            .tag("group", groupKey)
                            .tag("queue", job.getQueueName() != null ? job.getQueueName() : "unknown")
                            .register(meterRegistry)
                            .increment();
                } catch (Exception ignored) {}
            }
            log.debug("PriorityQuotaGovernor: Low-priority job [{}] (p={}) yielding for group [{}] (headroom: {}% < {}%)",
                    job.getId(), jobPriority, groupKey, Math.round(headroomFraction * 100.0), Math.round(headroomYieldThreshold * 100.0));
            return InterceptorDecision.delay(yieldDelayMs, true);
        }

        return InterceptorDecision.proceed();
    }

    public long getYieldCount(String groupKey) {
        AtomicLong c = yieldCounters.get(groupKey);
        return c != null ? c.get() : 0;
    }

    public boolean isYieldingActive(String groupKey) {
        if (groupKey == null || groupKey.isBlank()) return false;
        double headroomFraction = rateLimiter.getRemainingHeadroomPercent(groupKey) / 100.0;
        return headroomFraction < headroomYieldThreshold;
    }

    public double getHeadroomYieldThreshold() {
        return headroomYieldThreshold;
    }

    public static class Builder {
        private GroupRateLimiterPlugin rateLimiter;
        private Function<Job<?>, String> groupKeyResolver;
        private double headroomYieldThreshold = 0.25; // Default: 25%
        private int priorityThreshold = 1;            // Default: Priority 1 is protected
        private long yieldDelayMs = 150;
        private MeterRegistry meterRegistry;

        public Builder rateLimiter(GroupRateLimiterPlugin rateLimiter) {
            this.rateLimiter = rateLimiter;
            return this;
        }

        public Builder meterRegistry(MeterRegistry meterRegistry) {
            this.meterRegistry = meterRegistry;
            return this;
        }

        public Builder groupKeyResolver(Function<Job<?>, String> groupKeyResolver) {
            this.groupKeyResolver = groupKeyResolver;
            return this;
        }

        public Builder headroomYieldThreshold(double headroomYieldThreshold) {
            this.headroomYieldThreshold = headroomYieldThreshold;
            return this;
        }

        public Builder priorityThreshold(int priorityThreshold) {
            this.priorityThreshold = priorityThreshold;
            return this;
        }

        public Builder yieldDelayMs(long yieldDelayMs) {
            this.yieldDelayMs = yieldDelayMs;
            return this;
        }

        public PriorityQuotaGovernorPlugin build() {
            return new PriorityQuotaGovernorPlugin(rateLimiter, groupKeyResolver, headroomYieldThreshold, priorityThreshold, yieldDelayMs, meterRegistry);
        }
    }
}
