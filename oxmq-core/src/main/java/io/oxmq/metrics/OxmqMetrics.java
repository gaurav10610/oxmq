package io.oxmq.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * Native performance telemetry and metrics engine for OxMQ using Micrometer.
 * Configured with Prometheus histogram buckets for cluster-wide quantile aggregation.
 */
public class OxmqMetrics {

    private final MeterRegistry registry;
    private final ConcurrentMap<String, Counter> enqueuedCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> completedCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> failedCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> retriedCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> stalledCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> durationTimers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> waitTimeTimers = new ConcurrentHashMap<>();
    private volatile boolean groupMetricsEnabled = false;

    public OxmqMetrics() {
        this(Metrics.globalRegistry);
    }

    public OxmqMetrics(MeterRegistry registry) {
        this.registry = registry != null ? registry : new CompositeMeterRegistry();
    }

    public boolean isGroupMetricsEnabled() {
        return groupMetricsEnabled;
    }

    public void setGroupMetricsEnabled(boolean groupMetricsEnabled) {
        this.groupMetricsEnabled = groupMetricsEnabled;
    }

    public void recordJobEnqueued(String queueName) {
        enqueuedCounters.computeIfAbsent(queueName, q -> Counter.builder("oxmq.jobs.enqueued")
                .description("Total number of jobs enqueued")
                .tag("queue", q)
                .register(registry))
                .increment();
    }

    public void recordJobCompleted(String queueName, Duration duration) {
        recordJobCompleted(queueName, null, duration);
    }

    public void recordJobCompleted(String queueName, String groupId, Duration duration) {
        boolean hasGroup = groupMetricsEnabled && groupId != null && !groupId.isBlank();
        String counterKey = hasGroup ? queueName + "::" + groupId : queueName;
        completedCounters.computeIfAbsent(counterKey, k -> {
            var builder = Counter.builder("oxmq.jobs.completed")
                    .description("Total number of successfully processed jobs")
                    .tag("queue", queueName);
            if (hasGroup) {
                builder.tag("group", groupId);
            }
            return builder.register(registry);
        }).increment();

        if (duration != null) {
            getOrCreateDurationTimer(queueName, hasGroup ? groupId : null).record(duration);
        }
    }

    public void recordJobFailed(String queueName, Duration duration, String errorType) {
        recordJobFailed(queueName, null, duration, errorType);
    }

    public void recordJobFailed(String queueName, String groupId, Duration duration, String errorType) {
        boolean hasGroup = groupMetricsEnabled && groupId != null && !groupId.isBlank();
        String err = errorType != null ? errorType : "Unknown";
        String counterKey = hasGroup ? queueName + "::" + groupId + "::" + err : queueName + "::" + err;
        failedCounters.computeIfAbsent(counterKey, k -> {
            var builder = Counter.builder("oxmq.jobs.failed")
                    .description("Total number of failed jobs")
                    .tag("queue", queueName)
                    .tag("error", err);
            if (hasGroup) {
                builder.tag("group", groupId);
            }
            return builder.register(registry);
        }).increment();

        if (duration != null) {
            getOrCreateDurationTimer(queueName, hasGroup ? groupId : null).record(duration);
        }
    }

    public void recordJobRetried(String queueName) {
        retriedCounters.computeIfAbsent(queueName, q -> Counter.builder("oxmq.jobs.retried")
                .description("Total number of job retries scheduled")
                .tag("queue", q)
                .register(registry))
                .increment();
    }

    public void recordJobStalled(String queueName) {
        stalledCounters.computeIfAbsent(queueName, q -> Counter.builder("oxmq.jobs.stalled")
                .description("Total number of stalled jobs detected and recovered")
                .tag("queue", q)
                .register(registry))
                .increment();
    }

    public void recordJobWaitTime(String queueName, Duration waitTime) {
        if (waitTime != null && !waitTime.isNegative()) {
            getOrCreateWaitTimeTimer(queueName).record(waitTime);
        }
    }

    public void registerActiveGauge(String queueName, Supplier<Number> activeSupplier) {
        Gauge.builder("oxmq.jobs.active", activeSupplier)
                .description("Current number of active jobs executing")
                .tag("queue", queueName)
                .register(registry);
    }

    public void registerWaitingGauge(String queueName, Supplier<Number> waitingSupplier) {
        Gauge.builder("oxmq.jobs.waiting", waitingSupplier)
                .description("Current number of jobs in wait list")
                .tag("queue", queueName)
                .register(registry);
    }

    public void registerDelayedGauge(String queueName, Supplier<Number> delayedSupplier) {
        Gauge.builder("oxmq.jobs.delayed", delayedSupplier)
                .description("Current number of delayed jobs")
                .tag("queue", queueName)
                .register(registry);
    }

    private Timer getOrCreateDurationTimer(String queueName, String groupId) {
        String key = (groupId != null && groupMetricsEnabled) ? queueName + "::" + groupId : queueName;
        return durationTimers.computeIfAbsent(key, k -> {
            var builder = Timer.builder("oxmq.job.duration")
                    .description("Execution duration of processed jobs")
                    .tag("queue", queueName)
                    .minimumExpectedValue(Duration.ofMillis(1))
                    .maximumExpectedValue(Duration.ofMinutes(10));
            if (groupId != null && groupMetricsEnabled) {
                builder.tag("group", groupId);
            }
            return builder.publishPercentileHistogram().register(registry);
        });
    }

    private Timer getOrCreateWaitTimeTimer(String queueName) {
        return waitTimeTimers.computeIfAbsent(queueName, q -> Timer.builder("oxmq.job.wait_time")
                .description("Time spent waiting in queue before execution")
                .tag("queue", q)
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofMinutes(10))
                .publishPercentileHistogram()
                .register(registry));
    }

    public MeterRegistry getRegistry() {
        return registry;
    }
}
