package io.oxmq.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * Native performance telemetry and metrics engine for OxMQ using Micrometer.
 */
public class OxmqMetrics {

    private final MeterRegistry registry;
    private final ConcurrentMap<String, Timer> durationTimers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> waitTimeTimers = new ConcurrentHashMap<>();

    public OxmqMetrics() {
        this(Metrics.globalRegistry);
    }

    public OxmqMetrics(MeterRegistry registry) {
        this.registry = registry != null ? registry : Metrics.globalRegistry;
    }

    public void recordJobEnqueued(String queueName) {
        Counter.builder("oxmq.jobs.enqueued")
                .description("Total number of jobs enqueued")
                .tag("queue", queueName)
                .register(registry)
                .increment();
    }

    public void recordJobCompleted(String queueName, Duration duration) {
        Counter.builder("oxmq.jobs.completed")
                .description("Total number of successfully processed jobs")
                .tag("queue", queueName)
                .register(registry)
                .increment();

        if (duration != null) {
            getOrCreateDurationTimer(queueName).record(duration);
        }
    }

    public void recordJobFailed(String queueName, Duration duration, String errorType) {
        Counter.builder("oxmq.jobs.failed")
                .description("Total number of failed jobs")
                .tag("queue", queueName)
                .tag("error", errorType != null ? errorType : "Unknown")
                .register(registry)
                .increment();

        if (duration != null) {
            getOrCreateDurationTimer(queueName).record(duration);
        }
    }

    public void recordJobRetried(String queueName) {
        Counter.builder("oxmq.jobs.retried")
                .description("Total number of job retries scheduled")
                .tag("queue", queueName)
                .register(registry)
                .increment();
    }

    public void recordJobStalled(String queueName) {
        Counter.builder("oxmq.jobs.stalled")
                .description("Total number of stalled jobs detected and recovered")
                .tag("queue", queueName)
                .register(registry)
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

    private Timer getOrCreateDurationTimer(String queueName) {
        return durationTimers.computeIfAbsent(queueName, q -> Timer.builder("oxmq.job.duration")
                .description("Execution duration of processed jobs")
                .tag("queue", q)
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry));
    }

    private Timer getOrCreateWaitTimeTimer(String queueName) {
        return waitTimeTimers.computeIfAbsent(queueName, q -> Timer.builder("oxmq.job.wait_time")
                .description("Time spent waiting in queue before execution")
                .tag("queue", q)
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry));
    }

    public MeterRegistry getRegistry() {
        return registry;
    }
}
