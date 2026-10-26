package io.oxmq.oxdoc.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.oxdoc.queue.OxDocQueueConstants;
import io.oxmq.spring.OxmqTemplate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Enforces a strict Redis Memory Ceiling (default 200 MB) and downstream queue watermark backpressure
 * across OxDoc 365 ingestion pipelines.
 *
 * <p>Dual-Sensing Architecture:
 * <ol>
 *   <li><b>Real Redis Memory Sensor</b>: Samples Redis {@code used_memory} via {@code INFO memory} directly.
 *       Triggers when used memory reaches 85% of ceiling (170 MB), and recovers when below 60% (120 MB).</li>
 *   <li><b>Downstream Queue Backlog Sensor</b>: Aggregates waiting and prioritized jobs across downstream
 *       processing queues. Triggers when backlog &gt; 35,000 jobs and recovers when &lt; 10,000 jobs.</li>
 * </ol>
 *
 * <p><b>Zero Data Loss Guarantee</b>: Backpressure never drops or discards jobs or delta tokens.
 * It paces continuation pages and delays baseline crawl execution using OxMQ's native delayed job primitives,
 * allowing downstream workers to drain completed jobs and maintain strict memory limits.
 */
@Service
public class BackpressureService {

    private static final Logger log = LoggerFactory.getLogger(BackpressureService.class);

    private final RedisConnectionManager connectionManager;
    private final OxmqTemplate oxmqTemplate;

    private final boolean enabled;
    private final long maxRedisMemoryBytes;
    private final double highMemoryThresholdPct;
    private final double lowMemoryThresholdPct;
    private final long highQueueWatermark;
    private final long lowQueueWatermark;
    private final long backoffDelayMs;
    private final long sampleIntervalMs;
    private final List<String> checkQueues;

    private volatile long cachedUsedMemoryBytes = -1L;
    private volatile long lastSampleTimeMs = 0L;
    private final AtomicBoolean isBackpressured = new AtomicBoolean(false);
    private final AtomicLong totalYields = new AtomicLong(0);
    private volatile String lastThrottleReason = "NONE";

    // Test simulation hooks
    private volatile Long simulatedUsedMemoryBytes = null;
    private volatile Long simulatedQueueBacklog = null;

    @Autowired
    public BackpressureService(
            @Autowired(required = false) RedisConnectionManager connectionManager,
            @Autowired(required = false) OxmqTemplate oxmqTemplate,
            @Autowired(required = false) MeterRegistry meterRegistry,
            @Value("${oxdoc.backpressure.enabled:true}") boolean enabled,
            @Value("${oxdoc.backpressure.max-redis-memory-bytes:209715200}") long maxRedisMemoryBytes, // 200 MB default
            @Value("${oxdoc.backpressure.high-memory-threshold-pct:0.85}") double highMemoryThresholdPct,
            @Value("${oxdoc.backpressure.low-memory-threshold-pct:0.60}") double lowMemoryThresholdPct,
            @Value("${oxdoc.backpressure.high-queue-watermark:35000}") long highQueueWatermark,
            @Value("${oxdoc.backpressure.low-queue-watermark:10000}") long lowQueueWatermark,
            @Value("${oxdoc.backpressure.backoff-delay-ms:2500}") long backoffDelayMs,
            @Value("${oxdoc.backpressure.sample-interval-ms:1000}") long sampleIntervalMs
    ) {
        this.connectionManager = connectionManager;
        this.oxmqTemplate = oxmqTemplate;
        this.enabled = enabled;
        this.maxRedisMemoryBytes = maxRedisMemoryBytes;
        this.highMemoryThresholdPct = highMemoryThresholdPct;
        this.lowMemoryThresholdPct = lowMemoryThresholdPct;
        this.highQueueWatermark = highQueueWatermark;
        this.lowQueueWatermark = lowQueueWatermark;
        this.backoffDelayMs = backoffDelayMs;
        this.sampleIntervalMs = sampleIntervalMs;
        this.checkQueues = List.of(
                OxDocQueueConstants.QUEUE_CONTENT_CATEGORIZATION,
                OxDocQueueConstants.QUEUE_DOCUMENT_EXTRACTION,
                OxDocQueueConstants.QUEUE_CATALOG_INDEX
        );

        if (meterRegistry != null) {
            registerMetrics(meterRegistry);
        }

        log.info("Initialized OxDoc BackpressureService: ceiling={} MB, highMemTrigger={} MB ({}%), " +
                 "lowMemRecovery={} MB ({}%), highQueueTrigger={}, lowQueueRecovery={}, backoffDelay={}ms, enabled={}",
                toMB(maxRedisMemoryBytes),
                toMB((long) (maxRedisMemoryBytes * highMemoryThresholdPct)), Math.round(highMemoryThresholdPct * 100),
                toMB((long) (maxRedisMemoryBytes * lowMemoryThresholdPct)), Math.round(lowMemoryThresholdPct * 100),
                highQueueWatermark, lowQueueWatermark, backoffDelayMs, enabled);
    }

    private void registerMetrics(MeterRegistry registry) {
        Gauge.builder("oxdoc.redis.memory.used.bytes", this::getRedisUsedMemoryBytes)
                .description("Current Redis used memory in bytes (from INFO memory)")
                .register(registry);

        Gauge.builder("oxdoc.redis.memory.ceiling.bytes", () -> maxRedisMemoryBytes)
                .description("Strict configured Redis memory ceiling in bytes")
                .register(registry);

        Gauge.builder("oxdoc.backpressure.active", () -> isBackpressured.get() ? 1.0 : 0.0)
                .description("Whether ingestion backpressure is currently active (1=active, 0=normal)")
                .register(registry);

        Gauge.builder("oxdoc.downstream.backlog.jobs", this::getDownstreamBacklogJobs)
                .description("Aggregate in-flight and waiting jobs across downstream processing queues")
                .register(registry);

        Counter.builder("oxdoc.backpressure.yields.total")
                .description("Total number of times delta ingestion was throttled or delayed to preserve memory")
                .register(registry);
    }

    /**
     * Determines whether upstream ingestion workers should throttle/delay execution.
     * Implements hysteresis to prevent rapid flapping around watermark thresholds.
     */
    public boolean shouldThrottle() {
        if (!enabled) {
            return false;
        }

        long usedBytes = getRedisUsedMemoryBytes();
        long backlog = getDownstreamBacklogJobs();

        long highMemoryBytes = (long) (maxRedisMemoryBytes * highMemoryThresholdPct);
        long lowMemoryBytes = (long) (maxRedisMemoryBytes * lowMemoryThresholdPct);

        boolean memoryExceeded = usedBytes >= highMemoryBytes;
        boolean queueExceeded = backlog >= highQueueWatermark;

        if (!isBackpressured.get()) {
            if (memoryExceeded || queueExceeded) {
                if (isBackpressured.compareAndSet(false, true)) {
                    totalYields.incrementAndGet();
                    lastThrottleReason = memoryExceeded
                            ? String.format("REDIS_MEM_EXCEEDED: %d MB >= %d MB ceiling threshold", toMB(usedBytes), toMB(highMemoryBytes))
                            : String.format("QUEUE_BACKLOG_EXCEEDED: %d jobs >= %d watermark", backlog, highQueueWatermark);

                    log.warn("[BACKPRESSURE ACTIVATED] Strict memory ceiling 200MB guarded. Reason: {}. " +
                             "used_memory={} MB (ceiling: {} MB), downstream_backlog={} jobs. Pacing ingestion.",
                            lastThrottleReason, toMB(usedBytes), toMB(maxRedisMemoryBytes), backlog);
                }
            }
        } else {
            boolean memoryHealthy = usedBytes < lowMemoryBytes;
            boolean queueHealthy = backlog < lowQueueWatermark;

            if (memoryHealthy && queueHealthy) {
                if (isBackpressured.compareAndSet(true, false)) {
                    lastThrottleReason = "NONE";
                    log.info("[BACKPRESSURE RELEASED] Redis memory and downstream backlog recovered below low watermarks. " +
                             "used_memory={} MB, downstream_backlog={} jobs. Resuming full ingestion throughput.",
                            toMB(usedBytes), backlog);
                }
            }
        }

        return isBackpressured.get();
    }

    /**
     * Returns real-time Redis memory usage in bytes by sampling {@code INFO memory}.
     * Caches the value for {@link #sampleIntervalMs} to avoid unnecessary Redis round-trips.
     */
    public long getRedisUsedMemoryBytes() {
        if (simulatedUsedMemoryBytes != null) {
            return simulatedUsedMemoryBytes;
        }

        long now = System.currentTimeMillis();
        if (now - lastSampleTimeMs < sampleIntervalMs && cachedUsedMemoryBytes >= 0) {
            return cachedUsedMemoryBytes;
        }

        if (connectionManager == null) {
            return 0L;
        }

        try {
            String info = connectionManager.getCommandConnection().sync().info("memory");
            if (info != null && !info.isBlank()) {
                for (String line : info.split("\\r?\\n")) {
                    if (line.startsWith("used_memory:")) {
                        long bytes = Long.parseLong(line.substring("used_memory:".length()).trim());
                        this.cachedUsedMemoryBytes = bytes;
                        this.lastSampleTimeMs = now;
                        return bytes;
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Could not sample Redis memory via INFO memory: {}", e.getMessage());
        }

        return cachedUsedMemoryBytes >= 0 ? cachedUsedMemoryBytes : 0L;
    }

    /**
     * Returns aggregate waiting and prioritized jobs across downstream queues.
     */
    public long getDownstreamBacklogJobs() {
        if (simulatedQueueBacklog != null) {
            return simulatedQueueBacklog;
        }

        if (oxmqTemplate == null) {
            return 0L;
        }

        long total = 0L;
        for (String queueName : checkQueues) {
            try {
                Map<String, Long> counts = oxmqTemplate.getQueue(queueName).getJobCounts();
                total += counts.getOrDefault("wait", 0L);
                total += counts.getOrDefault("prioritized", 0L);
                total += counts.getOrDefault("waiting-children", 0L);
            } catch (Exception e) {
                log.debug("Failed querying job counts for queue {}: {}", queueName, e.getMessage());
            }
        }
        return total;
    }

    /**
     * Returns backpressure telemetry map for the live dashboard and REST endpoints.
     */
    public Map<String, Object> getTelemetry() {
        long usedBytes = getRedisUsedMemoryBytes();
        double usedMB = Math.round(usedBytes / (1024.0 * 1024.0) * 10.0) / 10.0;
        double ceilingMB = Math.round(maxRedisMemoryBytes / (1024.0 * 1024.0) * 10.0) / 10.0;
        double usagePct = maxRedisMemoryBytes > 0
                ? Math.round((double) usedBytes / maxRedisMemoryBytes * 1000.0) / 10.0
                : 0.0;

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("enabled", enabled);
        m.put("redisUsedMemoryBytes", usedBytes);
        m.put("redisUsedMemoryMB", usedMB);
        m.put("redisMaxMemoryCeilingMB", ceilingMB);
        m.put("memoryUsagePercent", usagePct);
        m.put("highMemoryThresholdMB", toMB((long) (maxRedisMemoryBytes * highMemoryThresholdPct)));
        m.put("lowMemoryThresholdMB", toMB((long) (maxRedisMemoryBytes * lowMemoryThresholdPct)));
        m.put("downstreamBacklogJobs", getDownstreamBacklogJobs());
        m.put("highQueueWatermark", highQueueWatermark);
        m.put("lowQueueWatermark", lowQueueWatermark);
        m.put("isBackpressureActive", isBackpressured.get());
        m.put("totalYields", totalYields.get());
        m.put("lastThrottleReason", lastThrottleReason);
        m.put("backoffDelayMs", backoffDelayMs);
        return m;
    }

    public long getBackoffDelayMs() {
        return backoffDelayMs;
    }

    public boolean isBackpressureActive() {
        return isBackpressured.get();
    }

    public long getMaxRedisMemoryBytes() {
        return maxRedisMemoryBytes;
    }

    public double getRedisUsedMemoryMB() {
        return Math.round(getRedisUsedMemoryBytes() / (1024.0 * 1024.0) * 10.0) / 10.0;
    }

    public double getRedisMaxMemoryCeilingMB() {
        return Math.round(maxRedisMemoryBytes / (1024.0 * 1024.0) * 10.0) / 10.0;
    }

    public String getLastThrottleReason() {
        return lastThrottleReason;
    }

    public long getTotalYields() {
        return totalYields.get();
    }

    // Simulation helpers for unit/integration tests
    public void setSimulatedUsedMemoryBytes(Long bytes) {
        this.simulatedUsedMemoryBytes = bytes;
    }

    public void setSimulatedQueueBacklog(Long backlog) {
        this.simulatedQueueBacklog = backlog;
    }

    public void reset() {
        this.simulatedUsedMemoryBytes = null;
        this.simulatedQueueBacklog = null;
        this.isBackpressured.set(false);
        this.totalYields.set(0);
        this.lastThrottleReason = "NONE";
        this.cachedUsedMemoryBytes = -1L;
    }

    private static long toMB(long bytes) {
        return Math.round(bytes / (1024.0 * 1024.0));
    }
}
