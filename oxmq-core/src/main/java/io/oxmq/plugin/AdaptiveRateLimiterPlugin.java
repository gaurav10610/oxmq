package io.oxmq.plugin;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.exception.RateLimitCooldownException;
import io.oxmq.interceptor.ErrorDecision;
import io.oxmq.interceptor.JobInterceptor;
import io.oxmq.model.Job;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enterprise Adaptive Capacity Controller Plugin for OxMQ.
 * <p>
 * Implements Additive Increase, Multiplicative Decrease (AIMD) dynamic capacity tuning
 * with oscillation damping to prevent yo-yo throttling storms.
 * <p>
 * Features:
 * <ul>
 *   <li>Auto-downgrade on HTTP 429, 503, or rate limit exceptions.</li>
 *   <li>Oscillation damping lock to freeze upgrades after a throttle event.</li>
 *   <li>Auto-upgrade after sustained clean execution windows or consecutive successes.</li>
 *   <li>Static override support (pinned capacity for VIP or reserved groups).</li>
 *   <li>Cluster-wide synchronization via Redis with graceful in-memory fallback.</li>
 *   <li>Seamless composition with {@link GroupRateLimiterPlugin} via {@link #asLimitResolver()}.</li>
 * </ul>
 */
public class AdaptiveRateLimiterPlugin implements JobInterceptor {

    private static final Logger log = LoggerFactory.getLogger(AdaptiveRateLimiterPlugin.class);

    private final RedisConnectionManager connectionManager;
    private final String keyPrefix;
    private final Function<Job<?>, String> groupKeyResolver;
    private final int defaultInitialLimit;
    private final int minLimit;
    private final int maxLimit;
    private final int increaseStep;
    private final double backoffFactor;
    private final Duration dampingDuration;
    private final int cleanThreshold;
    private final Predicate<Throwable> rateLimitMatcher;
    private final MeterRegistry meterRegistry;

    // Local state caches (backed by Redis when available)
    private final ConcurrentMap<String, AtomicInteger> currentLimits = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Integer> staticOverrides = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> dampingExpiryTimestamps = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> lastNegativeCheckTimestamps = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicInteger> cleanCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicLong> upgradeCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AtomicLong> downgradeCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> upgradeMetricCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> downgradeMetricCounters = new ConcurrentHashMap<>();

    public AdaptiveRateLimiterPlugin(
            RedisConnectionManager connectionManager,
            String keyPrefix,
            Function<Job<?>, String> groupKeyResolver,
            int defaultInitialLimit,
            int minLimit,
            int maxLimit,
            int increaseStep,
            double backoffFactor,
            Duration dampingDuration,
            int cleanThreshold,
            Predicate<Throwable> rateLimitMatcher,
            MeterRegistry meterRegistry
    ) {
        this.connectionManager = connectionManager;
        this.keyPrefix = keyPrefix != null ? keyPrefix : "oxmq:adaptive";
        this.groupKeyResolver = Objects.requireNonNull(groupKeyResolver, "groupKeyResolver must not be null");
        this.defaultInitialLimit = Math.max(1, defaultInitialLimit);
        this.minLimit = Math.max(1, minLimit);
        this.maxLimit = Math.max(this.minLimit, maxLimit);
        this.increaseStep = Math.max(1, increaseStep);
        this.backoffFactor = Math.min(0.99, Math.max(0.1, backoffFactor));
        this.dampingDuration = dampingDuration != null ? dampingDuration : Duration.ofSeconds(30);
        this.cleanThreshold = Math.max(1, cleanThreshold);
        this.rateLimitMatcher = rateLimitMatcher != null
                ? rateLimitMatcher
                : t -> t instanceof RateLimitCooldownException;
        this.meterRegistry = meterRegistry != null ? meterRegistry : Metrics.globalRegistry;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void afterProcess(Job<?> job, Object result, Duration duration) {
        String groupKey = resolveGroupKey(job);
        if (groupKey == null || isStaticOverride(groupKey)) {
            return;
        }

        recordCleanExecution(groupKey);
    }

    @Override
    public ErrorDecision onError(Job<?> job, Throwable error, Duration duration) {
        String groupKey = resolveGroupKey(job);
        if (groupKey == null) {
            return ErrorDecision.bubbleUp();
        }

        if (error instanceof RateLimitCooldownException || rateLimitMatcher.test(error)) {
            recordThrottle(groupKey);
        }

        return ErrorDecision.bubbleUp();
    }

    /**
     * Records a rate-limiting throttle event for a group, triggering Multiplicative Decrease and damping lock.
     */
    public synchronized void recordThrottle(String groupKey) {
        if (groupKey == null || isStaticOverride(groupKey)) {
            return;
        }

        int current = getLimit(groupKey);
        int downgraded = Math.max(minLimit, (int) Math.round(current * backoffFactor));

        // Activate oscillation damping lock
        long dampingUntil = System.currentTimeMillis() + dampingDuration.toMillis();
        dampingExpiryTimestamps.put(groupKey, dampingUntil);
        cleanCounters.computeIfAbsent(groupKey, k -> new AtomicInteger(0)).set(0);

        if (downgraded != current) {
            currentLimits.computeIfAbsent(groupKey, k -> new AtomicInteger(current)).set(downgraded);
            long downs = downgradeCounters.computeIfAbsent(groupKey, k -> new AtomicLong(0)).incrementAndGet();
            syncLimitToRedis(groupKey, downgraded, dampingUntil);

            incrementDowngradeMetric(groupKey);

            log.warn("AdaptiveRateLimiter: Group [{}] throttled! Multiplicative Decrease: {} -> {} ops/s. Upgrades locked for {}s.",
                    groupKey, current, downgraded, dampingDuration.toSeconds());
        } else {
            log.warn("AdaptiveRateLimiter: Group [{}] throttled at floor ({} ops/s). Upgrades locked for {}s.",
                    groupKey, minLimit, dampingDuration.toSeconds());
        }
    }

    private void incrementDowngradeMetric(String groupKey) {
        if (meterRegistry != null) {
            try {
                downgradeMetricCounters.computeIfAbsent(groupKey, k ->
                        Counter.builder("oxmq.plugin.adaptive.downgrades")
                                .description("Adaptive capacity downgrades triggered by throttle events")
                                .tag("group", k)
                                .register(meterRegistry)
                ).increment();
            } catch (Exception ignored) {}
        }
    }

    private void incrementUpgradeMetric(String groupKey) {
        if (meterRegistry != null) {
            try {
                upgradeMetricCounters.computeIfAbsent(groupKey, k ->
                        Counter.builder("oxmq.plugin.adaptive.upgrades")
                                .description("Adaptive capacity upgrades triggered by sustained clean execution")
                                .tag("group", k)
                                .register(meterRegistry)
                ).increment();
            } catch (Exception ignored) {}
        }
    }

    /**
     * Records clean execution for a group, triggering Additive Increase when sustained clean threshold is reached.
     */
    public void recordCleanExecution(String groupKey) {
        if (groupKey == null || isStaticOverride(groupKey)) {
            return;
        }

        long dampingUntil = getDampingExpiry(groupKey);
        long now = System.currentTimeMillis();
        if (now < dampingUntil) {
            // Damping active; freeze upgrades
            return;
        }

        int current = getLimit(groupKey);
        if (current >= maxLimit) {
            return;
        }

        AtomicInteger counter = cleanCounters.computeIfAbsent(groupKey, k -> new AtomicInteger(0));
        int cleanCount = counter.incrementAndGet();

        if (cleanCount >= cleanThreshold) {
            synchronized (counter) {
                if (counter.get() >= cleanThreshold) {
                    counter.set(0);
                    int curLimit = getLimit(groupKey);
                    if (curLimit < maxLimit) {
                        int upgraded = Math.min(maxLimit, curLimit + increaseStep);
                        currentLimits.computeIfAbsent(groupKey, k -> new AtomicInteger(curLimit)).set(upgraded);
                        upgradeCounters.computeIfAbsent(groupKey, k -> new AtomicLong(0)).incrementAndGet();
                        syncLimitToRedis(groupKey, upgraded, 0L);

                        incrementUpgradeMetric(groupKey);

                        log.info("AdaptiveRateLimiter: Group [{}] sustained clean execution ({} successes). Additive Increase: {} -> {} ops/s!",
                                groupKey, cleanThreshold, curLimit, upgraded);
                    }
                }
            }
        }
    }

    /**
     * Returns the current dynamic or static limit for the specified group.
     */
    public int getLimit(String groupKey) {
        if (groupKey == null || groupKey.isBlank() || "all".equalsIgnoreCase(groupKey)) {
            return defaultInitialLimit;
        }

        Integer staticLimit = staticOverrides.get(groupKey);
        if (staticLimit != null) {
            return staticLimit;
        }

        // Check local cached value
        AtomicInteger cached = currentLimits.get(groupKey);
        if (cached != null) {
            return cached.get();
        }

        // Try reading from Redis if connected
        if (connectionManager != null) {
            try {
                StatefulRedisConnection<String, String> conn = connectionManager.getCommandConnection();
                String val = conn.sync().hget(keyPrefix + ":limits", groupKey);
                if (val != null) {
                    int redisLimit = Integer.parseInt(val);
                    currentLimits.computeIfAbsent(groupKey, k -> new AtomicInteger(redisLimit)).set(redisLimit);
                    registerGaugeIfNeeded(groupKey);
                    return redisLimit;
                }
            } catch (Exception e) {
                log.debug("Redis limit read deferred for group [{}]: {}", groupKey, e.getMessage());
            }
        }

        // Initialize with default
        currentLimits.computeIfAbsent(groupKey, k -> new AtomicInteger(defaultInitialLimit));
        registerGaugeIfNeeded(groupKey);
        return defaultInitialLimit;
    }

    /**
     * Directly returns a resolver function compatible with {@link GroupRateLimiterPlugin.Builder#dynamicLimit(Function)}.
     */
    public Function<String, Integer> asLimitResolver() {
        return this::getLimit;
    }

    /**
     * Pins a static override limit for a group, bypassing adaptive changes.
     */
    public void setStaticOverride(String groupKey, int limit) {
        if (groupKey == null) return;
        int clamped = Math.max(1, limit);
        staticOverrides.put(groupKey, clamped);
        currentLimits.computeIfAbsent(groupKey, k -> new AtomicInteger(clamped)).set(clamped);
        syncLimitToRedis(groupKey, clamped, 0L);
        log.info("AdaptiveRateLimiter: Static override pinned for [{}] = {} ops/s", groupKey, clamped);
    }

    /**
     * Clears a static override, re-enabling adaptive capacity tuning.
     */
    public void clearStaticOverride(String groupKey) {
        if (groupKey == null) return;
        staticOverrides.remove(groupKey);
        log.info("AdaptiveRateLimiter: Static override cleared for [{}]; adaptive tuning re-enabled", groupKey);
    }

    public boolean isStaticOverride(String groupKey) {
        return groupKey != null && staticOverrides.containsKey(groupKey);
    }

    public boolean isDampingActive(String groupKey) {
        if (groupKey == null) return false;
        return System.currentTimeMillis() < getDampingExpiry(groupKey);
    }

    public long getDampingRemainingMs(String groupKey) {
        if (groupKey == null) return 0;
        long diff = getDampingExpiry(groupKey) - System.currentTimeMillis();
        return Math.max(0, diff);
    }

    public void resetDampingLock(String groupKey) {
        if (groupKey != null) {
            dampingExpiryTimestamps.remove(groupKey);
            lastNegativeCheckTimestamps.remove(groupKey);
        }
    }

    public long getUpgradeCount(String groupKey) {
        AtomicLong c = upgradeCounters.get(groupKey);
        return c != null ? c.get() : 0;
    }

    public long getDowngradeCount(String groupKey) {
        AtomicLong c = downgradeCounters.get(groupKey);
        return c != null ? c.get() : 0;
    }

    public int getConsecutiveCleanSuccesses(String groupKey) {
        AtomicInteger c = cleanCounters.get(groupKey);
        return c != null ? c.get() : 0;
    }

    private String resolveGroupKey(Job<?> job) {
        try {
            return groupKeyResolver.apply(job);
        } catch (Exception e) {
            return null;
        }
    }

    private long getDampingExpiry(String groupKey) {
        Long expiry = dampingExpiryTimestamps.get(groupKey);
        long now = System.currentTimeMillis();
        if (expiry != null) {
            if (now < expiry) {
                return expiry;
            }
            dampingExpiryTimestamps.remove(groupKey);
        }

        Long lastCheck = lastNegativeCheckTimestamps.get(groupKey);
        if (lastCheck != null && (now - lastCheck) < 1000L) {
            return 0L;
        }

        if (connectionManager != null) {
            try {
                String val = connectionManager.getCommandConnection().sync().get(keyPrefix + ":damping:" + groupKey);
                lastNegativeCheckTimestamps.put(groupKey, now);
                if (val != null) {
                    long redisExpiry = Long.parseLong(val);
                    if (now < redisExpiry) {
                        dampingExpiryTimestamps.put(groupKey, redisExpiry);
                        return redisExpiry;
                    }
                }
            } catch (Exception ignored) {}
        }
        return 0L;
    }

    private void syncLimitToRedis(String groupKey, int limit, long dampingUntil) {
        if (connectionManager == null) return;
        try {
            RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
            sync.hset(keyPrefix + ":limits", groupKey, String.valueOf(limit));
            if (dampingUntil > System.currentTimeMillis()) {
                long ttlMs = dampingUntil - System.currentTimeMillis();
                sync.psetex(keyPrefix + ":damping:" + groupKey, ttlMs, String.valueOf(dampingUntil));
            }
        } catch (Exception e) {
            log.debug("Redis state sync deferred for group [{}]: {}", groupKey, e.getMessage());
        }
    }

    private void registerGaugeIfNeeded(String groupKey) {
        if (meterRegistry != null) {
            try {
                Gauge.builder("oxmq.plugin.adaptive.capacity", () -> getLimit(groupKey))
                        .description("Current dynamic capacity limit enforced by AdaptiveRateLimiterPlugin")
                        .tag("group", groupKey)
                        .register(meterRegistry);
            } catch (Exception ignored) {}
        }
    }

    public static class Builder {
        private RedisConnectionManager connectionManager;
        private String keyPrefix = "oxmq:adaptive";
        private Function<Job<?>, String> groupKeyResolver;
        private int initialLimit = 50;
        private int minLimit = 10;
        private int maxLimit = 1000;
        private int increaseStep = 25;
        private double backoffFactor = 0.5;
        private Duration dampingDuration = Duration.ofSeconds(30);
        private int cleanThreshold = 5;
        private Predicate<Throwable> rateLimitMatcher;
        private MeterRegistry meterRegistry;

        public Builder connectionManager(RedisConnectionManager connectionManager) {
            this.connectionManager = connectionManager;
            return this;
        }

        public Builder keyPrefix(String keyPrefix) {
            this.keyPrefix = keyPrefix;
            return this;
        }

        public Builder groupKeyResolver(Function<Job<?>, String> groupKeyResolver) {
            this.groupKeyResolver = groupKeyResolver;
            return this;
        }

        public Builder initialLimit(int initialLimit) {
            this.initialLimit = initialLimit;
            return this;
        }

        public Builder minLimit(int minLimit) {
            this.minLimit = minLimit;
            return this;
        }

        public Builder maxLimit(int maxLimit) {
            this.maxLimit = maxLimit;
            return this;
        }

        public Builder increaseStep(int increaseStep) {
            this.increaseStep = increaseStep;
            return this;
        }

        public Builder backoffFactor(double backoffFactor) {
            this.backoffFactor = backoffFactor;
            return this;
        }

        public Builder dampingDuration(Duration dampingDuration) {
            this.dampingDuration = dampingDuration;
            return this;
        }

        public Builder cleanThreshold(int cleanThreshold) {
            this.cleanThreshold = cleanThreshold;
            return this;
        }

        public Builder rateLimitMatcher(Predicate<Throwable> rateLimitMatcher) {
            this.rateLimitMatcher = rateLimitMatcher;
            return this;
        }

        public Builder meterRegistry(MeterRegistry meterRegistry) {
            this.meterRegistry = meterRegistry;
            return this;
        }

        public AdaptiveRateLimiterPlugin build() {
            return new AdaptiveRateLimiterPlugin(
                    connectionManager,
                    keyPrefix,
                    groupKeyResolver,
                    initialLimit,
                    minLimit,
                    maxLimit,
                    increaseStep,
                    backoffFactor,
                    dampingDuration,
                    cleanThreshold,
                    rateLimitMatcher,
                    meterRegistry
            );
        }
    }
}
