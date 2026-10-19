package io.oxmq.plugin;

import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.exception.RateLimitCooldownException;
import io.oxmq.interceptor.ErrorDecision;
import io.oxmq.interceptor.InterceptorDecision;
import io.oxmq.interceptor.JobInterceptor;
import io.oxmq.model.Job;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adaptive Circuit Breaker &amp; Cooldown Sentinel for external upstream APIs (e.g. HTTP 429 / 503).
 * <p>
 * When an external service signals rate-limiting with a {@code Retry-After} backoff (e.g. by throwing
 * {@link RateLimitCooldownException}), this plugin sets an atomic distributed cooldown in Redis.
 * <p>
 * All workers across all distributed nodes will automatically defer jobs belonging to that group
 * using {@link InterceptorDecision#delay(long, boolean)} without failing attempts or hammering the upstream API.
 */
public class RateLimitSentinelPlugin implements JobInterceptor {

    private static final Logger log = LoggerFactory.getLogger(RateLimitSentinelPlugin.class);

    private final RedisConnectionManager connectionManager;
    private final String keyPrefix;
    private final Function<Job<?>, String> groupKeyResolver;
    private final Predicate<Throwable> rateLimitErrorMatcher;
    private final MeterRegistry meterRegistry;

    private final ConcurrentMap<String, AtomicLong> throttleCounters = new ConcurrentHashMap<>();

    public RateLimitSentinelPlugin(
            RedisConnectionManager connectionManager,
            String keyPrefix,
            Function<Job<?>, String> groupKeyResolver,
            Predicate<Throwable> rateLimitErrorMatcher
    ) {
        this(connectionManager, keyPrefix, groupKeyResolver, rateLimitErrorMatcher, Metrics.globalRegistry);
    }

    public RateLimitSentinelPlugin(
            RedisConnectionManager connectionManager,
            String keyPrefix,
            Function<Job<?>, String> groupKeyResolver,
            Predicate<Throwable> rateLimitErrorMatcher,
            MeterRegistry meterRegistry
    ) {
        this.connectionManager = Objects.requireNonNull(connectionManager, "connectionManager must not be null");
        this.keyPrefix = keyPrefix != null ? keyPrefix : "oxmq:sentinel";
        this.groupKeyResolver = Objects.requireNonNull(groupKeyResolver, "groupKeyResolver must not be null");
        this.rateLimitErrorMatcher = rateLimitErrorMatcher != null ? rateLimitErrorMatcher : t -> t instanceof RateLimitCooldownException;
        this.meterRegistry = meterRegistry != null ? meterRegistry : Metrics.globalRegistry;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public InterceptorDecision beforeProcess(Job<?> job) {
        String groupKey;
        try {
            groupKey = groupKeyResolver.apply(job);
        } catch (Exception e) {
            return InterceptorDecision.proceed();
        }

        if (groupKey == null || groupKey.isBlank() || "all".equalsIgnoreCase(groupKey)) {
            return InterceptorDecision.proceed();
        }

        long remainingMs = getCooldownRemainingMs(groupKey);
        if (remainingMs > 0) {
            if (meterRegistry != null) {
                try {
                    Counter.builder("oxmq.plugin.sentinel.delays")
                            .description("Jobs preemptively delayed by RateLimitSentinelPlugin due to active 429 cooldown")
                            .tag("group", groupKey)
                            .tag("queue", job.getQueueName() != null ? job.getQueueName() : "unknown")
                            .register(meterRegistry)
                            .increment();
                } catch (Exception ignored) {}
            }
            log.debug("Preemptively deferring job [{}] for group [{}]: Cooldown Sentinel active for next {} ms",
                    job.getId(), groupKey, remainingMs);
            return InterceptorDecision.delay(remainingMs, true);
        }

        return InterceptorDecision.proceed();
    }

    @Override
    public ErrorDecision onError(Job<?> job, Throwable error, Duration duration) {
        String groupKey;
        try {
            groupKey = groupKeyResolver.apply(job);
        } catch (Exception e) {
            return ErrorDecision.bubbleUp();
        }

        if (groupKey == null || groupKey.isBlank() || "all".equalsIgnoreCase(groupKey)) {
            return ErrorDecision.bubbleUp();
        }

        if (error instanceof RateLimitCooldownException rce) {
            Duration cooldown = rce.getCooldownDuration();
            String explicitKey = rce.getGroupKey() != null ? rce.getGroupKey() : groupKey;
            triggerCooldown(explicitKey, cooldown);
            throttleCounters.computeIfAbsent(explicitKey, k -> new AtomicLong(0)).incrementAndGet();
            if (meterRegistry != null) {
                try {
                    Counter.builder("oxmq.plugin.sentinel.cooldowns")
                            .description("Total 429/503 cooldown events triggered by RateLimitSentinelPlugin")
                            .tag("group", explicitKey)
                            .register(meterRegistry)
                            .increment();
                } catch (Exception ignored) {}
            }
            log.warn("RateLimitSentinel triggered cooldown on group [{}] for {} ms", explicitKey, cooldown.toMillis());
            return ErrorDecision.cooldown(cooldown, explicitKey);
        }

        if (rateLimitErrorMatcher.test(error)) {
            Duration defaultCooldown = Duration.ofSeconds(3);
            triggerCooldown(groupKey, defaultCooldown);
            throttleCounters.computeIfAbsent(groupKey, k -> new AtomicLong(0)).incrementAndGet();
            if (meterRegistry != null) {
                try {
                    Counter.builder("oxmq.plugin.sentinel.cooldowns")
                            .description("Total 429/503 cooldown events triggered by RateLimitSentinelPlugin")
                            .tag("group", groupKey)
                            .register(meterRegistry)
                            .increment();
                } catch (Exception ignored) {}
            }
            log.warn("RateLimitSentinel matched error {} on group [{}]; triggering 3s cooldown", error.getClass().getSimpleName(), groupKey);
            return ErrorDecision.cooldown(defaultCooldown, groupKey);
        }

        return ErrorDecision.bubbleUp();
    }

    public void triggerCooldown(String groupKey, Duration duration) {
        if (groupKey == null || groupKey.isBlank()) return;
        long durationMs = duration != null ? duration.toMillis() : 3000;
        long expiryTime = System.currentTimeMillis() + durationMs;
        String redisKey = keyPrefix + ":cooldown:" + groupKey;

        try {
            RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
            sync.set(redisKey, String.valueOf(expiryTime), SetArgs.Builder.px(durationMs));
        } catch (Exception e) {
            log.warn("Failed setting cooldown in Redis for group [{}]: {}", groupKey, e.getMessage());
        }
    }

    public void clearCooldown(String groupKey) {
        if (groupKey == null || groupKey.isBlank()) return;
        String redisKey = keyPrefix + ":cooldown:" + groupKey;
        try {
            connectionManager.getCommandConnection().sync().del(redisKey);
        } catch (Exception ignored) {}
    }

    public boolean isCooldownActive(String groupKey) {
        return getCooldownRemainingMs(groupKey) > 0;
    }

    public long getCooldownRemainingMs(String groupKey) {
        if (groupKey == null || groupKey.isBlank()) return 0;
        String redisKey = keyPrefix + ":cooldown:" + groupKey;

        try {
            RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
            String val = sync.get(redisKey);
            if (val == null) return 0;
            long expiryTime = Long.parseLong(val);
            long diff = expiryTime - System.currentTimeMillis();
            return Math.max(0, diff);
        } catch (Exception e) {
            return 0;
        }
    }

    public long getThrottleCount(String groupKey) {
        AtomicLong c = throttleCounters.get(groupKey);
        return c != null ? c.get() : 0;
    }

    public static class Builder {
        private RedisConnectionManager connectionManager;
        private String keyPrefix = "oxmq:sentinel";
        private Function<Job<?>, String> groupKeyResolver;
        private Predicate<Throwable> rateLimitErrorMatcher;
        private MeterRegistry meterRegistry;

        public Builder connectionManager(RedisConnectionManager connectionManager) {
            this.connectionManager = connectionManager;
            return this;
        }

        public Builder meterRegistry(MeterRegistry meterRegistry) {
            this.meterRegistry = meterRegistry;
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

        public Builder errorMatcher(Predicate<Throwable> rateLimitErrorMatcher) {
            this.rateLimitErrorMatcher = rateLimitErrorMatcher;
            return this;
        }

        public RateLimitSentinelPlugin build() {
            return new RateLimitSentinelPlugin(connectionManager, keyPrefix, groupKeyResolver, rateLimitErrorMatcher, meterRegistry);
        }
    }
}
