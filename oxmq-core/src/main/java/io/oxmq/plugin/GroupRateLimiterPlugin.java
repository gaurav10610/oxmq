package io.oxmq.plugin;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.interceptor.InterceptorDecision;
import io.oxmq.interceptor.JobInterceptor;
import io.oxmq.model.Job;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-performance, distributed multi-tenant / group rate limiter plugin for OxMQ.
 * <p>
 * Enforces dynamic or static rate limits scoped by a custom group key (e.g. {@code tenantId},
 * {@code customerId}, {@code apiKey}, or {@code storeDomain}) across all distributed worker nodes.
 * <p>
 * When a group's rate limit is reached within the current time window, jobs are non-destructively
 * moved to the delayed set via {@link InterceptorDecision#delay(long, boolean)} with {@code skipAttempt=true},
 * preserving attempts and preventing task starvation without modifying BullMQ's core Lua scripts.
 */
public class GroupRateLimiterPlugin implements JobInterceptor {

    private static final Logger log = LoggerFactory.getLogger(GroupRateLimiterPlugin.class);

    private final RedisConnectionManager connectionManager;
    private final String keyPrefix;
    private final Function<Job<?>, String> groupKeyResolver;
    private final Function<String, Integer> limitResolver;
    private final Duration windowDuration;
    private final long retryDelayMs;
    private final MeterRegistry meterRegistry;

    private final ConcurrentMap<String, AtomicLong> yieldCounters = new ConcurrentHashMap<>();

    public GroupRateLimiterPlugin(
            RedisConnectionManager connectionManager,
            String keyPrefix,
            Function<Job<?>, String> groupKeyResolver,
            Function<String, Integer> limitResolver,
            Duration windowDuration,
            long retryDelayMs
    ) {
        this(connectionManager, keyPrefix, groupKeyResolver, limitResolver, windowDuration, retryDelayMs, Metrics.globalRegistry);
    }

    public GroupRateLimiterPlugin(
            RedisConnectionManager connectionManager,
            String keyPrefix,
            Function<Job<?>, String> groupKeyResolver,
            Function<String, Integer> limitResolver,
            Duration windowDuration,
            long retryDelayMs,
            MeterRegistry meterRegistry
    ) {
        this.connectionManager = Objects.requireNonNull(connectionManager, "connectionManager must not be null");
        this.keyPrefix = keyPrefix != null ? keyPrefix : "oxmq:limiter";
        this.groupKeyResolver = Objects.requireNonNull(groupKeyResolver, "groupKeyResolver must not be null");
        this.limitResolver = Objects.requireNonNull(limitResolver, "limitResolver must not be null");
        this.windowDuration = windowDuration != null ? windowDuration : Duration.ofSeconds(1);
        this.retryDelayMs = Math.max(20, retryDelayMs);
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
            log.warn("Failed resolving groupKey for job {}: {}", job.getId(), e.getMessage());
            return InterceptorDecision.proceed();
        }

        if (groupKey == null || groupKey.isBlank() || "all".equalsIgnoreCase(groupKey)) {
            return InterceptorDecision.proceed();
        }

        int limit = limitResolver.apply(groupKey);
        if (limit <= 0) {
            return InterceptorDecision.proceed();
        }

        long epochBucket = Instant.now().getEpochSecond();
        String redisKey = keyPrefix + ":" + groupKey + ":" + epochBucket;

        try {
            StatefulRedisConnection<String, String> conn = connectionManager.getCommandConnection();
            RedisCommands<String, String> sync = conn.sync();

            Long currentUsage = sync.incr(redisKey);
            if (currentUsage != null && currentUsage == 1) {
                sync.expire(redisKey, Math.max(2, windowDuration.toSeconds() + 1));
            }

            if (currentUsage != null && currentUsage > limit) {
                yieldCounters.computeIfAbsent(groupKey, k -> new AtomicLong(0)).incrementAndGet();
                if (meterRegistry != null) {
                    try {
                        Counter.builder("oxmq.plugin.ratelimiter.yields")
                                .description("Jobs delayed by GroupRateLimiterPlugin due to quota exhaustion")
                                .tag("group", groupKey)
                                .tag("queue", job.getQueueName() != null ? job.getQueueName() : "unknown")
                                .register(meterRegistry)
                                .increment();
                    } catch (Exception ignored) {}
                }
                log.debug("Rate limit exceeded for group [{}] (usage: {}/{}); moving job [{}] to delayed",
                        groupKey, currentUsage, limit, job.getId());
                return InterceptorDecision.delay(retryDelayMs, true);
            }

            return InterceptorDecision.proceed();
        } catch (Exception e) {
            log.warn("Redis error in GroupRateLimiterPlugin for group [{}]: {}", groupKey, e.getMessage());
            return InterceptorDecision.proceed(); // Fail open if Redis is unavailable
        }
    }

    public int getCurrentUsage(String groupKey) {
        if (groupKey == null || groupKey.isBlank()) return 0;
        long epochBucket = Instant.now().getEpochSecond();
        String redisKey = keyPrefix + ":" + groupKey + ":" + epochBucket;
        try {
            String val = connectionManager.getCommandConnection().sync().get(redisKey);
            return val != null ? Integer.parseInt(val) : 0;
        } catch (Exception e) {
            return 0;
        }
    }

    public int getRemainingTokens(String groupKey) {
        int limit = limitResolver.apply(groupKey);
        int usage = getCurrentUsage(groupKey);
        return Math.max(0, limit - usage);
    }

    public double getRemainingHeadroomPercent(String groupKey) {
        int limit = limitResolver.apply(groupKey);
        if (limit <= 0) return 100.0;
        int remaining = getRemainingTokens(groupKey);
        return Math.round(((double) remaining / limit) * 1000.0) / 10.0;
    }

    public long getYieldCount(String groupKey) {
        AtomicLong c = yieldCounters.get(groupKey);
        return c != null ? c.get() : 0;
    }

    public int getLimit(String groupKey) {
        return limitResolver.apply(groupKey);
    }

    public static class Builder {
        private RedisConnectionManager connectionManager;
        private String keyPrefix = "oxmq:limiter";
        private Function<Job<?>, String> groupKeyResolver;
        private Function<String, Integer> limitResolver;
        private Duration windowDuration = Duration.ofSeconds(1);
        private long retryDelayMs = 200;
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

        public Builder dynamicLimit(Function<String, Integer> limitResolver) {
            this.limitResolver = limitResolver;
            return this;
        }

        public Builder staticLimit(int limit) {
            this.limitResolver = key -> limit;
            return this;
        }

        public Builder windowDuration(Duration windowDuration) {
            this.windowDuration = windowDuration;
            return this;
        }

        public Builder retryDelayMs(long retryDelayMs) {
            this.retryDelayMs = retryDelayMs;
            return this;
        }

        public GroupRateLimiterPlugin build() {
            return new GroupRateLimiterPlugin(connectionManager, keyPrefix, groupKeyResolver, limitResolver, windowDuration, retryDelayMs, meterRegistry);
        }
    }
}
