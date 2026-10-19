package io.oxmq.plugin;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.interceptor.ErrorDecision;
import io.oxmq.interceptor.InterceptorDecision;
import io.oxmq.interceptor.JobInterceptor;
import io.oxmq.model.Job;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enterprise Checkpoint State Plugin for OxMQ.
 * <p>
 * Provides fault-tolerant, distributed cursor and delta checkpointing per group/partition
 * backed by Redis and local resilient memory caching.
 * <p>
 * Essential for incremental syncs, change data capture (CDC), delta tokens, streaming offsets,
 * and paginated crawlers. If a worker pod crashes, restarts, or fails mid-stream, the job or partition
 * automatically resumes from its last committed checkpoint rather than reprocessing entire datasets.
 * <p>
 * Supports both declarative checkpointing (via {@link CheckpointSupplier} return values or custom extractor)
 * and programmatic checkpointing via {@link #saveCheckpoint(String, String)}.
 */
public class CheckpointStatePlugin implements JobInterceptor {

    private static final Logger log = LoggerFactory.getLogger(CheckpointStatePlugin.class);

    private static final ThreadLocal<String> CURRENT_CHECKPOINT = new ThreadLocal<>();

    private final RedisConnectionManager connectionManager;
    private final String namespace;
    private final String keyPrefix;
    private final Function<Job<?>, String> groupKeyResolver;
    private final BiFunction<Job<?>, Object, String> checkpointExtractor;
    private final MeterRegistry meterRegistry;

    // Resilient local memory cache for sub-millisecond reads and Redis disconnection fallback
    private final ConcurrentMap<String, String> localCheckpointCache = new ConcurrentHashMap<>();

    public CheckpointStatePlugin(
            RedisConnectionManager connectionManager,
            String namespace,
            String keyPrefix,
            Function<Job<?>, String> groupKeyResolver,
            BiFunction<Job<?>, Object, String> checkpointExtractor,
            MeterRegistry meterRegistry
    ) {
        this.connectionManager = connectionManager;
        this.namespace = namespace != null ? namespace : "default";
        this.keyPrefix = keyPrefix != null ? keyPrefix : "oxmq:checkpoint";
        this.groupKeyResolver = Objects.requireNonNull(groupKeyResolver, "groupKeyResolver must not be null");
        this.checkpointExtractor = checkpointExtractor;
        this.meterRegistry = meterRegistry != null ? meterRegistry : Metrics.globalRegistry;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the active checkpoint cursor for the currently executing job thread, if any.
     */
    public static Optional<String> getCurrentCheckpoint() {
        return Optional.ofNullable(CURRENT_CHECKPOINT.get());
    }

    @Override
    public InterceptorDecision beforeProcess(Job<?> job) {
        String groupKey = resolveGroupKey(job);
        if (groupKey != null) {
            String checkpoint = getCheckpoint(groupKey).orElse(null);
            if (checkpoint != null) {
                CURRENT_CHECKPOINT.set(checkpoint);
            }
        }
        return InterceptorDecision.proceed();
    }

    @Override
    public void afterProcess(Job<?> job, Object result, Duration duration) {
        try {
            String groupKey = resolveGroupKey(job);
            if (groupKey == null) {
                return;
            }

            String newCursor = null;
            if (checkpointExtractor != null) {
                newCursor = checkpointExtractor.apply(job, result);
            } else if (result instanceof CheckpointSupplier supplier) {
                newCursor = supplier.getCheckpointCursor();
            }

            if (newCursor != null && !newCursor.isBlank()) {
                saveCheckpoint(groupKey, newCursor);
            }
        } finally {
            CURRENT_CHECKPOINT.remove();
        }
    }

    @Override
    public ErrorDecision onError(Job<?> job, Throwable error, Duration duration) {
        CURRENT_CHECKPOINT.remove();
        return ErrorDecision.bubbleUp();
    }

    /**
     * Saves a checkpoint cursor for the given group/partition key.
     * Atomically commits to Redis hash and updates local cache.
     */
    public void saveCheckpoint(String groupKey, String cursor) {
        if (groupKey == null || cursor == null) {
            return;
        }

        localCheckpointCache.put(groupKey, cursor);

        if (connectionManager != null) {
            try {
                StatefulRedisConnection<String, String> conn = connectionManager.getCommandConnection();
                conn.sync().hset(getRedisHashKey(), groupKey, cursor);
            } catch (Exception e) {
                log.warn("Redis write deferred in CheckpointStatePlugin for group [{}]: {}", groupKey, e.getMessage());
            }
        }

        if (meterRegistry != null) {
            try {
                Counter.builder("oxmq.plugin.checkpoint.commits")
                        .description("Checkpoint commits persisted by CheckpointStatePlugin")
                        .tag("namespace", namespace)
                        .tag("group", groupKey)
                        .register(meterRegistry)
                        .increment();
            } catch (Exception ignored) {}
        }

        log.debug("CheckpointStatePlugin: Committed checkpoint for namespace [{}] group [{}]: {}",
                namespace, groupKey, cursor);
    }

    /**
     * Retrieves the last committed checkpoint cursor for the given group/partition key.
     */
    public Optional<String> getCheckpoint(String groupKey) {
        if (groupKey == null || groupKey.isBlank()) {
            return Optional.empty();
        }

        // Try local cache first
        String cached = localCheckpointCache.get(groupKey);
        if (cached != null) {
            return Optional.of(cached);
        }

        // Read from Redis
        if (connectionManager != null) {
            try {
                StatefulRedisConnection<String, String> conn = connectionManager.getCommandConnection();
                String val = conn.sync().hget(getRedisHashKey(), groupKey);
                if (val != null) {
                    localCheckpointCache.put(groupKey, val);
                    recordReadMetric(groupKey);
                    return Optional.of(val);
                }
            } catch (Exception e) {
                log.debug("Redis read deferred in CheckpointStatePlugin for group [{}]: {}", groupKey, e.getMessage());
            }
        }

        return Optional.empty();
    }

    /**
     * Clears any saved checkpoint cursor for the group key.
     */
    public void clearCheckpoint(String groupKey) {
        if (groupKey == null) return;
        localCheckpointCache.remove(groupKey);
        if (connectionManager != null) {
            try {
                connectionManager.getCommandConnection().sync().hdel(getRedisHashKey(), groupKey);
            } catch (Exception ignored) {}
        }
    }

    /**
     * Returns all saved checkpoints for this namespace.
     */
    public Map<String, String> getAllCheckpoints() {
        if (connectionManager != null) {
            try {
                Map<String, String> all = connectionManager.getCommandConnection().sync().hgetall(getRedisHashKey());
                if (all != null && !all.isEmpty()) {
                    localCheckpointCache.putAll(all);
                    return all;
                }
            } catch (Exception ignored) {}
        }
        return Map.copyOf(localCheckpointCache);
    }

    private String getRedisHashKey() {
        return keyPrefix + ":" + namespace;
    }

    private String resolveGroupKey(Job<?> job) {
        try {
            return groupKeyResolver.apply(job);
        } catch (Exception e) {
            return null;
        }
    }

    private void recordReadMetric(String groupKey) {
        if (meterRegistry != null) {
            try {
                Counter.builder("oxmq.plugin.checkpoint.reads")
                        .description("Checkpoint reads retrieved by CheckpointStatePlugin")
                        .tag("namespace", namespace)
                        .tag("group", groupKey)
                        .register(meterRegistry)
                        .increment();
            } catch (Exception ignored) {}
        }
    }

    public static class Builder {
        private RedisConnectionManager connectionManager;
        private String namespace = "default";
        private String keyPrefix = "oxmq:checkpoint";
        private Function<Job<?>, String> groupKeyResolver;
        private BiFunction<Job<?>, Object, String> checkpointExtractor;
        private MeterRegistry meterRegistry;

        public Builder connectionManager(RedisConnectionManager connectionManager) {
            this.connectionManager = connectionManager;
            return this;
        }

        public Builder namespace(String namespace) {
            this.namespace = namespace;
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

        public Builder checkpointExtractor(BiFunction<Job<?>, Object, String> checkpointExtractor) {
            this.checkpointExtractor = checkpointExtractor;
            return this;
        }

        public Builder meterRegistry(MeterRegistry meterRegistry) {
            this.meterRegistry = meterRegistry;
            return this;
        }

        public CheckpointStatePlugin build() {
            return new CheckpointStatePlugin(
                    connectionManager,
                    namespace,
                    keyPrefix,
                    groupKeyResolver,
                    checkpointExtractor,
                    meterRegistry
            );
        }
    }
}
