package io.oxmq;

import io.lettuce.core.RedisClient;
import io.oxmq.client.QueueKeys;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.lua.LuaScriptManager;
import io.oxmq.metrics.OxmqMetrics;
import io.oxmq.serializer.JacksonJobSerializer;
import io.oxmq.serializer.JobSerializer;
import java.io.Closeable;
import java.util.Objects;

/**
 * Service-level entry point and factory for OxMQ distributed queue and workflow engine.
 *
 * <p>Enables the <b>"Define Once, Inherit Everywhere"</b> pattern in plain Java and non-Spring
 * microservices (Quarkus, Micronaut, Dropwizard, Guice, or standalone CLI applications).
 *
 * <p>When an {@link OxmqClient} is configured with a service prefix (e.g., {@code "billing-service"}),
 * all queues, workers, batch workers, events, and DAG flow producers created via this client
 * automatically inherit that service namespace without requiring per-queue configuration.
 */
public class OxmqClient implements Closeable {

    private final String prefix;
    private final RedisConnectionManager connectionManager;
    private final LuaScriptManager scriptManager;
    private final JobSerializer serializer;
    private final OxmqMetrics metrics;
    private final int defaultConcurrency;
    private final boolean defaultVirtualThreads;

    public OxmqClient(String prefix,
                      RedisConnectionManager connectionManager,
                      LuaScriptManager scriptManager,
                      JobSerializer serializer,
                      OxmqMetrics metrics,
                      int defaultConcurrency,
                      boolean defaultVirtualThreads) {
        this.prefix = QueueKeys.resolvePrefix(prefix);
        this.connectionManager = Objects.requireNonNull(connectionManager, "connectionManager must not be null");
        this.scriptManager = scriptManager != null ? scriptManager : new LuaScriptManager();
        this.serializer = serializer != null ? serializer : new JacksonJobSerializer();
        this.metrics = metrics != null ? metrics : new OxmqMetrics();
        this.defaultConcurrency = defaultConcurrency > 0 ? defaultConcurrency : 20;
        this.defaultVirtualThreads = defaultVirtualThreads;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a typed {@link OxmqQueue} inheriting this service's namespace.
     *
     * @param queueName    the queue name
     * @param payloadClass the payload class
     * @param <T>          payload type
     * @return initialized queue
     */
    public <T> OxmqQueue<T> queue(String queueName, Class<T> payloadClass) {
        return OxmqQueue.<T>builder()
                .prefix(this.prefix)
                .name(queueName)
                .connectionManager(this.connectionManager)
                .scriptManager(this.scriptManager)
                .serializer(this.serializer)
                .metrics(this.metrics)
                .payloadClass(payloadClass)
                .build();
    }

    /**
     * Creates an untyped (Object) {@link OxmqQueue} inheriting this service's namespace.
     *
     * @param queueName the queue name
     * @return initialized queue
     */
    public OxmqQueue<Object> queue(String queueName) {
        return queue(queueName, Object.class);
    }

    /**
     * Creates an {@link OxmqWorker} for the given queue inheriting this service's namespace
     * and default concurrency / virtual thread settings.
     *
     * @param queueName    the queue name
     * @param payloadClass the payload class
     * @param processor    the job processor
     * @param <T>          payload type
     * @return unstarted worker
     */
    public <T> Worker<T> worker(String queueName, Class<T> payloadClass, JobProcessor<T, ?> processor) {
        return worker(queueName, payloadClass, this.defaultConcurrency, processor);
    }

    /**
     * Creates an {@link OxmqWorker} for the given queue inheriting this service's namespace
     * with custom concurrency.
     *
     * @param queueName    the queue name
     * @param payloadClass the payload class
     * @param concurrency  worker concurrency
     * @param processor    the job processor
     * @param <T>          payload type
     * @return unstarted worker
     */
    public <T> Worker<T> worker(String queueName, Class<T> payloadClass, int concurrency, JobProcessor<T, ?> processor) {
        return OxmqWorker.<T>builder()
                .prefix(this.prefix)
                .queueName(queueName)
                .connectionManager(this.connectionManager)
                .scriptManager(this.scriptManager)
                .serializer(this.serializer)
                .metrics(this.metrics)
                .concurrency(concurrency)
                .useVirtualThreads(this.defaultVirtualThreads)
                .payloadClass(payloadClass)
                .processor(processor)
                .build();
    }

    /**
     * Creates an {@link OxmqBatchWorker} for high-throughput bulk ingestion inheriting this service's namespace.
     *
     * @param queueName    the queue name
     * @param payloadClass the payload class
     * @param batchSize    number of jobs to pop per atomic batch
     * @param processor    the batch job processor
     * @param <T>          payload type
     * @return unstarted batch worker
     */
    public <T> OxmqBatchWorker<T> batchWorker(String queueName, Class<T> payloadClass, int batchSize, BatchJobProcessor<T, ?> processor) {
        return OxmqBatchWorker.<T>builder()
                .prefix(this.prefix)
                .queueName(queueName)
                .connectionManager(this.connectionManager)
                .scriptManager(this.scriptManager)
                .serializer(this.serializer)
                .metrics(this.metrics)
                .batchSize(batchSize)
                .useVirtualThreads(this.defaultVirtualThreads)
                .payloadClass(payloadClass)
                .processor(processor)
                .build();
    }

    /**
     * Creates a {@link FlowProducer} for parent-child DAG workflow trees inheriting this service's namespace.
     *
     * @return initialized flow producer
     */
    public FlowProducer flowProducer() {
        return new FlowProducer(this.connectionManager, this.scriptManager, this.serializer, this.prefix);
    }

    /**
     * Creates a {@link QueueEvents} listener subscribing to Redis Pub/Sub events for the queue
     * within this service's namespace.
     *
     * @param queueName the queue name
     * @return unstarted QueueEvents instance
     */
    public QueueEvents events(String queueName) {
        return new QueueEvents(this.prefix, queueName, this.connectionManager, this.serializer);
    }

    /**
     * Returns an {@link OxmqClient} scoped to an external target service namespace, reusing the underlying
     * Redis connection pool, Lua script manager, and serializer.
     *
     * @param targetPrefix the target service namespace prefix
     * @return client scoped to target prefix
     */
    public OxmqClient forService(String targetPrefix) {
        return new OxmqClient(
                targetPrefix,
                this.connectionManager,
                this.scriptManager,
                this.serializer,
                this.metrics,
                this.defaultConcurrency,
                this.defaultVirtualThreads
        );
    }

    public String getPrefix() {
        return prefix;
    }

    public RedisConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public LuaScriptManager getScriptManager() {
        return scriptManager;
    }

    public JobSerializer getSerializer() {
        return serializer;
    }

    public OxmqMetrics getMetrics() {
        return metrics;
    }

    @Override
    public void close() {
        connectionManager.close();
    }

    public static class Builder {
        private String prefix;
        private RedisConnectionManager connectionManager;
        private RedisClient redisClient;
        private String redisUri;
        private LuaScriptManager scriptManager;
        private JobSerializer serializer;
        private OxmqMetrics metrics;
        private int defaultConcurrency = 20;
        private boolean defaultVirtualThreads = true;

        public Builder prefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder redisUri(String redisUri) {
            this.redisUri = redisUri;
            return this;
        }

        public Builder redisClient(RedisClient redisClient) {
            this.redisClient = redisClient;
            return this;
        }

        public Builder connectionManager(RedisConnectionManager connectionManager) {
            this.connectionManager = connectionManager;
            return this;
        }

        public Builder scriptManager(LuaScriptManager scriptManager) {
            this.scriptManager = scriptManager;
            return this;
        }

        public Builder serializer(JobSerializer serializer) {
            this.serializer = serializer;
            return this;
        }

        public Builder metrics(OxmqMetrics metrics) {
            this.metrics = metrics;
            return this;
        }

        public Builder defaultConcurrency(int defaultConcurrency) {
            this.defaultConcurrency = defaultConcurrency;
            return this;
        }

        public Builder defaultVirtualThreads(boolean defaultVirtualThreads) {
            this.defaultVirtualThreads = defaultVirtualThreads;
            return this;
        }

        public OxmqClient build() {
            if (connectionManager == null) {
                if (redisClient != null) {
                    connectionManager = new RedisConnectionManager(redisClient);
                } else if (redisUri != null) {
                    connectionManager = new RedisConnectionManager(redisUri);
                } else {
                    connectionManager = new RedisConnectionManager("redis://localhost:6379");
                }
            }
            return new OxmqClient(
                    prefix,
                    connectionManager,
                    scriptManager,
                    serializer,
                    metrics,
                    defaultConcurrency,
                    defaultVirtualThreads
            );
        }
    }
}
