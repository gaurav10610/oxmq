package io.oxmq.spring;

import io.lettuce.core.RedisClient;
import io.oxmq.FlowProducer;
import io.oxmq.OxmqQueue;
import io.oxmq.QueueEvents;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.lua.LuaScriptManager;
import io.oxmq.metrics.OxmqMetrics;
import io.oxmq.model.FlowJob;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobRequest;
import io.oxmq.model.JobState;
import io.oxmq.serializer.JobSerializer;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Spring Template for high-level, thread-safe, and idiomatic OxMQ distributed job operations.
 *
 * <p>Implements the standard Spring Template design pattern (similar to {@code KafkaTemplate},
 * {@code RabbitTemplate}, and {@code RedisTemplate}). Reuses queue instances and Redis connections
 * efficiently across the application.
 */
public class OxmqTemplate {

    private final RedisClient redisClient;
    private final RedisConnectionManager connectionManager;
    private final LuaScriptManager scriptManager;
    private final JobSerializer serializer;
    private final OxmqMetrics metrics;
    private final FlowProducer flowProducer;
    private final String defaultPrefix;
    private final JobOptions defaultJobOptions;
    private final int maxLenEvents;

    private final ConcurrentMap<String, OxmqQueue<?>> queueCache = new ConcurrentHashMap<>();

    public OxmqTemplate(RedisClient redisClient,
                        RedisConnectionManager connectionManager,
                        LuaScriptManager scriptManager,
                        JobSerializer serializer,
                        OxmqMetrics metrics,
                        FlowProducer flowProducer) {
        this(redisClient, connectionManager, scriptManager, serializer, metrics, flowProducer, null, null, -1);
    }

    public OxmqTemplate(RedisClient redisClient,
                        RedisConnectionManager connectionManager,
                        LuaScriptManager scriptManager,
                        JobSerializer serializer,
                        OxmqMetrics metrics,
                        FlowProducer flowProducer,
                        String defaultPrefix) {
        this(redisClient, connectionManager, scriptManager, serializer, metrics, flowProducer, defaultPrefix, null, -1);
    }

    public OxmqTemplate(RedisClient redisClient,
                        RedisConnectionManager connectionManager,
                        LuaScriptManager scriptManager,
                        JobSerializer serializer,
                        OxmqMetrics metrics,
                        FlowProducer flowProducer,
                        String defaultPrefix,
                        JobOptions defaultJobOptions,
                        int maxLenEvents) {
        this.redisClient = Objects.requireNonNull(redisClient, "redisClient must not be null");
        this.connectionManager = Objects.requireNonNull(connectionManager, "connectionManager must not be null");
        this.scriptManager = Objects.requireNonNull(scriptManager, "scriptManager must not be null");
        this.serializer = Objects.requireNonNull(serializer, "serializer must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
        this.flowProducer = flowProducer;
        this.defaultPrefix = defaultPrefix != null && !defaultPrefix.isBlank() ? defaultPrefix.trim() : null;
        this.defaultJobOptions = defaultJobOptions;
        this.maxLenEvents = maxLenEvents;
    }

    /**
     * Retrieves or creates a typed {@link OxmqQueue} instance for the given queue name,
     * using the default service-level namespace.
     */
    public <T> OxmqQueue<T> getQueue(String queueName, Class<T> payloadClass) {
        return getQueue(this.defaultPrefix, queueName, payloadClass);
    }

    /**
     * Retrieves or creates a typed {@link OxmqQueue} instance for the given queue name
     * within a specific namespace prefix.
     */
    @SuppressWarnings("unchecked")
    public <T> OxmqQueue<T> getQueue(String prefix, String queueName, Class<T> payloadClass) {
        Objects.requireNonNull(queueName, "queueName must not be null");
        String effectivePrefix = prefix != null && !prefix.isBlank() ? prefix.trim() : this.defaultPrefix;
        String cacheKey = (effectivePrefix != null ? effectivePrefix : "bull") + ":" + queueName;
        return (OxmqQueue<T>) queueCache.computeIfAbsent(cacheKey, k ->
                OxmqQueue.<T>builder()
                        .prefix(effectivePrefix)
                        .name(queueName)
                        .connectionManager(connectionManager)
                        .scriptManager(scriptManager)
                        .serializer(serializer)
                        .metrics(metrics)
                        .payloadClass(payloadClass != null ? payloadClass : (Class<T>) Object.class)
                        .defaultJobOptions(defaultJobOptions)
                        .maxLenEvents(maxLenEvents)
                        .build()
        );
    }

    /**
     * Retrieves or creates an {@link OxmqQueue} instance with untyped (Object) payload,
     * using the default service-level namespace.
     */
    public OxmqQueue<Object> getQueue(String queueName) {
        return getQueue(this.defaultPrefix, queueName, Object.class);
    }

    /**
     * Retrieves or creates an {@link OxmqQueue} instance with untyped (Object) payload
     * within a specific namespace prefix.
     */
    public OxmqQueue<Object> getQueue(String prefix, String queueName) {
        return getQueue(prefix, queueName, Object.class);
    }

    /**
     * Enqueues a job with a default job name and default options.
     */
    public <T> Job<T> send(String queueName, T payload) {
        return send(queueName, "default", payload, JobOptions.defaults());
    }

    /**
     * Enqueues a job with a specific job name and default options.
     */
    public <T> Job<T> send(String queueName, String jobName, T payload) {
        return send(queueName, jobName, payload, JobOptions.defaults());
    }

    /**
     * Enqueues a job with a default job name and custom options (e.g., delay, priority, retries).
     */
    public <T> Job<T> send(String queueName, T payload, JobOptions options) {
        return send(queueName, "default", payload, options);
    }

    /**
     * Enqueues a job with a specific job name, payload, and options.
     */
    @SuppressWarnings("unchecked")
    public <T> Job<T> send(String queueName, String jobName, T payload, JobOptions options) {
        Class<T> clazz = payload != null ? (Class<T>) payload.getClass() : (Class<T>) Object.class;
        return getQueue(queueName, clazz).add(jobName, payload, options);
    }

    /**
     * Asynchronously enqueues a job.
     */
    public <T> CompletableFuture<Job<T>> sendAsync(String queueName, T payload) {
        return CompletableFuture.supplyAsync(() -> send(queueName, payload));
    }

    /**
     * Asynchronously enqueues a job with a name and options.
     */
    public <T> CompletableFuture<Job<T>> sendAsync(String queueName, String jobName, T payload, JobOptions options) {
        return CompletableFuture.supplyAsync(() -> send(queueName, jobName, payload, options));
    }

    /**
     * Enqueues multiple jobs in a single Redis atomic roundtrip.
     */
    @SuppressWarnings("unchecked")
    public <T> List<Job<T>> sendBulk(String queueName, List<JobRequest<T>> requests) {
        OxmqQueue<T> queue = (OxmqQueue<T>) getQueue(queueName);
        return queue.addBulk(requests);
    }

    /**
     * Retrieves an existing job by ID.
     */
    public <T> Job<T> getJob(String queueName, String jobId, Class<T> payloadClass) {
        return getQueue(queueName, payloadClass).getJob(jobId);
    }

    /**
     * Retrieves an existing job by ID.
     */
    public Job<Object> getJob(String queueName, String jobId) {
        return getQueue(queueName).getJob(jobId);
    }

    /**
     * Pauses the queue, preventing workers from picking up new jobs.
     */
    public void pause(String queueName) {
        getQueue(queueName).pause();
    }

    /**
     * Resumes a paused queue.
     */
    public void resume(String queueName) {
        getQueue(queueName).resume();
    }

    /**
     * Checks if the queue is paused.
     */
    public boolean isPaused(String queueName) {
        return getQueue(queueName).isPaused();
    }

    /**
     * Gets the count of jobs in a specific state for a queue.
     */
    public long count(String queueName, JobState state) {
        return getQueue(queueName).count(state);
    }

    /**
     * Completely wipes out all state and Redis keys for a queue.
     */
    public void obliterate(String queueName) {
        getQueue(queueName).obliterate();
        queueCache.remove(queueName);
    }

    /**
     * Drains all waiting jobs from the queue.
     */
    public void drain(String queueName) {
        drain(queueName, false);
    }

    /**
     * Drains waiting (and optionally delayed) jobs from the queue.
     */
    public void drain(String queueName, boolean delayed) {
        getQueue(queueName).drain(delayed);
    }

    /**
     * Retries failed or completed jobs in bulk on the specified queue.
     */
    public long retryJobs(String queueName, io.oxmq.model.RetryJobsOptions options) {
        return getQueue(queueName).retryJobs(options);
    }

    /**
     * Promotes delayed jobs in bulk to waiting state on the specified queue.
     */
    public long promoteJobs(String queueName, int count) {
        return getQueue(queueName).promoteJobs(count);
    }

    /**
     * Gets job count breakdown per priority on the specified queue.
     */
    public java.util.Map<Integer, Long> getCountsPerPriority(String queueName, int... priorities) {
        return getQueue(queueName).getCountsPerPriority(priorities);
    }

    /**
     * Retrieves child dependency counts for a parent job in a DAG workflow.
     */
    public io.oxmq.model.JobDependencyCounts getDependencyCounts(String queueName, String jobId) {
        return getQueue(queueName).getDependencyCounts(jobId);
    }

    /**
     * Trims the telemetry/event stream of the specified queue.
     */
    public void trimEvents(String queueName, long maxLength) {
        getQueue(queueName).trimEvents(maxLength);
    }

    /**
     * Returns the FlowProducer instance for orchestrating parent-child DAG workflows.
     */
    public FlowProducer getFlowProducer() {
        return flowProducer;
    }

    /**
     * Adds an entire DAG flow tree atomically.
     *
     * @param flowTree the root of the flow tree
     * @return the generated parent Job ID
     */
    public String addFlow(FlowJob<?> flowTree) {
        if (flowProducer == null) {
            throw new IllegalStateException("FlowProducer is not available in OxmqTemplate");
        }
        return flowProducer.add(flowTree);
    }

    /**
     * Adds multiple DAG flow trees in bulk.
     *
     * @param flowTrees the roots of the flow trees
     * @return the generated parent Job IDs
     */
    public List<String> addFlows(List<FlowJob<?>> flowTrees) {
        if (flowProducer == null) {
            throw new IllegalStateException("FlowProducer is not available in OxmqTemplate");
        }
        return flowProducer.addBulk(flowTrees);
    }

    /**
     * Waits for a job in the specified queue to finish and returns its result.
     */
    public Object waitUntilFinished(String queueName, String jobId, java.time.Duration timeout) throws Exception {
        return getQueue(queueName).waitUntilFinished(jobId, timeout);
    }

    /**
     * Asynchronously waits for a job in the specified queue to finish.
     */
    public CompletableFuture<Object> waitUntilFinishedAsync(String queueName, String jobId) {
        return getQueue(queueName).waitUntilFinishedAsync(jobId);
    }

    /**
     * Creates a new QueueEvents instance for listening to queue lifecycle events.
     */
    public QueueEvents createQueueEvents(String queueName) {
        return createQueueEvents(this.defaultPrefix, queueName);
    }

    /**
     * Creates a new QueueEvents instance for a specific namespace prefix and queue name.
     */
    public QueueEvents createQueueEvents(String prefix, String queueName) {
        return QueueEvents.builder()
                .prefix(prefix)
                .queueName(queueName)
                .connectionManager(connectionManager)
                .serializer(serializer)
                .build();
    }

    public RedisClient getRedisClient() {
        return redisClient;
    }

    public RedisConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public JobSerializer getSerializer() {
        return serializer;
    }

    public OxmqMetrics getMetrics() {
        return metrics;
    }

    public String getDefaultPrefix() {
        return defaultPrefix;
    }

    /**
     * Creates a scoped operations view targeting an external service's namespace for cross-service dispatching.
     *
     * @param targetPrefix the target service namespace prefix
     * @return cross-service queue operations
     */
    public ServiceQueueOperations toService(String targetPrefix) {
        return new ServiceQueueOperations(targetPrefix);
    }

    /**
     * Fluent cross-service queue operations scoped to a specific target service namespace.
     */
    public class ServiceQueueOperations {
        private final String targetPrefix;

        public ServiceQueueOperations(String targetPrefix) {
            this.targetPrefix = Objects.requireNonNull(targetPrefix, "targetPrefix must not be null");
        }

        public <T> OxmqQueue<T> getQueue(String queueName, Class<T> payloadClass) {
            return OxmqTemplate.this.getQueue(targetPrefix, queueName, payloadClass);
        }

        public OxmqQueue<Object> getQueue(String queueName) {
            return OxmqTemplate.this.getQueue(targetPrefix, queueName, Object.class);
        }

        @SuppressWarnings("unchecked")
        public <T> Job<T> send(String queueName, String jobName, T payload, JobOptions options) {
            Class<T> clazz = payload != null ? (Class<T>) payload.getClass() : (Class<T>) Object.class;
            return getQueue(queueName, clazz).add(jobName, payload, options);
        }

        public <T> Job<T> send(String queueName, String jobName, T payload) {
            return send(queueName, jobName, payload, JobOptions.defaults());
        }

        public <T> Job<T> send(String queueName, T payload, JobOptions options) {
            return send(queueName, "default", payload, options);
        }

        public <T> Job<T> send(String queueName, T payload) {
            return send(queueName, "default", payload, JobOptions.defaults());
        }

        public <T> CompletableFuture<Job<T>> sendAsync(String queueName, String jobName, T payload, JobOptions options) {
            return CompletableFuture.supplyAsync(() -> send(queueName, jobName, payload, options));
        }

        public <T> CompletableFuture<Job<T>> sendAsync(String queueName, T payload) {
            return CompletableFuture.supplyAsync(() -> send(queueName, payload));
        }

        public <T> Job<T> add(String queueName, T data) {
            return send(queueName, data);
        }

        public <T> Job<T> add(String queueName, String jobName, T data, JobOptions opts) {
            return send(queueName, jobName, data, opts);
        }

        public <T> CompletableFuture<Job<T>> addAsync(String queueName, T data) {
            return sendAsync(queueName, data);
        }

        public <T> CompletableFuture<Job<T>> addAsync(String queueName, String jobName, T data, JobOptions opts) {
            return sendAsync(queueName, jobName, data, opts);
        }

        public Object waitUntilFinished(String queueName, String jobId, java.time.Duration timeout) throws Exception {
            return getQueue(queueName).waitUntilFinished(jobId, timeout);
        }

        public CompletableFuture<Object> waitUntilFinishedAsync(String queueName, String jobId) {
            return getQueue(queueName).waitUntilFinishedAsync(jobId);
        }

        public String getTargetPrefix() {
            return targetPrefix;
        }
    }
}
