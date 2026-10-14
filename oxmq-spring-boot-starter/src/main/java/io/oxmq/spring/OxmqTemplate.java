package io.oxmq.spring;

import io.lettuce.core.RedisClient;
import io.oxmq.FlowProducer;
import io.oxmq.OxmqQueue;
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

    private final ConcurrentMap<String, OxmqQueue<?>> queueCache = new ConcurrentHashMap<>();

    public OxmqTemplate(RedisClient redisClient,
                        RedisConnectionManager connectionManager,
                        LuaScriptManager scriptManager,
                        JobSerializer serializer,
                        OxmqMetrics metrics,
                        FlowProducer flowProducer) {
        this.redisClient = Objects.requireNonNull(redisClient, "redisClient must not be null");
        this.connectionManager = Objects.requireNonNull(connectionManager, "connectionManager must not be null");
        this.scriptManager = Objects.requireNonNull(scriptManager, "scriptManager must not be null");
        this.serializer = Objects.requireNonNull(serializer, "serializer must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
        this.flowProducer = flowProducer;
    }

    /**
     * Retrieves or creates a typed {@link OxmqQueue} instance for the given queue name.
     */
    @SuppressWarnings("unchecked")
    public <T> OxmqQueue<T> getQueue(String queueName, Class<T> payloadClass) {
        Objects.requireNonNull(queueName, "queueName must not be null");
        return (OxmqQueue<T>) queueCache.computeIfAbsent(queueName, name ->
                OxmqQueue.<T>builder()
                        .name(name)
                        .connectionManager(connectionManager)
                        .scriptManager(scriptManager)
                        .serializer(serializer)
                        .metrics(metrics)
                        .payloadClass(payloadClass != null ? payloadClass : (Class<T>) Object.class)
                        .build()
        );
    }

    /**
     * Retrieves or creates an {@link OxmqQueue} instance with untyped (Object) payload.
     */
    public OxmqQueue<Object> getQueue(String queueName) {
        return getQueue(queueName, Object.class);
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
}
