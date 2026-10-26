package io.oxmq;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.oxmq.client.QueueKeys;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.lua.BullScripts;
import io.oxmq.lua.ExtensionScripts;
import io.oxmq.lua.LuaScript;
import io.oxmq.lua.LuaScriptManager;
import io.oxmq.metrics.OxmqMetrics;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import io.oxmq.serializer.JacksonJobSerializer;
import io.oxmq.serializer.JobSerializer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-performance Redis-backed job queue producer implementation.
 *
 * @param <T> Payload data type
 */
public class OxmqQueue<T> implements Queue<T> {

    private static final Logger log = LoggerFactory.getLogger(OxmqQueue.class);
    private static final String[] DEFAULT_COUNT_TYPES = {"wait", "active", "delayed", "completed", "failed", "paused", "waiting-children", "prioritized"};

    private final String name;
    private final String prefix;
    private final QueueKeys queueKeys;
    private final RedisConnectionManager connectionManager;
    private final LuaScriptManager scriptManager;
    private final BullScripts bullScripts;
    private final io.oxmq.lua.ExtensionScripts extensionScripts;
    private final JobSerializer serializer;
    private final OxmqMetrics metrics;
    private final Class<T> payloadClass;
    private final io.oxmq.backend.QueueBackend backend;
    private final JobOptions defaultJobOptions;

    @SuppressWarnings("unchecked")
    private final io.oxmq.model.JobContext queueJobContext = new io.oxmq.model.JobContext() {
        @Override
        public void updateData(String jobId, Object newData) {
            OxmqQueue.this.updateData(jobId, (T) newData);
        }

        @Override
        public void promote(String jobId) {
            OxmqQueue.this.promote(jobId);
        }

        @Override
        public void changeDelay(String jobId, java.time.Duration delay) {
            OxmqQueue.this.changeDelay(jobId, delay);
        }

        @Override
        public void changePriority(String jobId, int priority) {
            OxmqQueue.this.changePriority(jobId, priority);
        }

        @Override
        public void retry(String jobId) {
            OxmqQueue.this.retry(jobId);
        }

        @Override
        public boolean remove(String jobId) {
            return OxmqQueue.this.remove(jobId);
        }

        @Override
        public JobState getState(String jobId) {
            return JobState.fromBullMqState(OxmqQueue.this.getState(jobId));
        }

        @Override
        public io.oxmq.model.JobDependencies getDependencies(String jobId) {
            return OxmqQueue.this.getDependencies(jobId);
        }

        @Override
        public io.oxmq.model.JobNode getFlow(String jobId, int maxDepth) {
            return OxmqQueue.this.getFlow(jobId, maxDepth);
        }

        @Override
        public boolean clearLogs(String jobId) {
            return OxmqQueue.this.clearJobLogs(jobId);
        }

        @Override
        public void addLog(String jobId, String log) {
            OxmqQueue.this.addJobLog(jobId, log);
        }

        @Override
        public void updateProgress(String jobId, int progress, Object payload) {
            OxmqQueue.this.updateJobProgress(jobId, progress);
        }
    };

    public OxmqQueue(String prefix, String name, RedisConnectionManager connectionManager, LuaScriptManager scriptManager,
                     JobSerializer serializer, OxmqMetrics metrics, Class<T> payloadClass, io.oxmq.backend.QueueBackend backend,
                     JobOptions defaultJobOptions, int maxLenEvents) {
        this.name = Objects.requireNonNull(name, "Queue name must not be null");
        this.queueKeys = new QueueKeys(prefix, name);
        this.prefix = this.queueKeys.getQualifiedName();
        this.connectionManager = connectionManager;
        this.scriptManager = scriptManager != null ? scriptManager : new LuaScriptManager();
        this.bullScripts = new BullScripts(this.scriptManager);
        this.extensionScripts = new io.oxmq.lua.ExtensionScripts(this.scriptManager);
        this.serializer = serializer != null ? serializer : new JacksonJobSerializer();
        this.metrics = metrics != null ? metrics : new OxmqMetrics();
        this.payloadClass = payloadClass;
        this.backend = backend != null ? backend : new io.oxmq.backend.RedisQueueBackend(this.queueKeys, this.connectionManager, this.scriptManager, this.bullScripts);
        this.defaultJobOptions = defaultJobOptions;

        if (this.connectionManager != null) {
            try {
                long maxEvents = maxLenEvents > 0 ? maxLenEvents : 10000;
                this.connectionManager.getCommandConnection().sync().hset(queueKeys.toKey("meta"), "opts.maxLenEvents", String.valueOf(maxEvents));
            } catch (Exception e) {
                log.debug("Failed to set opts.maxLenEvents on queue meta: {}", e.getMessage());
            }
        }

        registerGauges();
    }

    public OxmqQueue(String prefix, String name, RedisConnectionManager connectionManager, LuaScriptManager scriptManager,
                     JobSerializer serializer, OxmqMetrics metrics, Class<T> payloadClass, io.oxmq.backend.QueueBackend backend) {
        this(prefix, name, connectionManager, scriptManager, serializer, metrics, payloadClass, backend, null, -1);
    }

    public OxmqQueue(String name, RedisConnectionManager connectionManager, LuaScriptManager scriptManager,
                     JobSerializer serializer, OxmqMetrics metrics, Class<T> payloadClass) {
        this(null, name, connectionManager, scriptManager, serializer, metrics, payloadClass, null);
    }

    public OxmqQueue(String name, RedisConnectionManager connectionManager, LuaScriptManager scriptManager,
                     JobSerializer serializer, OxmqMetrics metrics, Class<T> payloadClass, io.oxmq.backend.QueueBackend backend) {
        this(null, name, connectionManager, scriptManager, serializer, metrics, payloadClass, backend);
    }

    @Override
    public io.oxmq.backend.QueueBackend getBackend() {
        return backend;
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    private void registerGauges() {
        metrics.registerWaitingGauge(name, new CachedGaugeSupplier(() -> count(JobState.WAITING), 5000));
        metrics.registerActiveGauge(name, new CachedGaugeSupplier(() -> count(JobState.ACTIVE), 5000));
        metrics.registerDelayedGauge(name, new CachedGaugeSupplier(() -> count(JobState.DELAYED), 5000));
    }

    /**
     * Caches gauge evaluation results for a TTL window (5 seconds) to avoid spamming Redis with
     * synchronous network round-trips during high-frequency Prometheus scrapes.
     */
    private static class CachedGaugeSupplier implements java.util.function.Supplier<Number> {
        private final java.util.function.Supplier<Number> delegate;
        private final long ttlMs;
        private volatile long lastFetchTime = 0;
        private volatile Number cachedValue = 0L;

        CachedGaugeSupplier(java.util.function.Supplier<Number> delegate, long ttlMs) {
            this.delegate = delegate;
            this.ttlMs = ttlMs;
        }

        @Override
        public Number get() {
            long now = System.currentTimeMillis();
            if (now - lastFetchTime > ttlMs) {
                try {
                    cachedValue = delegate.get();
                    lastFetchTime = now;
                } catch (Exception ignored) {
                    // Retain previous cached value on transient Redis communication glitches
                }
            }
            return cachedValue;
        }
    }

    @Override
    public String getName() {
        return name;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getNamespace() {
        return queueKeys.getPrefix();
    }

    public QueueKeys getQueueKeys() {
        return queueKeys;
    }

    public JobOptions getDefaultJobOptions() {
        return defaultJobOptions;
    }

    private JobOptions resolveJobOptions(JobOptions opts) {
        if (opts == null) {
            return defaultJobOptions != null ? defaultJobOptions : JobOptions.defaults();
        }
        if (defaultJobOptions == null) {
            return opts;
        }
        JobOptions merged = new JobOptions();
        merged.setJobId(opts.getJobId() != null ? opts.getJobId() : defaultJobOptions.getJobId());
        merged.setDelayMs(opts.getDelayMs() > 0 ? opts.getDelayMs() : defaultJobOptions.getDelayMs());
        merged.setAttempts(opts.getAttempts() > 1 ? opts.getAttempts() : defaultJobOptions.getAttempts());
        merged.setBackoff(opts.getBackoff() != null ? opts.getBackoff() : defaultJobOptions.getBackoff());
        merged.setPriority(opts.getPriority() != 0 ? opts.getPriority() : defaultJobOptions.getPriority());
        merged.setLifo(opts.isLifo() || defaultJobOptions.isLifo());
        merged.setParentKey(opts.getParentKey() != null ? opts.getParentKey() : defaultJobOptions.getParentKey());
        merged.setDeduplicationId(opts.getDeduplicationId() != null ? opts.getDeduplicationId() : defaultJobOptions.getDeduplicationId());
        merged.setDeduplicationTtlMs(opts.getDeduplicationTtlMs() > 0 ? opts.getDeduplicationTtlMs() : defaultJobOptions.getDeduplicationTtlMs());
        merged.setKeepLastIfActive(opts.isKeepLastIfActive() || defaultJobOptions.isKeepLastIfActive());
        merged.setKeepLogs(opts.getKeepLogs() > 0 ? opts.getKeepLogs() : defaultJobOptions.getKeepLogs());
        merged.setGroupKey(opts.getGroupKey() != null ? opts.getGroupKey() : defaultJobOptions.getGroupKey());
        merged.setTraceparent(opts.getTraceparent() != null ? opts.getTraceparent() : defaultJobOptions.getTraceparent());
        merged.setTracestate(opts.getTracestate() != null ? opts.getTracestate() : defaultJobOptions.getTracestate());
        merged.setTelemetry(opts.getTelemetry() != null ? opts.getTelemetry() : defaultJobOptions.getTelemetry());
        merged.setFailParentOnFailure(opts.isFailParentOnFailure() || defaultJobOptions.isFailParentOnFailure());
        merged.setRemoveDependencyOnFailure(opts.isRemoveDependencyOnFailure() || defaultJobOptions.isRemoveDependencyOnFailure());
        merged.setContinueParentOnFailure(opts.isContinueParentOnFailure() || defaultJobOptions.isContinueParentOnFailure());
        merged.setIgnoreDependencyOnFailure(opts.isIgnoreDependencyOnFailure() || defaultJobOptions.isIgnoreDependencyOnFailure());

        if (opts.getKeepCompletedJobs() != null) {
            merged.setKeepCompletedJobs(opts.getKeepCompletedJobs());
        } else if (opts.isRemoveOnComplete()) {
            merged.setRemoveOnComplete(true);
        } else if (defaultJobOptions.getKeepCompletedJobs() != null) {
            merged.setKeepCompletedJobs(defaultJobOptions.getKeepCompletedJobs());
        } else if (defaultJobOptions.isRemoveOnComplete()) {
            merged.setRemoveOnComplete(true);
        }

        if (opts.getKeepFailedJobs() != null) {
            merged.setKeepFailedJobs(opts.getKeepFailedJobs());
        } else if (opts.isRemoveOnFail()) {
            merged.setRemoveOnFail(true);
        } else if (defaultJobOptions.getKeepFailedJobs() != null) {
            merged.setKeepFailedJobs(defaultJobOptions.getKeepFailedJobs());
        } else if (defaultJobOptions.isRemoveOnFail()) {
            merged.setRemoveOnFail(true);
        }

        return merged;
    }

    @Override
    public Job<T> add(String jobName, T data) {
        return add(jobName, data, null);
    }

    @Override
    public Job<T> add(String jobName, T data, JobOptions opts) {
        Objects.requireNonNull(jobName, "jobName must not be null");
        JobOptions options = resolveJobOptions(opts);

        String serializedData = serializer.serialize(data);
        long now = System.currentTimeMillis();
        long delayMs = options.getDelayMs();
        String customJobId = options.getJobId() != null ? options.getJobId() : "";

        Map<String, Object> optsMap = options.toMap();

        StatefulRedisConnection<String, byte[]> binConn = connectionManager.getBinaryConnection();
        String jobId;
        String groupId = options.getGroup() != null ? options.getGroup().getId() : options.getGroupKey();
        if (groupId != null && !groupId.isBlank()) {
            long maxSize = options.getGroup() != null ? options.getGroup().getMaxSize() : 0;
            int priority = options.getGroup() != null && options.getGroup().getPriority() > 0
                    ? options.getGroup().getPriority()
                    : options.getPriority();
            String dedupId = options.getDeduplicationId();
            jobId = extensionScripts.addGroupJob(binConn, queueKeys, customJobId, jobName,
                    serializedData, serializer.serialize(options), groupId, maxSize, priority, dedupId, now);
        } else if (delayMs > 0) {
            jobId = bullScripts.addDelayedJob(binConn, queueKeys, customJobId, jobName, serializedData, optsMap, now, delayMs);
        } else if (options.getPriority() > 0) {
            jobId = bullScripts.addPrioritizedJob(binConn, queueKeys, customJobId, jobName, serializedData, optsMap, now, options.getPriority());
        } else {
            jobId = bullScripts.addStandardJob(binConn, queueKeys, customJobId, jobName, serializedData, optsMap, now);
        }

        metrics.recordJobEnqueued(name);
        log.debug("Enqueued job {} [id: {}] in queue {}", jobName, jobId, name);

        Job<T> job = new Job<>(jobId, jobName, data, options);
        job.setQueueName(name);
        job.setTimestamp(now);
        wireJobLifecycle(job);
        return job;
    }

    @Override
    public CompletableFuture<Job<T>> addAsync(String jobName, T data) {
        return addAsync(jobName, data, null);
    }

    @Override
    public CompletableFuture<Job<T>> addAsync(String jobName, T data, JobOptions opts) {
        Objects.requireNonNull(jobName, "jobName must not be null");
        JobOptions options = resolveJobOptions(opts);

        String serializedData = serializer.serialize(data);
        long now = System.currentTimeMillis();
        long delayMs = options.getDelayMs();
        String customJobId = options.getJobId() != null ? options.getJobId() : "";

        Map<String, Object> optsMap = options.toMap();

        StatefulRedisConnection<String, byte[]> binConn = connectionManager.getBinaryConnection();
        CompletableFuture<String> futureJobId;
        String groupId = options.getGroup() != null ? options.getGroup().getId() : options.getGroupKey();
        if (groupId != null && !groupId.isBlank()) {
            long maxSize = options.getGroup() != null ? options.getGroup().getMaxSize() : 0;
            int priority = options.getGroup() != null && options.getGroup().getPriority() > 0
                    ? options.getGroup().getPriority()
                    : options.getPriority();
            String dedupId = options.getDeduplicationId();
            futureJobId = extensionScripts.addGroupJobAsync(binConn, queueKeys, customJobId, jobName,
                    serializedData, serializer.serialize(options), groupId, maxSize, priority, dedupId, now);
        } else if (delayMs > 0) {
            futureJobId = bullScripts.addDelayedJobAsync(binConn, queueKeys, customJobId, jobName, serializedData, optsMap, now, delayMs);
        } else if (options.getPriority() > 0) {
            futureJobId = bullScripts.addPrioritizedJobAsync(binConn, queueKeys, customJobId, jobName, serializedData, optsMap, now, options.getPriority());
        } else {
            futureJobId = bullScripts.addStandardJobAsync(binConn, queueKeys, customJobId, jobName, serializedData, optsMap, now);
        }

        return futureJobId.thenApply(jobId -> {
            metrics.recordJobEnqueued(name);
            log.debug("Enqueued job {} [id: {}] in queue {}", jobName, jobId, name);

            Job<T> job = new Job<>(jobId, jobName, data, options);
            job.setQueueName(name);
            job.setTimestamp(now);
            wireJobLifecycle(job);
            return job;
        });
    }

    @Override
    public Job<T> getJob(String jobId) {
        if (jobId == null || jobId.isBlank()) {
            return null;
        }
        StatefulRedisConnection<String, String> conn = connectionManager.getCommandConnection();
        Map<String, String> fields = conn.sync().hgetall(prefix + ":" + jobId);
        if (fields == null || fields.isEmpty()) {
            return null;
        }
        return buildJobFromFields(jobId, fields, null);
    }

    private Job<T> buildJobFromFields(String jobId, Map<String, String> fields, JobState state) {
        if (fields == null || fields.isEmpty()) {
            return null;
        }

        String jobName = fields.get("name");
        String dataStr = fields.get("data");
        String optsStr = fields.get("opts");
        String progressStr = fields.get("progress");
        String attemptsStr = fields.get("attemptsMade");
        String timestampStr = fields.get("timestamp");
        String processedStr = fields.get("processedOn");
        String finishedStr = fields.get("finishedOn");
        String returnStr = fields.get("returnvalue");
        String failedStr = fields.get("failedReason");
        String parentKeyStr = fields.get("parentKey");
        String checkpointStr = fields.get("checkpoint");
        String checkpointStepStr = fields.get("checkpoint_step");

        T data = payloadClass != null ? serializer.deserialize(dataStr, payloadClass) : (T) dataStr;
        JobOptions opts = (optsStr == null || optsStr.isBlank() || "{}".equals(optsStr))
                ? JobOptions.defaults()
                : serializer.deserialize(optsStr, JobOptions.class);

        Job<T> job = new Job<>(jobId, jobName, data, opts);
        job.setQueueName(name);
        if (progressStr != null) job.setProgress(Integer.parseInt(progressStr));
        if (attemptsStr != null) job.setAttemptsMade(Integer.parseInt(attemptsStr));
        if (timestampStr != null) job.setTimestamp(Long.parseLong(timestampStr));
        if (processedStr != null) job.setProcessedOn(Long.parseLong(processedStr));
        if (finishedStr != null) job.setFinishedOn(Long.parseLong(finishedStr));
        if (returnStr != null) job.setReturnvalue(returnStr);
        if (failedStr != null) job.setFailedReason(failedStr);
        if (parentKeyStr != null) job.setParentKey(parentKeyStr);
        if (checkpointStr != null) job.setCheckpointJson(checkpointStr);
        if (checkpointStepStr != null) job.setCheckpointStep(checkpointStepStr);

        if (state != null) {
            job.setState(state);
        } else {
            job.setStateSupplier(() -> {
                String stateStr = getState(jobId);
                return JobState.fromBullMqState(stateStr);
            });
        }

        // Memory optimization: lazily load children values only when requested
        job.setChildrenValuesSupplier(() -> {
            try {
                StatefulRedisConnection<String, String> conn = connectionManager.getCommandConnection();
                Map<String, String> childValsRaw = conn.sync().hgetall(prefix + ":" + jobId + ":processed");
                if (childValsRaw == null || childValsRaw.isEmpty()) {
                    childValsRaw = conn.sync().hgetall(prefix + ":" + jobId + ":childrenValues");
                }
                if (childValsRaw != null && !childValsRaw.isEmpty()) {
                    Map<String, Object> childVals = new java.util.HashMap<>(childValsRaw.size());
                    for (Map.Entry<String, String> entry : childValsRaw.entrySet()) {
                        try {
                            childVals.put(entry.getKey(), serializer.deserialize(entry.getValue(), Object.class));
                        } catch (Exception ex) {
                            childVals.put(entry.getKey(), entry.getValue());
                        }
                    }
                    return childVals;
                }
            } catch (Exception ignored) {}
            return null;
        });

        // Memory optimization: lazily load ignored children failures when requested
        job.setIgnoredChildrenFailuresSupplier(() -> {
            try {
                StatefulRedisConnection<String, String> conn = connectionManager.getCommandConnection();
                Map<String, String> failures = conn.sync().hgetall(prefix + ":" + jobId + ":failed");
                return failures != null ? failures : Map.of();
            } catch (Exception ignored) {}
            return Map.of();
        });

        wireJobLifecycle(job);
        return job;
    }

    private void wireJobLifecycle(Job<T> job) {
        if (job == null) return;
        job.setContext(queueJobContext);
    }

    @Override
    public void pause() {
        bullScripts.pause(connectionManager.getBinaryConnection(), queueKeys, true);
        log.info("Paused queue {}", name);
    }

    @Override
    public void resume() {
        bullScripts.pause(connectionManager.getBinaryConnection(), queueKeys, false);
        log.info("Resumed queue {}", name);
    }

    @Override
    public boolean isPaused() {
        RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
        String paused = sync.hget(queueKeys.toKey("meta"), "paused");
        return "1".equals(paused) || "true".equalsIgnoreCase(paused);
    }

    @Override
    public long count(JobState state) {
        RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
        try {
            return switch (state) {
                case WAITING -> sync.llen(queueKeys.toKey("wait"));
                case ACTIVE -> sync.llen(queueKeys.toKey("active"));
                case DELAYED -> sync.zcard(queueKeys.toKey("delayed"));
                case COMPLETED -> sync.zcard(queueKeys.toKey("completed"));
                case FAILED -> sync.zcard(queueKeys.toKey("failed"));
                case WAITING_CHILDREN -> sync.zcard(queueKeys.toKey("waiting-children"));
                default -> 0L;
            };
        } catch (Exception e) {
            return 0L;
        }
    }

    @Override
    public long clean(long graceMs, int limit, JobState state) {
        String set = switch (state) {
            case COMPLETED -> "completed";
            case FAILED -> "failed";
            case DELAYED -> "delayed";
            default -> throw new IllegalArgumentException("Cannot clean state: " + state);
        };
        Long cleaned = bullScripts.cleanJobsInSet(connectionManager.getBinaryConnection(), queueKeys, set, graceMs, limit);
        return cleaned != null ? cleaned : 0L;
    }

    @Override
    public void obliterate() {
        bullScripts.obliterate(connectionManager.getBinaryConnection(), queueKeys, 1000, true);
        log.info("Obliterated queue {}", name);
    }

    @Override
    public List<Job<T>> addBulk(List<io.oxmq.model.JobRequest<T>> jobs) {
        if (jobs == null || jobs.isEmpty()) {
            return List.of();
        }
        if (jobs.size() == 1) {
            io.oxmq.model.JobRequest<T> req = jobs.get(0);
            return List.of(add(req.name(), req.data(), req.options()));
        }

        StatefulRedisConnection<String, byte[]> binConn = connectionManager.getBinaryConnection();
        long now = System.currentTimeMillis();
        int size = jobs.size();
        List<CompletableFuture<String>> futures = new java.util.ArrayList<>(size);
        JobOptions[] resolvedOptions = new JobOptions[size];

        for (int i = 0; i < size; i++) {
            io.oxmq.model.JobRequest<T> req = jobs.get(i);
            String jobName = req.name() != null ? req.name() : "default";
            T data = req.data();
            JobOptions options = resolveJobOptions(req.options());
            resolvedOptions[i] = options;
            String serializedData = serializer.serialize(data);
            long delayMs = options.getDelayMs();
            String customJobId = options.getJobId() != null ? options.getJobId() : "";

            String groupId = options.getGroup() != null ? options.getGroup().getId() : options.getGroupKey();
            CompletableFuture<String> future;

            if (groupId != null && !groupId.isBlank()) {
                long maxSize = options.getGroup() != null ? options.getGroup().getMaxSize() : 0;
                int priority = options.getGroup() != null && options.getGroup().getPriority() > 0
                        ? options.getGroup().getPriority()
                        : options.getPriority();
                String dedupId = options.getDeduplicationId();
                future = extensionScripts.addGroupJobAsync(binConn, queueKeys, customJobId, jobName,
                        serializedData, serializer.serialize(options), groupId, maxSize, priority, dedupId, now);
            } else {
                Map<String, Object> optsMap = options.toMap();
                if (delayMs > 0) {
                    future = bullScripts.addDelayedJobAsync(binConn, queueKeys, customJobId, jobName, serializedData, optsMap, now, delayMs);
                } else if (options.getPriority() > 0) {
                    future = bullScripts.addPrioritizedJobAsync(binConn, queueKeys, customJobId, jobName, serializedData, optsMap, now, options.getPriority());
                } else {
                    future = bullScripts.addStandardJobAsync(binConn, queueKeys, customJobId, jobName, serializedData, optsMap, now);
                }
            }

            futures.add(future);
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Error awaiting pipelined addBulk futures on queue {}", name, e);
        }

        List<Job<T>> createdJobs = new java.util.ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            io.oxmq.model.JobRequest<T> req = jobs.get(i);
            JobOptions options = resolvedOptions[i];
            CompletableFuture<String> fut = futures.get(i);
            String jobId;
            try {
                jobId = fut.get();
                if (jobId == null) {
                    jobId = "unknown";
                }
            } catch (Exception ex) {
                log.warn("Failed to retrieve jobId for bulk job #{} on queue {}: {}", i, name, ex.getMessage());
                jobId = "unknown";
            }

            String jobName = req.name() != null ? req.name() : "default";
            Job<T> job = new Job<>(jobId, jobName, req.data(), options);
            job.setQueueName(name);
            job.setTimestamp(now);
            wireJobLifecycle(job);
            createdJobs.add(job);
            metrics.recordJobEnqueued(name);
        }

        return createdJobs;
    }

    @Override
    public void promote(String jobId) {
        Objects.requireNonNull(jobId, "jobId must not be null");
        bullScripts.promote(connectionManager.getBinaryConnection(), queueKeys, jobId);
        log.debug("Promoted job {} in queue {}", jobId, name);
    }

    @Override
    public void changeDelay(String jobId, java.time.Duration delay) {
        Objects.requireNonNull(jobId, "jobId must not be null");
        long delayMs = delay != null ? delay.toMillis() : 0L;
        bullScripts.changeDelay(connectionManager.getBinaryConnection(), queueKeys, jobId, delayMs);
        log.debug("Changed delay for job {} to {} ms in queue {}", jobId, delayMs, name);
    }

    @Override
    public void changePriority(String jobId, int priority) {
        Objects.requireNonNull(jobId, "jobId must not be null");
        bullScripts.changePriority(connectionManager.getBinaryConnection(), queueKeys, jobId, priority, false);
        log.debug("Changed priority for job {} to {} in queue {}", jobId, priority, name);
    }

    @Override
    public void retry(String jobId) {
        retry(jobId, io.oxmq.model.RetryOptions.defaults());
    }

    @Override
    public void retry(String jobId, io.oxmq.model.RetryOptions options) {
        Objects.requireNonNull(jobId, "jobId must not be null");
        io.oxmq.model.RetryOptions opts = options != null ? options : io.oxmq.model.RetryOptions.defaults();
        String state = getState(jobId);
        if ("failed".equals(state) || "completed".equals(state)) {
            bullScripts.reprocessJob(connectionManager.getBinaryConnection(), queueKeys, jobId, state, false,
                    opts.isResetAttemptsMade(), opts.isResetAttemptsStarted());
        } else {
            bullScripts.retryJob(connectionManager.getBinaryConnection(), queueKeys, jobId, "0", false);
        }
        log.debug("Retried job {} in queue {}", jobId, name);
    }

    @Override
    public boolean remove(String jobId) {
        return remove(jobId, false);
    }

    @Override
    public boolean remove(String jobId, boolean removeChildren) {
        if (jobId == null || jobId.isBlank()) {
            return false;
        }
        int res = bullScripts.removeJob(connectionManager.getBinaryConnection(), queueKeys, jobId, removeChildren);
        log.debug("Removed job {} from queue {} (result: {})", jobId, name, res);
        return res == 1;
    }

    @Override
    public void updateData(String jobId, T data) {
        Objects.requireNonNull(jobId, "jobId must not be null");
        String serialized = serializer.serialize(data);
        bullScripts.updateData(connectionManager.getBinaryConnection(), queueKeys, jobId, serialized);
        log.debug("Updated data for job {} in queue {}", jobId, name);
    }

    @Override
    public void drain(boolean delayed) {
        bullScripts.drain(connectionManager.getBinaryConnection(), queueKeys, delayed);
        log.info("Drained queue {} (delayed={})", name, delayed);
    }

    @Override
    public String getState(String jobId) {
        if (jobId == null || jobId.isBlank()) {
            return "unknown";
        }
        return bullScripts.getState(connectionManager.getBinaryConnection(), queueKeys, jobId);
    }

    @Override
    public java.util.Map<String, Long> getJobCounts() {
        List<Long> counts = bullScripts.getCounts(connectionManager.getBinaryConnection(), queueKeys, DEFAULT_COUNT_TYPES);
        java.util.Map<String, Long> map = new java.util.LinkedHashMap<>(DEFAULT_COUNT_TYPES.length);
        for (int i = 0; i < DEFAULT_COUNT_TYPES.length && i < counts.size(); i++) {
            map.put(DEFAULT_COUNT_TYPES[i], counts.get(i));
        }
        return map;
    }

    @Override
    public List<String> getJobLogs(String jobId) {
        return getJobLogs(jobId, 0, -1);
    }

    @Override
    public List<String> getJobLogs(String jobId, int start, int end) {
        if (jobId == null || jobId.isBlank()) {
            return List.of();
        }
        RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
        List<String> logs = sync.lrange(queueKeys.toJobLogsKey(jobId), start, end);
        return logs != null ? logs : List.of();
    }

    @Override
    public boolean clearJobLogs(String jobId) {
        if (jobId == null || jobId.isBlank()) {
            return false;
        }
        try {
            RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
            Long del = sync.del(queueKeys.toJobLogsKey(jobId));
            return del != null && del > 0;
        } catch (Exception e) {
            log.warn("Failed to clear logs for job {} in queue {}: {}", jobId, name, e.getMessage());
            return false;
        }
    }

    @Override
    public long addJobLog(String jobId, String logRow) {
        return addJobLog(jobId, logRow, 0);
    }

    @Override
    public long addJobLog(String jobId, String logRow, int keepLogs) {
        if (jobId == null || jobId.isBlank() || logRow == null) {
            return 0L;
        }
        try {
            return bullScripts.addLog(connectionManager.getBinaryConnection(), queueKeys, jobId, logRow, keepLogs);
        } catch (Exception e) {
            log.warn("Failed to append log for job {} in queue {}: {}", jobId, name, e.getMessage());
            return 0L;
        }
    }

    @Override
    public void updateJobProgress(String jobId, int progress) {
        if (jobId == null || jobId.isBlank()) {
            return;
        }
        try {
            bullScripts.updateProgress(connectionManager.getBinaryConnection(), queueKeys, jobId, String.valueOf(progress));
        } catch (Exception e) {
            log.warn("Failed to update progress for job {} in queue {}: {}", jobId, name, e.getMessage());
        }
    }

    @Override
    public boolean removeDeduplicationKey(String deduplicationId) {
        if (deduplicationId == null || deduplicationId.isBlank()) {
            return false;
        }
        RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
        Long del = sync.del(queueKeys.toKey("de:" + deduplicationId));
        return del != null && del > 0;
    }

    @Override
    public String getDeduplicationJobId(String deduplicationId) {
        if (deduplicationId == null || deduplicationId.isBlank()) {
            return null;
        }
        try {
            RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
            return sync.get(queueKeys.toKey("de:" + deduplicationId));
        } catch (Exception e) {
            log.warn("Failed to get deduplication job ID for key {} in queue {}: {}", deduplicationId, name, e.getMessage());
            return null;
        }
    }

    @Override
    public String upsertJobScheduler(String schedulerId, java.time.Duration every, String jobName, T data, JobOptions opts) {
        Objects.requireNonNull(schedulerId, "schedulerId must not be null");
        Objects.requireNonNull(jobName, "jobName must not be null");
        long intervalMs = every != null ? every.toMillis() : 60_000L;
        long nextMillis = System.currentTimeMillis() + intervalMs;
        String serializedData = serializer.serialize(data);
        JobOptions options = opts != null ? opts : JobOptions.defaults();
        return bullScripts.addJobScheduler(connectionManager.getBinaryConnection(), queueKeys,
            schedulerId, nextMillis, jobName, serializedData, options.toMap(), intervalMs, null, null);
    }

    @Override
    public boolean removeJobScheduler(String schedulerId) {
        if (schedulerId == null || schedulerId.isBlank()) {
            return false;
        }
        int res = bullScripts.removeJobScheduler(connectionManager.getBinaryConnection(), queueKeys, schedulerId);
        return res == 0;
    }

    @Override
    public String upsertJobScheduler(String schedulerId, String cronPattern, String jobName, T data, JobOptions opts) {
        Objects.requireNonNull(schedulerId, "schedulerId must not be null");
        Objects.requireNonNull(jobName, "jobName must not be null");
        Objects.requireNonNull(cronPattern, "cronPattern must not be null");
        long now = System.currentTimeMillis();
        long nextMillis = io.oxmq.util.CronEvaluator.parse(cronPattern).nextExecution(now, null);
        String serializedData = serializer.serialize(data);
        JobOptions options = opts != null ? opts : JobOptions.defaults();
        return bullScripts.addJobScheduler(connectionManager.getBinaryConnection(), queueKeys, schedulerId, nextMillis, jobName, serializedData, options.toMap(), null, cronPattern, null);
    }

    @Override
    public long retryJobs(io.oxmq.model.RetryJobsOptions options) {
        io.oxmq.model.RetryJobsOptions opts = options != null ? options : io.oxmq.model.RetryJobsOptions.defaults();
        return retryJobs(opts.getState(), opts.getCount(), opts.getTimestamp());
    }

    @Override
    public long retryJobs(JobState state, int count, Long timestamp) {
        String stateStr = state != null ? state.name().toLowerCase() : "failed";
        int batchCount = count > 0 ? count : 1000;
        long ts = timestamp != null ? timestamp : System.currentTimeMillis();
        long iterations = 0;
        Long cursor;
        do {
            cursor = bullScripts.moveJobsToWait(connectionManager.getBinaryConnection(), queueKeys, stateStr, batchCount, ts);
            iterations++;
        } while (cursor != null && cursor == 1L);
        return iterations;
    }

    @Override
    public long promoteJobs(int count) {
        int batchCount = count > 0 ? count : 1000;
        long iterations = 0;
        Long res;
        do {
            res = bullScripts.moveJobsToWait(connectionManager.getBinaryConnection(), queueKeys, "delayed", batchCount, Long.MAX_VALUE);
            iterations++;
        } while (res != null && res == 1L);
        return iterations;
    }

    @Override
    public void trimEvents(long maxLength) {
        connectionManager.getCommandConnection().sync().xtrim(queueKeys.getKey("events"), maxLength);
    }

    @Override
    public Map<Integer, Long> getCountsPerPriority(int... priorities) {
        if (priorities == null || priorities.length == 0) {
            return Map.of();
        }
        List<Long> counts = bullScripts.getCountsPerPriority(connectionManager.getBinaryConnection(), queueKeys, priorities);
        Map<Integer, Long> map = new java.util.LinkedHashMap<>();
        for (int i = 0; i < priorities.length; i++) {
            map.put(priorities[i], i < counts.size() ? counts.get(i) : 0L);
        }
        return map;
    }

    @Override
    public Map<JobState, List<String>> getRanges(JobState[] states, long start, long end, boolean asc) {
        if (states == null || states.length == 0) {
            return Map.of();
        }
        String[] stateNames = new String[states.length];
        for (int i = 0; i < states.length; i++) {
            stateNames[i] = states[i] == JobState.WAITING ? "wait" : states[i].name().toLowerCase();
        }
        List<List<String>> raw = bullScripts.getRanges(connectionManager.getBinaryConnection(), queueKeys, start, end, asc, stateNames);
        Map<JobState, List<String>> result = new java.util.LinkedHashMap<>();
        for (int i = 0; i < states.length; i++) {
            result.put(states[i], i < raw.size() ? raw.get(i) : List.of());
        }
        return result;
    }

    @Override
    public io.oxmq.model.JobDependencyCounts getDependencyCounts(String jobId) {
        List<Long> counts = bullScripts.getDependencyCounts(connectionManager.getBinaryConnection(), queueKeys, jobId,
                "processed", "unprocessed", "failed", "ignored");
        long processed = !counts.isEmpty() ? counts.get(0) : 0L;
        long unprocessed = counts.size() > 1 ? counts.get(1) : 0L;
        long failed = counts.size() > 2 ? counts.get(2) : 0L;
        long ignored = counts.size() > 3 ? counts.get(3) : 0L;
        return new io.oxmq.model.JobDependencyCounts(processed, unprocessed, failed, ignored);
    }

    @Override
    public io.oxmq.model.JobDependencies getDependencies(String jobId) {
        if (jobId == null || jobId.isBlank()) {
            return new io.oxmq.model.JobDependencies(jobId, Collections.emptyList(), Collections.emptyMap(), Collections.emptyMap());
        }
        try {
            var sync = connectionManager.getCommandConnection().sync();
            String jobKey = queueKeys.toJobKey(jobId);
            Set<String> unprocessed = sync.smembers(jobKey + ":dependencies");
            Map<String, String> processed = sync.hgetall(jobKey + ":processed");
            Map<String, String> failed = sync.hgetall(jobKey + ":failed");

            return new io.oxmq.model.JobDependencies(
                    jobId,
                    unprocessed != null ? new ArrayList<>(unprocessed) : Collections.emptyList(),
                    processed != null ? processed : Collections.emptyMap(),
                    failed != null ? failed : Collections.emptyMap()
            );
        } catch (Exception e) {
            log.warn("Failed to get dependencies for job {} in queue {}: {}", jobId, name, e.getMessage());
            return new io.oxmq.model.JobDependencies(jobId, Collections.emptyList(), Collections.emptyMap(), Collections.emptyMap());
        }
    }

    @Override
    public io.oxmq.model.JobNode getFlow(String jobId) {
        return getFlow(jobId, 10);
    }

    @Override
    public io.oxmq.model.JobNode getFlow(String jobId, int maxDepth) {
        try {
            FlowProducer flowProducer = new FlowProducer(connectionManager, scriptManager, serializer, queueKeys.getPrefix());
            return flowProducer.getFlow(name, jobId, maxDepth);
        } catch (Exception e) {
            log.warn("Failed to get flow for job {} in queue {}: {}", jobId, name, e.getMessage());
            return null;
        }
    }

    @Override
    public boolean removeChildDependency(String jobId, String parentKey) {
        return bullScripts.removeChildDependency(connectionManager.getBinaryConnection(), queueKeys, jobId, parentKey);
    }

    @Override
    public void removeUnprocessedChildren(String jobId) {
        bullScripts.removeUnprocessedChildren(connectionManager.getBinaryConnection(), queueKeys, jobId);
    }

    @Override
    public Map<String, Object> getJobScheduler(String schedulerId) {
        List<Object> raw = bullScripts.getJobScheduler(connectionManager.getBinaryConnection(), queueKeys, schedulerId);
        Map<String, Object> map = new java.util.LinkedHashMap<>();
        if (raw != null && raw.size() >= 2) {
            if (raw.get(0) instanceof Map<?, ?> m) {
                m.forEach((k, v) -> map.put(k != null ? k.toString() : "", v));
            }
            if (raw.get(1) != null) {
                map.put("nextTimestamp", raw.get(1));
            }
        }
        return map;
    }

    @Override
    public long getJobSchedulersCount() {
        Long card = connectionManager.getCommandConnection().sync().zcard(queueKeys.getKey("repeat"));
        return card != null ? card : 0L;
    }

    @Override
    public boolean isMaxed() {
        return bullScripts.isMaxed(connectionManager.getBinaryConnection(), queueKeys);
    }

    @Override
    public long removeOrphanedJobs(List<String> candidateJobIds) {
        Long removed = bullScripts.removeOrphanedJobs(connectionManager.getBinaryConnection(), queueKeys, candidateJobIds);
        return removed != null ? removed : 0L;
    }

    @Override
    public JobState getJobState(String jobId) {
        return JobState.fromBullMqState(getState(jobId));
    }

    @Override
    public boolean isCompleted(String jobId) {
        List<Object> res = isFinished(jobId, false);
        if (!res.isEmpty() && res.get(0) instanceof Number num) {
            return num.intValue() == 1;
        }
        return false;
    }

    @Override
    public boolean isFailed(String jobId) {
        List<Object> res = isFinished(jobId, false);
        if (!res.isEmpty() && res.get(0) instanceof Number num) {
            return num.intValue() == 2;
        }
        return false;
    }

    @Override
    public List<Object> isFinished(String jobId, boolean fetchValue) {
        if (jobId == null || jobId.isBlank()) {
            return List.of();
        }
        return bullScripts.isFinished(connectionManager.getBinaryConnection(), queueKeys, jobId, fetchValue);
    }

    private volatile QueueEvents internalQueueEvents;

    public synchronized QueueEvents getQueueEvents() {
        if (internalQueueEvents == null) {
            internalQueueEvents = QueueEvents.builder()
                    .prefix(queueKeys.getPrefix())
                    .queueName(name)
                    .connectionManager(connectionManager)
                    .serializer(serializer)
                    .build()
                    .start();
        }
        return internalQueueEvents;
    }

    @Override
    public Object waitUntilFinished(String jobId, Duration timeout) throws Exception {
        return waitUntilFinished(jobId, getQueueEvents(), timeout);
    }

    @Override
    public Object waitUntilFinished(String jobId, QueueEvents events, Duration timeout) throws Exception {
        List<Object> finishedCheck = isFinished(jobId, true);
        if (finishedCheck != null && !finishedCheck.isEmpty()) {
            int status = ((Number) finishedCheck.get(0)).intValue();
            if (status == 1) {
                Object val = finishedCheck.size() > 1 ? finishedCheck.get(1) : null;
                if (val instanceof byte[] b) {
                    return new String(b, StandardCharsets.UTF_8);
                }
                return val;
            } else if (status == 2) {
                Object err = finishedCheck.size() > 1 ? finishedCheck.get(1) : "Job failed";
                String reason = err instanceof byte[] b ? new String(b, StandardCharsets.UTF_8) : String.valueOf(err);
                throw new RuntimeException(reason);
            } else if (status == -1) {
                throw new IllegalArgumentException("Job " + jobId + " does not exist in queue " + name);
            }
        }
        return events.waitUntilFinished(jobId, timeout);
    }

    @Override
    public CompletableFuture<Object> waitUntilFinishedAsync(String jobId) {
        return waitUntilFinishedAsync(jobId, getQueueEvents());
    }

    @Override
    public CompletableFuture<Object> waitUntilFinishedAsync(String jobId, QueueEvents events) {
        List<Object> finishedCheck = isFinished(jobId, true);
        if (finishedCheck != null && !finishedCheck.isEmpty()) {
            int status = ((Number) finishedCheck.get(0)).intValue();
            if (status == 1) {
                Object val = finishedCheck.size() > 1 ? finishedCheck.get(1) : null;
                if (val instanceof byte[] b) {
                    val = new String(b, StandardCharsets.UTF_8);
                }
                return CompletableFuture.completedFuture(val);
            } else if (status == 2) {
                Object err = finishedCheck.size() > 1 ? finishedCheck.get(1) : "Job failed";
                String reason = err instanceof byte[] b ? new String(b, StandardCharsets.UTF_8) : String.valueOf(err);
                return CompletableFuture.failedFuture(new RuntimeException(reason));
            } else if (status == -1) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("Job " + jobId + " does not exist in queue " + name));
            }
        }
        return events.waitUntilFinishedAsync(jobId);
    }

    @Override
    public List<Job<T>> getJobs(List<JobState> states, int start, int end, boolean asc) {
        if (states == null || states.isEmpty()) {
            return List.of();
        }
        String[] stateStrings = new String[states.size()];
        for (int i = 0; i < states.size(); i++) {
            stateStrings[i] = states.get(i).getBullMqName();
        }

        List<Job<T>> result = new java.util.ArrayList<>();
        try {
            List<Object> rawResults = bullScripts.getJobs(connectionManager.getBinaryConnection(), queueKeys, start, end, asc, 1000, stateStrings);
            if (rawResults != null && !rawResults.isEmpty()) {
                for (int sIdx = 0; sIdx < rawResults.size(); sIdx++) {
                    JobState currentState = sIdx < states.size() ? states.get(sIdx) : null;
                    Object stateObj = rawResults.get(sIdx);
                    if (stateObj instanceof List<?> jobList) {
                        for (Object jobObj : jobList) {
                            if (jobObj instanceof List<?> jobTuple && jobTuple.size() >= 2) {
                                String jobId = toStr(jobTuple.get(0));
                                Map<String, String> fields = new java.util.HashMap<>();
                                Object fieldsObj = jobTuple.get(1);
                                if (fieldsObj instanceof List<?> rawFields) {
                                    for (int f = 0; f < rawFields.size() - 1; f += 2) {
                                        String k = toStr(rawFields.get(f));
                                        String v = toStr(rawFields.get(f + 1));
                                        if (k != null) {
                                            fields.put(k, v);
                                        }
                                    }
                                }
                                if (fields.isEmpty() && connectionManager != null) {
                                    fields = connectionManager.getCommandConnection().sync().hgetall(prefix + ":" + jobId);
                                }
                                Job<T> job = buildJobFromFields(jobId, fields, currentState);
                                if (job != null) {
                                    result.add(job);
                                }
                            }
                        }
                    }
                }
                return result;
            }
        } catch (Exception e) {
            log.debug("Optimized getJobs script returned error, falling back to ranges: {}", e.getMessage());
        }

        // Fallback via getRanges
        Map<JobState, List<String>> ranges = getRanges(states.toArray(new JobState[0]), start, end, asc);
        if (ranges != null) {
            for (JobState s : states) {
                List<String> ids = ranges.get(s);
                if (ids != null) {
                    for (String id : ids) {
                        Job<T> j = getJob(id);
                        if (j != null) {
                            j.setState(s);
                            result.add(j);
                        }
                    }
                }
            }
        }
        return result;
    }

    @Override
    public Map<JobState, Long> getJobCounts(JobState... states) {
        if (states == null || states.length == 0) {
            Map<String, Long> rawCounts = getJobCounts();
            Map<JobState, Long> counts = new java.util.EnumMap<>(JobState.class);
            for (Map.Entry<String, Long> entry : rawCounts.entrySet()) {
                JobState s = JobState.fromBullMqState(entry.getKey());
                if (s != null) {
                    counts.put(s, entry.getValue());
                }
            }
            return counts;
        }

        String[] typeNames = new String[states.length];
        for (int i = 0; i < states.length; i++) {
            typeNames[i] = states[i].getBullMqName();
        }
        List<Long> countsList = bullScripts.getCounts(connectionManager.getBinaryConnection(), queueKeys, typeNames);
        Map<JobState, Long> result = new java.util.EnumMap<>(JobState.class);
        for (int i = 0; i < states.length; i++) {
            result.put(states[i], i < countsList.size() && countsList.get(i) != null ? countsList.get(i) : 0L);
        }
        return result;
    }

    @Override
    public Long getRateLimitTtl() {
        return bullScripts.getRateLimitTtl(connectionManager.getBinaryConnection(), queueKeys, 0);
    }

    @Override
    public void setGlobalConcurrency(int concurrency) {
        connectionManager.getCommandConnection().sync().hset(queueKeys.getKey("meta"), "concurrency", String.valueOf(concurrency));
    }

    @Override
    public Integer getGlobalConcurrency() {
        String val = connectionManager.getCommandConnection().sync().hget(queueKeys.getKey("meta"), "concurrency");
        return val != null ? Integer.parseInt(val) : null;
    }

    @Override
    public boolean removeGlobalConcurrency() {
        Long removed = connectionManager.getCommandConnection().sync().hdel(queueKeys.getKey("meta"), "concurrency");
        return removed != null && removed > 0;
    }

    @Override
    public void setGlobalRateLimit(int max, long durationMillis) {
        Map<String, String> fields = Map.of(
            "max", String.valueOf(max),
            "duration", String.valueOf(durationMillis)
        );
        connectionManager.getCommandConnection().sync().hset(queueKeys.getKey("meta"), fields);
    }

    @Override
    public io.oxmq.model.GlobalRateLimit getGlobalRateLimit() {
        var sync = connectionManager.getCommandConnection().sync();
        String maxStr = sync.hget(queueKeys.getKey("meta"), "max");
        String durStr = sync.hget(queueKeys.getKey("meta"), "duration");
        if (maxStr != null && durStr != null) {
            try {
                return new io.oxmq.model.GlobalRateLimit(Integer.parseInt(maxStr), Long.parseLong(durStr));
            } catch (NumberFormatException ignored) {}
        }
        return null;
    }

    @Override
    public boolean removeGlobalRateLimit() {
        Long removed = connectionManager.getCommandConnection().sync().hdel(queueKeys.getKey("meta"), "max", "duration");
        return removed != null && removed > 0;
    }

    @Override
    public boolean removeRateLimitKey() {
        Long removed = connectionManager.getCommandConnection().sync().del(queueKeys.getKey("limiter"));
        return removed != null && removed > 0;
    }

    @Override
    public boolean pauseGroup(String groupId) {
        return extensionScripts.pauseGroup(connectionManager.getBinaryConnection(), queueKeys, groupId);
    }

    @Override
    public boolean resumeGroup(String groupId) {
        return extensionScripts.resumeGroup(connectionManager.getBinaryConnection(), queueKeys, groupId);
    }

    @Override
    public long deleteGroup(String groupId) {
        return extensionScripts.deleteGroup(connectionManager.getBinaryConnection(), queueKeys, groupId);
    }

    @Override
    public boolean setGroupRateLimit(String groupId, java.time.Duration duration) {
        return setGroupRateLimit(groupId, 1, duration);
    }

    @Override
    public boolean setGroupRateLimit(String groupId, int maxJobs, java.time.Duration duration) {
        return setGroupRateLimit(groupId, maxJobs, duration != null ? duration.toMillis() : 0);
    }

    @Override
    public boolean setGroupRateLimit(String groupId, int maxJobs, long durationMs) {
        return extensionScripts.setGroupRateLimit(connectionManager.getBinaryConnection(), queueKeys, groupId,
                maxJobs, durationMs);
    }

    @Override
    public long getGroupRateLimitTtl(String groupId) {
        return getGroupRateLimitTtl(groupId, 1);
    }

    @Override
    public long getGroupRateLimitTtl(String groupId, int maxJobs) {
        if (groupId == null || groupId.isBlank()) return 0;
        try {
            var sync = connectionManager.getCommandConnection().sync();
            String groupsLimitedKey = queueKeys.toKey("groups:limited");
            Double score = sync.zscore(groupsLimitedKey, groupId);
            if (score == null) {
                String groupLimitKey = queueKeys.toKey("groups:" + groupId + ":limit");
                Long pttl = sync.pttl(groupLimitKey);
                if (pttl != null && pttl > 0) {
                    if (maxJobs <= 1) return pttl;
                    String val = sync.get(groupLimitKey);
                    int current = val != null ? Integer.parseInt(val) : 0;
                    if (current >= maxJobs) return pttl;
                }
                return 0;
            }
            long remaining = (long) (score - System.currentTimeMillis());
            return Math.max(0, remaining);
        } catch (Exception e) {
            log.warn("Failed to get rate limit TTL for group {} in queue {}: {}", groupId, name, e.getMessage());
            return 0;
        }
    }

    @Override
    public List<String> getGroups(long start, long stop) {
        try {
            var sync = connectionManager.getCommandConnection().sync();
            LinkedHashSet<String> allGroups = new LinkedHashSet<>();
            List<String> waiting = sync.lrange(queueKeys.toKey("groups:waiting"), 0, -1);
            if (waiting != null) allGroups.addAll(waiting);
            Set<String> maxed = sync.smembers(queueKeys.toKey("groups:maxed"));
            if (maxed != null) allGroups.addAll(maxed);
            List<String> limited = sync.zrange(queueKeys.toKey("groups:limited"), 0, -1);
            if (limited != null) allGroups.addAll(limited);
            Set<String> paused = sync.smembers(queueKeys.toKey("groups:paused"));
            if (paused != null) allGroups.addAll(paused);

            List<String> list = new ArrayList<>(allGroups);
            int fromIndex = Math.min((int) Math.max(0, start), list.size());
            int toIndex = stop < 0 ? list.size() : Math.min((int) (stop + 1), list.size());
            if (fromIndex >= toIndex) return Collections.emptyList();
            return list.subList(fromIndex, toIndex);
        } catch (Exception e) {
            log.warn("Failed to retrieve groups for queue {}: {}", name, e.getMessage());
            return Collections.emptyList();
        }
    }

    @Override
    public Map<String, Long> getGroupsJobsCount(int limit) {
        try {
            int max = limit > 0 ? limit : 1000;
            List<String> groups = getGroups(0, max - 1);
            if (groups.isEmpty()) return Collections.emptyMap();
            var sync = connectionManager.getCommandConnection().sync();
            Map<String, Long> counts = new LinkedHashMap<>(groups.size());
            for (String gid : groups) {
                Long waitLen = sync.llen(queueKeys.toKey("groups:" + gid));
                Long prioLen = sync.zcard(queueKeys.toKey("groups:" + gid + ":prioritized"));
                long total = (waitLen != null ? waitLen : 0) + (prioLen != null ? prioLen : 0);
                counts.put(gid, total);
            }
            return counts;
        } catch (Exception e) {
            log.warn("Failed to get groups jobs count for queue {}: {}", name, e.getMessage());
            return Collections.emptyMap();
        }
    }

    @Override
    public Map<String, Long> getGroupsJobsCount() {
        return getGroupsJobsCount(1000);
    }

    @Override
    public boolean repairGroup(String groupId) {
        if (groupId == null || groupId.isBlank()) return false;
        int repaired = extensionScripts.repairGroup(connectionManager.getBinaryConnection(), queueKeys, groupId, 1);
        return repaired > 0;
    }

    @Override
    public int repairMaxedGroups() {
        return extensionScripts.repairGroup(connectionManager.getBinaryConnection(), queueKeys, "", 1);
    }

    @Override
    public List<io.oxmq.lua.ExtensionScripts.GroupEntry> getGroupsByStatus(String status, long start, long stop) {
        return extensionScripts.getGroupsByStatus(connectionManager.getBinaryConnection(), queueKeys, status, start, stop);
    }

    @Override
    public List<Job<T>> getGroupJobs(String groupId, long start, long stop) {
        List<String> jobIds = extensionScripts.getGroupJobs(connectionManager.getBinaryConnection(), queueKeys, groupId, start, stop);
        if (jobIds == null || jobIds.isEmpty()) {
            return Collections.emptyList();
        }
        List<Job<T>> result = new ArrayList<>(jobIds.size());
        for (String id : jobIds) {
            Job<T> job = getJob(id);
            if (job != null) {
                result.add(job);
            }
        }
        return result;
    }

    @Override
    public long getGroupJobsCount(String groupId) {
        if (groupId == null || groupId.isBlank()) return 0;
        var sync = connectionManager.getCommandConnection().sync();
        Long waitLen = sync.llen(prefix + ":groups:" + groupId);
        Long prioLen = sync.zcard(prefix + ":groups:" + groupId + ":prioritized");
        return (waitLen != null ? waitLen : 0) + (prioLen != null ? prioLen : 0);
    }

    @Override
    public long getGroupActiveCount(String groupId) {
        if (groupId == null || groupId.isBlank()) return 0;
        var sync = connectionManager.getCommandConnection().sync();
        String val = sync.hget(prefix + ":groups:active:count", groupId);
        if (val == null || val.isBlank()) return 0;
        try {
            return Long.parseLong(val);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    @Override
    public Map<Integer, Long> getCountsPerPriorityForGroup(String groupId, List<Integer> priorities) {
        if (groupId == null || groupId.isBlank() || priorities == null || priorities.isEmpty()) {
            return Collections.emptyMap();
        }
        var sync = connectionManager.getCommandConnection().sync();
        Map<Integer, Long> counts = new HashMap<>();
        String prioKey = prefix + ":groups:" + groupId + ":prioritized";
        String waitKey = prefix + ":groups:" + groupId;
        for (Integer p : priorities) {
            if (p == null) continue;
            if (p == 0) {
                Long len = sync.llen(waitKey);
                counts.put(0, len != null ? len : 0L);
            } else {
                Long count = sync.zcount(prioKey, io.lettuce.core.Range.create(p.doubleValue(), p.doubleValue()));
                counts.put(p, count != null ? count : 0L);
            }
        }
        return counts;
    }

    @Override
    public void setGroupConcurrency(String groupId, int concurrency) {
        if (groupId == null || groupId.isBlank()) return;
        var sync = connectionManager.getCommandConnection().sync();
        String key = queueKeys.toKey("groups:concurrency");
        if (concurrency > 0) {
            sync.hset(key, groupId, String.valueOf(concurrency));
        } else {
            sync.hdel(key, groupId);
        }
    }

    @Override
    public Integer getGroupConcurrency(String groupId) {
        if (groupId == null || groupId.isBlank()) return null;
        var sync = connectionManager.getCommandConnection().sync();
        String val = sync.hget(queueKeys.toKey("groups:concurrency"), groupId);
        if (val == null || val.isBlank()) return null;
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Override
    public boolean removeGroupConcurrency(String groupId) {
        if (groupId == null || groupId.isBlank()) return false;
        var sync = connectionManager.getCommandConnection().sync();
        Long del = sync.hdel(queueKeys.toKey("groups:concurrency"), groupId);
        return del != null && del > 0;
    }

    @Override
    public boolean cancelJob(String jobId, String reason) {
        if (jobId == null || jobId.isBlank()) return false;
        String resolvedReason = reason != null ? reason : "User requested cancellation";

        JobState state = getJobState(jobId);
        if (state == JobState.COMPLETED || state == JobState.FAILED) {
            return false;
        }

        if (state == JobState.WAITING || state == JobState.DELAYED || state == JobState.PRIORITIZED) {
            try {
                remove(jobId);
            } catch (Exception ignored) {
            }
        }

        var sync = connectionManager.getCommandConnection().sync();
        String controlChannel = queueKeys.toKey("control");
        String message = "CANCEL:" + jobId + ":" + resolvedReason;
        sync.publish(controlChannel, message);
        log.info("Dispatched cluster-wide cancellation for job [id: {}] on queue {}: {}", jobId, name, resolvedReason);
        return true;
    }

    private static String toStr(Object obj) {
        if (obj == null) return null;
        if (obj instanceof byte[] b) return new String(b, StandardCharsets.UTF_8);
        return obj.toString();
    }

    @Override
    public void close() {
        if (internalQueueEvents != null) {
            try {
                internalQueueEvents.close();
            } catch (Exception ignored) {}
        }
        backend.close();
    }

    public static class Builder<T> {
        private String name;
        private String prefix;
        private RedisConnectionManager connectionManager;
        private RedisClient redisClient;
        private String redisUri;
        private LuaScriptManager scriptManager;
        private JobSerializer serializer;
        private OxmqMetrics metrics;
        private Class<T> payloadClass;
        private io.oxmq.backend.QueueBackend backend;

        public Builder<T> name(String name) {
            this.name = name;
            return this;
        }

        public Builder<T> prefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder<T> redisUri(String redisUri) {
            this.redisUri = redisUri;
            return this;
        }

        public Builder<T> redisClient(RedisClient redisClient) {
            this.redisClient = redisClient;
            return this;
        }

        public Builder<T> connectionManager(RedisConnectionManager connectionManager) {
            this.connectionManager = connectionManager;
            return this;
        }

        public Builder<T> backend(io.oxmq.backend.QueueBackend backend) {
            this.backend = backend;
            return this;
        }

        public Builder<T> scriptManager(LuaScriptManager scriptManager) {
            this.scriptManager = scriptManager;
            return this;
        }

        public Builder<T> serializer(JobSerializer serializer) {
            this.serializer = serializer;
            return this;
        }

        public Builder<T> metrics(OxmqMetrics metrics) {
            this.metrics = metrics;
            return this;
        }

        private JobOptions defaultJobOptions;
        private int maxLenEvents = -1;

        public Builder<T> defaultJobOptions(JobOptions defaultJobOptions) {
            this.defaultJobOptions = defaultJobOptions;
            return this;
        }

        public Builder<T> maxLenEvents(int maxLenEvents) {
            this.maxLenEvents = maxLenEvents;
            return this;
        }

        public Builder<T> payloadClass(Class<T> payloadClass) {
            this.payloadClass = payloadClass;
            return this;
        }

        public OxmqQueue<T> build() {
            boolean owns = false;
            if (backend == null && connectionManager == null) {
                owns = true;
                if (redisClient != null) {
                    connectionManager = new RedisConnectionManager(redisClient);
                } else if (redisUri != null) {
                    connectionManager = new RedisConnectionManager(redisUri);
                } else {
                    connectionManager = new RedisConnectionManager("redis://localhost:6379");
                }
            }
            if (backend == null) {
                QueueKeys keys = new QueueKeys(prefix, name);
                backend = new io.oxmq.backend.RedisQueueBackend(keys, connectionManager, scriptManager, null, owns);
            }
            return new OxmqQueue<>(prefix, name, connectionManager, scriptManager, serializer, metrics, payloadClass, backend, defaultJobOptions, maxLenEvents);
        }
    }
}
