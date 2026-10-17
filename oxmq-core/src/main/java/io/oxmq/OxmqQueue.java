package io.oxmq;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.oxmq.client.QueueKeys;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.lua.BullScripts;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    private final String name;
    private final String prefix;
    private final QueueKeys queueKeys;
    private final RedisConnectionManager connectionManager;
    private final LuaScriptManager scriptManager;
    private final BullScripts bullScripts;
    private final JobSerializer serializer;
    private final OxmqMetrics metrics;
    private final Class<T> payloadClass;
    private final io.oxmq.backend.QueueBackend backend;

    public OxmqQueue(String prefix, String name, RedisConnectionManager connectionManager, LuaScriptManager scriptManager,
                     JobSerializer serializer, OxmqMetrics metrics, Class<T> payloadClass, io.oxmq.backend.QueueBackend backend) {
        this.name = Objects.requireNonNull(name, "Queue name must not be null");
        this.queueKeys = new QueueKeys(prefix, name);
        this.prefix = this.queueKeys.getQualifiedName();
        this.connectionManager = connectionManager;
        this.scriptManager = scriptManager != null ? scriptManager : new LuaScriptManager();
        this.bullScripts = new BullScripts(this.scriptManager);
        this.serializer = serializer != null ? serializer : new JacksonJobSerializer();
        this.metrics = metrics != null ? metrics : new OxmqMetrics();
        this.payloadClass = payloadClass;
        this.backend = backend != null ? backend : new io.oxmq.backend.RedisQueueBackend(this.queueKeys, this.connectionManager, this.scriptManager, this.bullScripts);

        registerGauges();
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
        metrics.registerWaitingGauge(name, () -> count(JobState.WAITING));
        metrics.registerActiveGauge(name, () -> count(JobState.ACTIVE));
        metrics.registerDelayedGauge(name, () -> count(JobState.DELAYED));
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

    @Override
    public Job<T> add(String jobName, T data) {
        return add(jobName, data, JobOptions.defaults());
    }

    @Override
    public Job<T> add(String jobName, T data, JobOptions opts) {
        Objects.requireNonNull(jobName, "jobName must not be null");
        JobOptions options = opts != null ? opts : JobOptions.defaults();

        String serializedData = serializer.serialize(data);
        long now = System.currentTimeMillis();
        long delayMs = options.getDelayMs();
        String customJobId = options.getJobId() != null ? options.getJobId() : "";

        Map<String, Object> optsMap = options.toMap();

        StatefulRedisConnection<String, byte[]> binConn = connectionManager.getBinaryConnection();
        String jobId;
        if (delayMs > 0) {
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
        return job;
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

        T data = payloadClass != null ? serializer.deserialize(dataStr, payloadClass) : (T) dataStr;
        JobOptions opts = serializer.deserialize(optsStr, JobOptions.class);

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

        String stateStr = getState(jobId);
        job.setState(JobState.fromBullMqState(stateStr));

        try {
            Map<String, String> childValsRaw = conn.sync().hgetall(prefix + ":" + jobId + ":processed");
            if (childValsRaw == null || childValsRaw.isEmpty()) {
                childValsRaw = conn.sync().hgetall(prefix + ":" + jobId + ":childrenValues");
            }
            if (childValsRaw != null && !childValsRaw.isEmpty()) {
                Map<String, Object> childVals = new java.util.HashMap<>();
                for (Map.Entry<String, String> entry : childValsRaw.entrySet()) {
                    try {
                        childVals.put(entry.getKey(), serializer.deserialize(entry.getValue(), Object.class));
                    } catch (Exception ex) {
                        childVals.put(entry.getKey(), entry.getValue());
                    }
                }
                job.setChildrenValues(childVals);
            }
        } catch (Exception ignored) {}

        return job;
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
        List<Job<T>> createdJobs = new java.util.ArrayList<>(jobs.size());
        for (io.oxmq.model.JobRequest<T> req : jobs) {
            createdJobs.add(add(req.name(), req.data(), req.options()));
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
        String[] types = {"wait", "active", "delayed", "completed", "failed", "paused", "waiting-children", "prioritized"};
        List<Long> counts = bullScripts.getCounts(connectionManager.getBinaryConnection(), queueKeys, types);
        java.util.Map<String, Long> map = new java.util.LinkedHashMap<>();
        for (int i = 0; i < types.length && i < counts.size(); i++) {
            map.put(types[i], counts.get(i));
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
    public boolean removeDeduplicationKey(String deduplicationId) {
        if (deduplicationId == null || deduplicationId.isBlank()) {
            return false;
        }
        RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
        Long del = sync.del(queueKeys.toKey("de:" + deduplicationId));
        return del != null && del > 0;
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
        long nextMillis = System.currentTimeMillis();
        String serializedData = serializer.serialize(data);
        JobOptions options = opts != null ? opts : JobOptions.defaults();
        return backend.addJobScheduler(schedulerId, nextMillis, jobName, serializedData, options.toMap(), null, cronPattern, null);
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
            cursor = backend.moveJobsToWait(stateStr, batchCount, ts);
            iterations++;
        } while (cursor != null && cursor == 1L);
        return iterations;
    }

    @Override
    public long promoteJobs(int count) {
        return backend.promoteJobs(count);
    }

    @Override
    public void trimEvents(long maxLength) {
        backend.trimEvents(maxLength);
    }

    @Override
    public Map<Integer, Long> getCountsPerPriority(int... priorities) {
        if (priorities == null || priorities.length == 0) {
            return Map.of();
        }
        List<Long> counts = backend.getCountsPerPriority(priorities);
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
        List<List<String>> raw = backend.getRanges(start, end, asc, stateNames);
        Map<JobState, List<String>> result = new java.util.LinkedHashMap<>();
        for (int i = 0; i < states.length; i++) {
            result.put(states[i], i < raw.size() ? raw.get(i) : List.of());
        }
        return result;
    }

    @Override
    public io.oxmq.model.JobDependencyCounts getDependencyCounts(String jobId) {
        return backend.getDependencyCounts(jobId);
    }

    @Override
    public boolean removeChildDependency(String jobId, String parentKey) {
        return backend.removeChildDependency(jobId, parentKey);
    }

    @Override
    public void removeUnprocessedChildren(String jobId) {
        backend.removeUnprocessedChildren(jobId);
    }

    @Override
    public Map<String, Object> getJobScheduler(String schedulerId) {
        List<Object> raw = backend.getJobScheduler(schedulerId);
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
        return backend.getJobSchedulersCount();
    }

    @Override
    public boolean isMaxed() {
        return backend.isMaxed();
    }

    @Override
    public long removeOrphanedJobs(List<String> candidateJobIds) {
        Long removed = backend.removeOrphanedJobs(candidateJobIds);
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
        return backend.isFinished(jobId, fetchValue);
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

        public Builder<T> payloadClass(Class<T> payloadClass) {
            this.payloadClass = payloadClass;
            return this;
        }

        public OxmqQueue<T> build() {
            if (backend == null && connectionManager == null) {
                if (redisClient != null) {
                    connectionManager = new RedisConnectionManager(redisClient);
                } else if (redisUri != null) {
                    connectionManager = new RedisConnectionManager(redisUri);
                } else {
                    connectionManager = new RedisConnectionManager("redis://localhost:6379");
                }
            }
            return new OxmqQueue<>(prefix, name, connectionManager, scriptManager, serializer, metrics, payloadClass, backend);
        }
    }
}
