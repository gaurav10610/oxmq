package io.oxmq;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.lua.LuaScript;
import io.oxmq.lua.LuaScriptManager;
import io.oxmq.metrics.OxmqMetrics;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import io.oxmq.serializer.JacksonJobSerializer;
import io.oxmq.serializer.JobSerializer;
import java.util.Map;
import java.util.Objects;
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
    private final RedisConnectionManager connectionManager;
    private final LuaScriptManager scriptManager;
    private final JobSerializer serializer;
    private final OxmqMetrics metrics;
    private final Class<T> payloadClass;

    public OxmqQueue(String name, RedisConnectionManager connectionManager, LuaScriptManager scriptManager,
                     JobSerializer serializer, OxmqMetrics metrics, Class<T> payloadClass) {
        this.name = Objects.requireNonNull(name, "Queue name must not be null");
        this.prefix = "bull:" + name;
        this.connectionManager = Objects.requireNonNull(connectionManager, "connectionManager must not be null");
        this.scriptManager = scriptManager != null ? scriptManager : new LuaScriptManager();
        this.serializer = serializer != null ? serializer : new JacksonJobSerializer();
        this.metrics = metrics != null ? metrics : new OxmqMetrics();
        this.payloadClass = payloadClass;

        registerGauges();
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

    @Override
    public Job<T> add(String jobName, T data) {
        return add(jobName, data, JobOptions.defaults());
    }

    @Override
    public Job<T> add(String jobName, T data, JobOptions opts) {
        Objects.requireNonNull(jobName, "jobName must not be null");
        JobOptions options = opts != null ? opts : JobOptions.defaults();

        String serializedData = serializer.serialize(data);
        String serializedOpts = serializer.serialize(options);
        long now = System.currentTimeMillis();
        long delayMs = options.getDelayMs();
        String customJobId = options.getJobId() != null ? options.getJobId() : "";
        String parentKey = options.getParentKey() != null ? options.getParentKey() : "";

        StatefulRedisConnection<String, String> conn = connectionManager.getCommandConnection();

        String[] keys = new String[]{
                prefix + ":wait",
                prefix + ":delayed",
                prefix + ":id",
                prefix + ":events",
                prefix + ":meta"
        };

        String jobId = scriptManager.eval(conn, LuaScript.ADD_JOB, ScriptOutputType.VALUE, keys,
                prefix,
                customJobId,
                jobName,
                serializedData != null ? serializedData : "{}",
                serializedOpts != null ? serializedOpts : "{}",
                String.valueOf(now),
                String.valueOf(delayMs),
                parentKey
        );

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

        try {
            Map<String, String> childValsRaw = conn.sync().hgetall(prefix + ":" + jobId + ":childrenValues");
            if (childValsRaw != null && !childValsRaw.isEmpty()) {
                Map<String, Object> childVals = new java.util.HashMap<>();
                for (Map.Entry<String, String> entry : childValsRaw.entrySet()) {
                    try {
                        childVals.put(entry.getKey(), serializer.deserialize(entry.getValue(), Map.class));
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
        StatefulRedisConnection<String, String> conn = connectionManager.getCommandConnection();
        scriptManager.eval(conn, LuaScript.PAUSE_QUEUE, ScriptOutputType.INTEGER,
                new String[]{prefix + ":meta", prefix + ":events"},
                "pause"
        );
        log.info("Paused queue {}", name);
    }

    @Override
    public void resume() {
        StatefulRedisConnection<String, String> conn = connectionManager.getCommandConnection();
        scriptManager.eval(conn, LuaScript.PAUSE_QUEUE, ScriptOutputType.INTEGER,
                new String[]{prefix + ":meta", prefix + ":events"},
                "resume"
        );
        log.info("Resumed queue {}", name);
    }

    @Override
    public boolean isPaused() {
        RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
        String paused = sync.hget(prefix + ":meta", "paused");
        return "1".equals(paused) || "true".equalsIgnoreCase(paused);
    }

    @Override
    public long count(JobState state) {
        RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
        try {
            return switch (state) {
                case WAITING -> sync.llen(prefix + ":wait");
                case ACTIVE -> sync.llen(prefix + ":active");
                case DELAYED -> sync.zcard(prefix + ":delayed");
                case COMPLETED -> sync.zcard(prefix + ":completed");
                case FAILED -> sync.zcard(prefix + ":failed");
                default -> 0L;
            };
        } catch (Exception e) {
            return 0L;
        }
    }

    @Override
    public long clean(long graceMs, int limit, JobState state) {
        String targetKey = switch (state) {
            case COMPLETED -> prefix + ":completed";
            case FAILED -> prefix + ":failed";
            default -> throw new IllegalArgumentException("Cannot clean state: " + state);
        };
        long cutoff = System.currentTimeMillis() - graceMs;
        Long cleaned = scriptManager.eval(connectionManager.getCommandConnection(), LuaScript.CLEAN_QUEUE,
                ScriptOutputType.INTEGER,
                new String[]{targetKey},
                prefix,
                String.valueOf(cutoff),
                String.valueOf(limit)
        );
        return cleaned != null ? cleaned : 0L;
    }

    @Override
    public void obliterate() {
        StatefulRedisConnection<String, String> conn = connectionManager.getCommandConnection();
        String[] keys = new String[]{
                prefix + ":wait",
                prefix + ":active",
                prefix + ":delayed",
                prefix + ":completed",
                prefix + ":failed",
                prefix + ":stalled",
                prefix + ":meta",
                prefix + ":limiter"
        };
        scriptManager.eval(conn, LuaScript.OBLITERATE, ScriptOutputType.INTEGER, keys, prefix);
        log.info("Obliterated queue {}", name);
    }

    @Override
    public void close() {
        connectionManager.close();
    }

    public static class Builder<T> {
        private String name;
        private RedisConnectionManager connectionManager;
        private RedisClient redisClient;
        private String redisUri;
        private LuaScriptManager scriptManager;
        private JobSerializer serializer;
        private OxmqMetrics metrics;
        private Class<T> payloadClass;

        public Builder<T> name(String name) {
            this.name = name;
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
            if (connectionManager == null) {
                if (redisClient != null) {
                    connectionManager = new RedisConnectionManager(redisClient);
                } else if (redisUri != null) {
                    connectionManager = new RedisConnectionManager(redisUri);
                } else {
                    connectionManager = new RedisConnectionManager("redis://localhost:6379");
                }
            }
            return new OxmqQueue<>(name, connectionManager, scriptManager, serializer, metrics, payloadClass);
        }
    }
}
