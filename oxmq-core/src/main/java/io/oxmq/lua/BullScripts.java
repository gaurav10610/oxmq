package io.oxmq.lua;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.oxmq.client.QueueKeys;
import io.oxmq.serializer.BullMsgPack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Strongly-typed caller for BullMQ Lua scripts executing on Redis.
 */
public class BullScripts {

    private static final Logger log = LoggerFactory.getLogger(BullScripts.class);

    private final LuaScriptManager scriptManager;

    public BullScripts(LuaScriptManager scriptManager) {
        this.scriptManager = Objects.requireNonNull(scriptManager, "scriptManager must not be null");
    }

    public BullScripts() {
        this(new LuaScriptManager());
    }

    public String addStandardJob(StatefulRedisConnection<String, byte[]> connection,
                                 QueueKeys keys, String customId, String name,
                                 String jsonData, Map<String, Object> opts, long timestamp) {
        String[] redisKeys = new String[] {
            keys.getKey("wait"),
            keys.getKey("paused"),
            keys.getKey("meta"),
            keys.getKey("id"),
            keys.getKey("completed"),
            keys.getKey("delayed"),
            keys.getKey("active"),
            keys.getKey("events"),
            keys.getKey("marker")
        };

        String parentKey = (String) opts.get("parentKey");
        @SuppressWarnings("unchecked")
        Map<String, Object> parent = (Map<String, Object>) opts.get("parent");
        String repeatJobKey = (String) opts.get("repeatJobKey");
        String deduplicationId = (String) opts.get("deduplicationId");

        byte[] packedArgs = BullMsgPack.buildAddJobArgs(
            keys.toKey(""), customId, name, timestamp,
            parentKey, parent, repeatJobKey, deduplicationId
        );
        byte[] packedOpts = BullMsgPack.pack(BullMsgPack.encodeOpts(opts));
        byte[] dataBytes = jsonData != null ? jsonData.getBytes(StandardCharsets.UTF_8) : "{}".getBytes(StandardCharsets.UTF_8);
        byte[] tsBytes = String.valueOf(timestamp).getBytes(StandardCharsets.UTF_8);

        Object result = scriptManager.evalBinary(connection, LuaScript.ADD_STANDARD_JOB,
            ScriptOutputType.VALUE, redisKeys, packedArgs, dataBytes, packedOpts, tsBytes);

        return parseJobIdResult(result, "addStandardJob");
    }

    public String addDelayedJob(StatefulRedisConnection<String, byte[]> connection,
                               QueueKeys keys, String customId, String name,
                               String jsonData, Map<String, Object> opts, long timestamp, long delay) {
        String[] redisKeys = new String[] {
            keys.getKey("marker"),
            keys.getKey("meta"),
            keys.getKey("id"),
            keys.getKey("delayed"),
            keys.getKey("completed"),
            keys.getKey("events")
        };

        String parentKey = (String) opts.get("parentKey");
        @SuppressWarnings("unchecked")
        Map<String, Object> parent = (Map<String, Object>) opts.get("parent");
        String repeatJobKey = (String) opts.get("repeatJobKey");
        String deduplicationId = (String) opts.get("deduplicationId");

        byte[] packedArgs = BullMsgPack.buildAddJobArgs(
            keys.toKey(""), customId, name, timestamp,
            parentKey, parent, repeatJobKey, deduplicationId
        );
        byte[] packedOpts = BullMsgPack.pack(BullMsgPack.encodeOpts(opts));
        byte[] dataBytes = jsonData != null ? jsonData.getBytes(StandardCharsets.UTF_8) : "{}".getBytes(StandardCharsets.UTF_8);
        byte[] tsBytes = String.valueOf(timestamp).getBytes(StandardCharsets.UTF_8);

        Object result = scriptManager.evalBinary(connection, LuaScript.ADD_DELAYED_JOB,
            ScriptOutputType.VALUE, redisKeys, packedArgs, dataBytes, packedOpts, tsBytes);

        return parseJobIdResult(result, "addDelayedJob");
    }

    public String addPrioritizedJob(StatefulRedisConnection<String, byte[]> connection,
                                    QueueKeys keys, String customId, String name,
                                    String jsonData, Map<String, Object> opts, long timestamp, int priority) {
        String[] redisKeys = new String[] {
            keys.getKey("marker"),
            keys.getKey("meta"),
            keys.getKey("id"),
            keys.getKey("prioritized"),
            keys.getKey("delayed"),
            keys.getKey("completed"),
            keys.getKey("active"),
            keys.getKey("events"),
            keys.getKey("pc")
        };

        String parentKey = (String) opts.get("parentKey");
        @SuppressWarnings("unchecked")
        Map<String, Object> parent = (Map<String, Object>) opts.get("parent");
        String repeatJobKey = (String) opts.get("repeatJobKey");
        String deduplicationId = (String) opts.get("deduplicationId");

        byte[] packedArgs = BullMsgPack.buildAddJobArgs(
            keys.toKey(""), customId, name, timestamp,
            parentKey, parent, repeatJobKey, deduplicationId
        );
        byte[] packedOpts = BullMsgPack.pack(BullMsgPack.encodeOpts(opts));
        byte[] dataBytes = jsonData != null ? jsonData.getBytes(StandardCharsets.UTF_8) : "{}".getBytes(StandardCharsets.UTF_8);
        byte[] tsBytes = String.valueOf(timestamp).getBytes(StandardCharsets.UTF_8);

        Object result = scriptManager.evalBinary(connection, LuaScript.ADD_PRIORITIZED_JOB,
            ScriptOutputType.VALUE, redisKeys, packedArgs, dataBytes, packedOpts, tsBytes);

        return parseJobIdResult(result, "addPrioritizedJob");
    }

    public String addParentJob(StatefulRedisConnection<String, byte[]> connection,
                               QueueKeys keys, String customId, String name,
                               String jsonData, Map<String, Object> opts, long timestamp) {
        String[] redisKeys = new String[] {
            keys.getKey("meta"),
            keys.getKey("id"),
            keys.getKey("delayed"),
            keys.getKey("waiting-children"),
            keys.getKey("completed"),
            keys.getKey("events")
        };

        String parentKey = (String) opts.get("parentKey");
        @SuppressWarnings("unchecked")
        Map<String, Object> parent = (Map<String, Object>) opts.get("parent");
        String repeatJobKey = (String) opts.get("repeatJobKey");
        String deduplicationId = (String) opts.get("deduplicationId");

        byte[] packedArgs = BullMsgPack.buildAddJobArgs(
            keys.toKey(""), customId, name, timestamp,
            parentKey, parent, repeatJobKey, deduplicationId
        );
        byte[] packedOpts = BullMsgPack.pack(BullMsgPack.encodeOpts(opts));
        byte[] dataBytes = jsonData != null ? jsonData.getBytes(StandardCharsets.UTF_8) : "{}".getBytes(StandardCharsets.UTF_8);

        Object result = scriptManager.evalBinary(connection, LuaScript.ADD_PARENT_JOB,
            ScriptOutputType.VALUE, redisKeys, packedArgs, dataBytes, packedOpts);

        return parseJobIdResult(result, "addParentJob");
    }

    public List<Object> moveToActive(StatefulRedisConnection<String, byte[]> connection,
                                     QueueKeys keys, String token, int lockDurationMs,
                                     Map<String, Object> limiter, String workerName, long timestamp) {
        String[] redisKeys = new String[] {
            keys.getKey("wait"),
            keys.getKey("active"),
            keys.getKey("prioritized"),
            keys.getKey("events"),
            keys.getKey("stalled"),
            keys.getKey("limiter"),
            keys.getKey("delayed"),
            keys.getKey("paused"),
            keys.getKey("meta"),
            keys.getKey("pc"),
            keys.getKey("marker")
        };

        Map<String, Object> workerOpts = new HashMap<>();
        workerOpts.put("token", token != null ? token : "0");
        workerOpts.put("lockDuration", lockDurationMs);
        if (limiter != null && !limiter.isEmpty()) {
            workerOpts.put("limiter", limiter);
        }
        if (workerName != null && !workerName.isBlank()) {
            workerOpts.put("name", workerName);
        }

        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] tsBytes = String.valueOf(timestamp).getBytes(StandardCharsets.UTF_8);
        byte[] optsBytes = BullMsgPack.pack(workerOpts);

        return scriptManager.evalBinary(connection, LuaScript.MOVE_TO_ACTIVE,
            ScriptOutputType.MULTI, redisKeys, prefixBytes, tsBytes, optsBytes);
    }

    public Object moveToFinished(StatefulRedisConnection<String, byte[]> connection,
                                 QueueKeys keys, String jobId, String value,
                                 String propVal, String target, String token,
                                 boolean fetchNext, Map<String, Object> jobOpts) {
        String[] redisKeys = new String[] {
            keys.getKey("wait"),
            keys.getKey("active"),
            keys.getKey("prioritized"),
            keys.getKey("events"),
            keys.getKey("stalled"),
            keys.getKey("limiter"),
            keys.getKey("delayed"),
            keys.getKey("paused"),
            keys.getKey("meta"),
            keys.getKey("pc"),
            keys.getKey(target),
            keys.toJobKey(jobId),
            keys.toKey("metrics:" + target),
            keys.getKey("marker")
        };

        long timestamp = System.currentTimeMillis();
        Map<String, Object> packedOpts = new HashMap<>();
        packedOpts.put("token", token != null ? token : "0");
        packedOpts.put("keepJobs", Map.of("count", 1000));
        packedOpts.put("maxMetricsSize", "");
        if (jobOpts != null) {
            packedOpts.put("fpof", Boolean.TRUE.equals(jobOpts.get("failParentOnFailure")));
            packedOpts.put("cpof", Boolean.TRUE.equals(jobOpts.get("continueParentOnFailure")));
            packedOpts.put("idof", Boolean.TRUE.equals(jobOpts.get("ignoreDependencyOnFailure")));
            packedOpts.put("rdof", Boolean.TRUE.equals(jobOpts.get("removeDependencyOnFailure")));
        }

        byte[] jobIdBytes = jobId.getBytes(StandardCharsets.UTF_8);
        byte[] tsBytes = String.valueOf(timestamp).getBytes(StandardCharsets.UTF_8);
        byte[] propValBytes = propVal.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = (value != null ? value : "").getBytes(StandardCharsets.UTF_8);
        byte[] targetBytes = target.getBytes(StandardCharsets.UTF_8);
        byte[] fetchNextBytes = (fetchNext ? "1" : "").getBytes(StandardCharsets.UTF_8);
        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] optsBytes = BullMsgPack.pack(packedOpts);
        byte[] emptyFieldsBytes = new byte[0];

        return scriptManager.evalBinary(connection, LuaScript.MOVE_TO_FINISHED,
            ScriptOutputType.MULTI, redisKeys, jobIdBytes, tsBytes, propValBytes,
            valueBytes, targetBytes, fetchNextBytes, prefixBytes, optsBytes, emptyFieldsBytes);
    }

    public boolean extendLock(StatefulRedisConnection<String, byte[]> connection,
                              QueueKeys keys, String jobId, String token, int durationMs) {
        String[] redisKeys = new String[] {
            keys.toJobLockKey(jobId),
            keys.getKey("stalled")
        };

        byte[] tokenBytes = token.getBytes(StandardCharsets.UTF_8);
        byte[] durationBytes = String.valueOf(durationMs).getBytes(StandardCharsets.UTF_8);
        byte[] jobIdBytes = jobId.getBytes(StandardCharsets.UTF_8);

        Long result = scriptManager.evalBinary(connection, LuaScript.EXTEND_LOCK,
            ScriptOutputType.INTEGER, redisKeys, tokenBytes, durationBytes, jobIdBytes);

        return result != null && result == 1L;
    }

    public List<Object> moveStalledJobsToWait(StatefulRedisConnection<String, byte[]> connection,
                                             QueueKeys keys, int maxStalledCount, int stalledIntervalMs) {
        String[] redisKeys = new String[] {
            keys.getKey("stalled"),
            keys.getKey("wait"),
            keys.getKey("active"),
            keys.getKey("failed"),
            keys.getKey("stalled-check"),
            keys.getKey("meta"),
            keys.getKey("paused"),
            keys.getKey("events"),
            keys.getKey("marker")
        };

        long timestamp = System.currentTimeMillis();
        byte[] maxStalledBytes = String.valueOf(maxStalledCount).getBytes(StandardCharsets.UTF_8);
        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] tsBytes = String.valueOf(timestamp).getBytes(StandardCharsets.UTF_8);
        byte[] intervalBytes = String.valueOf(stalledIntervalMs).getBytes(StandardCharsets.UTF_8);

        return scriptManager.evalBinary(connection, LuaScript.MOVE_STALLED_JOBS_TO_WAIT,
            ScriptOutputType.MULTI, redisKeys, maxStalledBytes, prefixBytes, tsBytes, intervalBytes);
    }

    public int updateProgress(StatefulRedisConnection<String, byte[]> connection,
                              QueueKeys keys, String jobId, String progressJson) {
        String[] redisKeys = new String[] {
            keys.toJobKey(jobId),
            keys.getKey("events"),
            keys.getKey("meta")
        };

        byte[] jobIdBytes = jobId.getBytes(StandardCharsets.UTF_8);
        byte[] progressBytes = (progressJson != null ? progressJson : "0").getBytes(StandardCharsets.UTF_8);

        Long result = scriptManager.evalBinary(connection, LuaScript.UPDATE_PROGRESS,
            ScriptOutputType.INTEGER, redisKeys, jobIdBytes, progressBytes);

        return result != null ? result.intValue() : -1;
    }

    public int addLog(StatefulRedisConnection<String, byte[]> connection,
                      QueueKeys keys, String jobId, String logRow, int keepLogs) {
        String[] redisKeys = new String[] {
            keys.toJobKey(jobId),
            keys.toJobLogsKey(jobId)
        };

        byte[] jobIdBytes = jobId.getBytes(StandardCharsets.UTF_8);
        byte[] logBytes = (logRow != null ? logRow : "").getBytes(StandardCharsets.UTF_8);
        byte[] keepLogsBytes = (keepLogs > 0 ? String.valueOf(keepLogs) : "").getBytes(StandardCharsets.UTF_8);

        Long result = scriptManager.evalBinary(connection, LuaScript.ADD_LOG,
            ScriptOutputType.INTEGER, redisKeys, jobIdBytes, logBytes, keepLogsBytes);

        return result != null ? result.intValue() : -1;
    }

    public void pause(StatefulRedisConnection<String, byte[]> connection,
                      QueueKeys keys, boolean pause) {
        String[] redisKeys = new String[] {
            keys.getKey("wait"),
            keys.getKey("paused"),
            keys.getKey("meta"),
            keys.getKey("prioritized"),
            keys.getKey("marker"),
            keys.getKey("events"),
            keys.getKey("delayed")
        };

        byte[] actionBytes = (pause ? "paused" : "resumed").getBytes(StandardCharsets.UTF_8);

        scriptManager.evalBinary(connection, LuaScript.PAUSE,
            ScriptOutputType.STATUS, redisKeys, actionBytes);
    }

    public Long obliterate(StatefulRedisConnection<String, byte[]> connection,
                           QueueKeys keys, int count, boolean force) {
        String[] redisKeys = new String[] {
            keys.getKey("meta"),
            keys.getKey("id")
        };

        byte[] countBytes = String.valueOf(count).getBytes(StandardCharsets.UTF_8);
        byte[] forceBytes = (force ? "1" : "").getBytes(StandardCharsets.UTF_8);

        return scriptManager.evalBinary(connection, LuaScript.OBLITERATE,
            ScriptOutputType.INTEGER, redisKeys, countBytes, forceBytes);
    }

    public Long cleanJobsInSet(StatefulRedisConnection<String, byte[]> connection,
                               QueueKeys keys, String set, long graceMs, int limit) {
        String[] redisKeys = new String[] {
            keys.toKey(set),
            keys.getKey("events"),
            keys.getKey("repeat")
        };

        long timestamp = System.currentTimeMillis();
        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] graceBytes = String.valueOf(timestamp - graceMs).getBytes(StandardCharsets.UTF_8);
        byte[] limitBytes = String.valueOf(limit).getBytes(StandardCharsets.UTF_8);
        byte[] setBytes = set.getBytes(StandardCharsets.UTF_8);

        return scriptManager.evalBinary(connection, LuaScript.CLEAN_JOBS_IN_SET,
            ScriptOutputType.INTEGER, redisKeys, prefixBytes, graceBytes, limitBytes, setBytes);
    }

    private String parseJobIdResult(Object result, String commandName) {
        if (result instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        } else if (result instanceof Long num) {
            if (num < 0) {
                throw new IllegalStateException(commandName + " returned error code: " + num);
            }
            return String.valueOf(num);
        } else if (result != null) {
            return result.toString();
        }
        return null;
    }

    public LuaScriptManager getScriptManager() {
        return scriptManager;
    }
}
