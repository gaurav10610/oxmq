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
        packedOpts.put("keepJobs", resolveKeepJobs(target, jobOpts));
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

    public List<String> extendLocks(StatefulRedisConnection<String, byte[]> connection,
                                    QueueKeys keys, List<String> jobIds, List<String> tokens, int durationMs) {
        if (jobIds == null || jobIds.isEmpty()) {
            return List.of();
        }
        String[] redisKeys = new String[] {
            keys.getKey("stalled")
        };

        byte[] baseKeyBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] tokensBytes = BullMsgPack.pack(tokens);
        byte[] jobIdsBytes = BullMsgPack.pack(jobIds);
        byte[] durationBytes = String.valueOf(durationMs).getBytes(StandardCharsets.UTF_8);

        List<Object> rawResult = scriptManager.evalBinary(connection, LuaScript.EXTEND_LOCKS,
            ScriptOutputType.MULTI, redisKeys, baseKeyBytes, tokensBytes, jobIdsBytes, durationBytes);

        if (rawResult == null || rawResult.isEmpty()) {
            return List.of();
        }
        List<String> failedJobIds = new ArrayList<>(rawResult.size());
        for (Object item : rawResult) {
            if (item instanceof byte[] bytes) {
                failedJobIds.add(new String(bytes, StandardCharsets.UTF_8));
            } else if (item != null) {
                failedJobIds.add(item.toString());
            }
        }
        return failedJobIds;
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
            keys.getKey(pause ? "wait" : "paused"),
            keys.getKey(pause ? "paused" : "wait"),
            keys.getKey("meta"),
            keys.getKey("prioritized"),
            keys.getKey("events"),
            keys.getKey("delayed"),
            keys.getKey("marker")
        };

        byte[] actionBytes = (pause ? "paused" : "resumed").getBytes(StandardCharsets.UTF_8);
        byte[] emitEventBytes = "1".getBytes(StandardCharsets.UTF_8);

        scriptManager.evalBinary(connection, LuaScript.PAUSE,
            ScriptOutputType.INTEGER, redisKeys, actionBytes, emitEventBytes);
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

    public Object moveToDelayed(StatefulRedisConnection<String, byte[]> connection,
                                QueueKeys keys, String jobId, String token, long delayMs, boolean skipAttempt) {
        String[] redisKeys = new String[] {
            keys.getKey("marker"),
            keys.getKey("active"),
            keys.getKey("prioritized"),
            keys.getKey("delayed"),
            keys.toJobKey(jobId),
            keys.getKey("events"),
            keys.getKey("meta"),
            keys.getKey("stalled"),
            keys.getKey("wait"),
            keys.getKey("limiter"),
            keys.getKey("pc")
        };

        long now = System.currentTimeMillis();
        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] tsBytes = String.valueOf(now).getBytes(StandardCharsets.UTF_8);
        byte[] jobIdBytes = jobId.getBytes(StandardCharsets.UTF_8);
        byte[] tokenBytes = (token != null ? token : "0").getBytes(StandardCharsets.UTF_8);
        byte[] delayBytes = String.valueOf(delayMs).getBytes(StandardCharsets.UTF_8);
        byte[] skipAttemptBytes = (skipAttempt ? "1" : "0").getBytes(StandardCharsets.UTF_8);
        byte[] emptyFieldsBytes = new byte[0];
        byte[] fetchNextBytes = "".getBytes(StandardCharsets.UTF_8);
        byte[] optsBytes = BullMsgPack.pack(Map.of());

        return scriptManager.evalBinary(connection, LuaScript.MOVE_TO_DELAYED,
            ScriptOutputType.INTEGER, redisKeys, prefixBytes, tsBytes, jobIdBytes,
            tokenBytes, delayBytes, skipAttemptBytes, emptyFieldsBytes, fetchNextBytes, optsBytes);
    }

    public Object moveToDelayed(StatefulRedisConnection<String, byte[]> connection,
                                QueueKeys keys, String jobId, String token, long delayMs) {
        return moveToDelayed(connection, keys, jobId, token, delayMs, false);
    }

    public int retryJob(StatefulRedisConnection<String, byte[]> connection,
                        QueueKeys keys, String jobId, String token, boolean lifo) {
        String[] redisKeys = new String[] {
            keys.getKey("active"),
            keys.getKey("wait"),
            keys.getKey("paused"),
            keys.toJobKey(jobId),
            keys.getKey("meta"),
            keys.getKey("events"),
            keys.getKey("delayed"),
            keys.getKey("prioritized"),
            keys.getKey("pc"),
            keys.getKey("marker"),
            keys.getKey("stalled")
        };

        long now = System.currentTimeMillis();
        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] tsBytes = String.valueOf(now).getBytes(StandardCharsets.UTF_8);
        byte[] pushCmdBytes = (lifo ? "RPUSH" : "LPUSH").getBytes(StandardCharsets.UTF_8);
        byte[] jobIdBytes = jobId.getBytes(StandardCharsets.UTF_8);
        byte[] tokenBytes = (token != null ? token : "0").getBytes(StandardCharsets.UTF_8);
        byte[] emptyFieldsBytes = new byte[0];

        Long result = scriptManager.evalBinary(connection, LuaScript.RETRY_JOB,
            ScriptOutputType.INTEGER, redisKeys, prefixBytes, tsBytes, pushCmdBytes,
            jobIdBytes, tokenBytes, emptyFieldsBytes);

        return result != null ? result.intValue() : -1;
    }

    public int reprocessJob(StatefulRedisConnection<String, byte[]> connection,
                            QueueKeys keys, String jobId, String state, boolean lifo,
                            boolean resetAttemptsMade, boolean resetAttemptsStarted) {
        String stateKey = keys.getKey(state != null ? state : "failed");
        String[] redisKeys = new String[] {
            keys.toJobKey(jobId),
            keys.getKey("events"),
            stateKey,
            keys.getKey("wait"),
            keys.getKey("meta"),
            keys.getKey("active"),
            keys.getKey("marker"),
            keys.getKey("prioritized"),
            keys.getKey("pc")
        };

        String propVal = "failed".equals(state) ? "failedReason" : "returnvalue";
        byte[] jobIdBytes = jobId.getBytes(StandardCharsets.UTF_8);
        byte[] pushCmdBytes = (lifo ? "RPUSH" : "LPUSH").getBytes(StandardCharsets.UTF_8);
        byte[] propValBytes = propVal.getBytes(StandardCharsets.UTF_8);
        byte[] prevStateBytes = (state != null ? state : "failed").getBytes(StandardCharsets.UTF_8);
        byte[] resetAtmBytes = (resetAttemptsMade ? "1" : "0").getBytes(StandardCharsets.UTF_8);
        byte[] resetAtsBytes = (resetAttemptsStarted ? "1" : "0").getBytes(StandardCharsets.UTF_8);

        Long result = scriptManager.evalBinary(connection, LuaScript.REPROCESS_JOB,
            ScriptOutputType.INTEGER, redisKeys, jobIdBytes, pushCmdBytes, propValBytes,
            prevStateBytes, resetAtmBytes, resetAtsBytes);

        return result != null ? result.intValue() : -1;
    }

    public int promote(StatefulRedisConnection<String, byte[]> connection,
                       QueueKeys keys, String jobId) {
        String[] redisKeys = new String[] {
            keys.getKey("delayed"),
            keys.getKey("wait"),
            keys.getKey("paused"),
            keys.getKey("meta"),
            keys.getKey("prioritized"),
            keys.getKey("active"),
            keys.getKey("pc"),
            keys.getKey("events"),
            keys.getKey("marker")
        };

        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] jobIdBytes = jobId.getBytes(StandardCharsets.UTF_8);

        Long result = scriptManager.evalBinary(connection, LuaScript.PROMOTE,
            ScriptOutputType.INTEGER, redisKeys, prefixBytes, jobIdBytes);

        return result != null ? result.intValue() : -1;
    }

    public int changeDelay(StatefulRedisConnection<String, byte[]> connection,
                           QueueKeys keys, String jobId, long delayMs) {
        String[] redisKeys = new String[] {
            keys.getKey("delayed"),
            keys.getKey("meta"),
            keys.getKey("marker"),
            keys.getKey("events")
        };

        long now = System.currentTimeMillis();
        byte[] delayBytes = String.valueOf(delayMs).getBytes(StandardCharsets.UTF_8);
        byte[] tsBytes = String.valueOf(now).getBytes(StandardCharsets.UTF_8);
        byte[] jobIdBytes = jobId.getBytes(StandardCharsets.UTF_8);
        byte[] jobKeyBytes = keys.toJobKey(jobId).getBytes(StandardCharsets.UTF_8);

        Long result = scriptManager.evalBinary(connection, LuaScript.CHANGE_DELAY,
            ScriptOutputType.INTEGER, redisKeys, delayBytes, tsBytes, jobIdBytes, jobKeyBytes);

        return result != null ? result.intValue() : -1;
    }

    public int changePriority(StatefulRedisConnection<String, byte[]> connection,
                              QueueKeys keys, String jobId, int priority, boolean lifo) {
        String[] redisKeys = new String[] {
            keys.getKey("wait"),
            keys.getKey("paused"),
            keys.getKey("meta"),
            keys.getKey("prioritized"),
            keys.getKey("active"),
            keys.getKey("pc"),
            keys.getKey("marker")
        };

        byte[] priorityBytes = String.valueOf(priority).getBytes(StandardCharsets.UTF_8);
        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] jobIdBytes = jobId.getBytes(StandardCharsets.UTF_8);
        byte[] lifoBytes = (lifo ? "1" : "").getBytes(StandardCharsets.UTF_8);

        Long result = scriptManager.evalBinary(connection, LuaScript.CHANGE_PRIORITY,
            ScriptOutputType.INTEGER, redisKeys, priorityBytes, prefixBytes, jobIdBytes, lifoBytes);

        return result != null ? result.intValue() : -1;
    }

    public int removeJob(StatefulRedisConnection<String, byte[]> connection,
                         QueueKeys keys, String jobId, boolean removeChildren) {
        String[] redisKeys = new String[] {
            keys.toJobKey(jobId),
            keys.getKey("repeat")
        };

        byte[] jobIdBytes = jobId.getBytes(StandardCharsets.UTF_8);
        byte[] removeChildrenBytes = (removeChildren ? "1" : "0").getBytes(StandardCharsets.UTF_8);
        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);

        Long result = scriptManager.evalBinary(connection, LuaScript.REMOVE_JOB,
            ScriptOutputType.INTEGER, redisKeys, jobIdBytes, removeChildrenBytes, prefixBytes);

        return result != null ? result.intValue() : 0;
    }

    public int updateData(StatefulRedisConnection<String, byte[]> connection,
                          QueueKeys keys, String jobId, String jsonData) {
        String[] redisKeys = new String[] {
            keys.toJobKey(jobId)
        };

        byte[] dataBytes = (jsonData != null ? jsonData : "{}").getBytes(StandardCharsets.UTF_8);

        Long result = scriptManager.evalBinary(connection, LuaScript.UPDATE_DATA,
            ScriptOutputType.INTEGER, redisKeys, dataBytes);

        return result != null ? result.intValue() : -1;
    }

    public void drain(StatefulRedisConnection<String, byte[]> connection,
                      QueueKeys keys, boolean delayed) {
        String[] redisKeys = new String[] {
            keys.getKey("wait"),
            keys.getKey("paused"),
            keys.getKey("delayed"),
            keys.getKey("prioritized"),
            keys.getKey("repeat")
        };

        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] delayedBytes = (delayed ? "1" : "").getBytes(StandardCharsets.UTF_8);

        scriptManager.evalBinary(connection, LuaScript.DRAIN,
            ScriptOutputType.STATUS, redisKeys, prefixBytes, delayedBytes);
    }

    public String getState(StatefulRedisConnection<String, byte[]> connection,
                           QueueKeys keys, String jobId) {
        String[] redisKeys = new String[] {
            keys.getKey("completed"),
            keys.getKey("failed"),
            keys.getKey("delayed"),
            keys.getKey("active"),
            keys.getKey("wait"),
            keys.getKey("paused"),
            keys.getKey("waiting-children"),
            keys.getKey("prioritized")
        };

        byte[] jobIdBytes = jobId.getBytes(StandardCharsets.UTF_8);

        Object result = scriptManager.evalBinary(connection, LuaScript.GET_STATE,
            ScriptOutputType.STATUS, redisKeys, jobIdBytes);

        return result != null ? result.toString() : "unknown";
    }

    public List<Long> getCounts(StatefulRedisConnection<String, byte[]> connection,
                               QueueKeys keys, String... types) {
        String[] redisKeys = new String[] {
            keys.toKey("")
        };

        byte[][] args = new byte[types.length][];
        for (int i = 0; i < types.length; i++) {
            args[i] = types[i].getBytes(StandardCharsets.UTF_8);
        }

        List<Object> rawList = scriptManager.evalBinary(connection, LuaScript.GET_COUNTS,
            ScriptOutputType.MULTI, redisKeys, args);

        List<Long> counts = new ArrayList<>();
        if (rawList != null) {
            for (Object obj : rawList) {
                if (obj instanceof Number n) {
                    counts.add(n.longValue());
                } else if (obj instanceof byte[] b) {
                    counts.add(Long.parseLong(new String(b, StandardCharsets.UTF_8)));
                } else {
                    counts.add(0L);
                }
            }
        }
        return counts;
    }

    public boolean removeDeduplicationKey(StatefulRedisConnection<String, byte[]> connection,
                                         QueueKeys keys, String deduplicationId, String jobId) {
        String[] redisKeys = new String[] {
            keys.toKey("de:" + deduplicationId)
        };

        byte[] jobIdBytes = (jobId != null ? jobId : "").getBytes(StandardCharsets.UTF_8);

        Long result = scriptManager.evalBinary(connection, LuaScript.REMOVE_DEDUPLICATION_KEY,
            ScriptOutputType.INTEGER, redisKeys, jobIdBytes);

        return result != null && result == 1L;
    }

    public String addJobScheduler(StatefulRedisConnection<String, byte[]> connection,
                                  QueueKeys keys, String jobSchedulerId, long nextMillis,
                                  String jobName, String jsonData, Map<String, Object> opts,
                                  Long every, String pattern, String tz) {
        String[] redisKeys = new String[] {
            keys.getKey("repeat"),
            keys.getKey("delayed"),
            keys.getKey("wait"),
            keys.getKey("paused"),
            keys.getKey("meta"),
            keys.getKey("prioritized"),
            keys.getKey("marker"),
            keys.getKey("id"),
            keys.getKey("events"),
            keys.getKey("pc"),
            keys.getKey("active")
        };

        Map<String, Object> schedulerOpts = new HashMap<>();
        schedulerOpts.put("name", jobName);
        if (every != null) schedulerOpts.put("every", every);
        if (pattern != null && !pattern.isBlank()) schedulerOpts.put("pattern", pattern);
        if (tz != null && !tz.isBlank()) schedulerOpts.put("tz", tz);

        byte[] nextMillisBytes = String.valueOf(nextMillis).getBytes(StandardCharsets.UTF_8);
        byte[] repeatOptsBytes = BullMsgPack.pack(schedulerOpts);
        byte[] schedulerIdBytes = jobSchedulerId.getBytes(StandardCharsets.UTF_8);
        byte[] dataBytes = (jsonData != null ? jsonData : "{}").getBytes(StandardCharsets.UTF_8);
        byte[] templateOptsBytes = BullMsgPack.pack(opts != null ? BullMsgPack.encodeOpts(opts) : Map.of());
        byte[] delayedOptsBytes = BullMsgPack.pack(opts != null ? BullMsgPack.encodeOpts(opts) : Map.of());
        byte[] nowBytes = String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8);
        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] producerBytes = "".getBytes(StandardCharsets.UTF_8);

        Object result = scriptManager.evalBinary(connection, LuaScript.ADD_JOB_SCHEDULER,
            ScriptOutputType.MULTI, redisKeys, nextMillisBytes, repeatOptsBytes,
            schedulerIdBytes, dataBytes, templateOptsBytes, delayedOptsBytes,
            nowBytes, prefixBytes, producerBytes);

        if (result instanceof List<?> list && !list.isEmpty()) {
            Object first = list.get(0);
            if (first instanceof byte[] bytes) {
                return new String(bytes, StandardCharsets.UTF_8);
            }
            return first != null ? first.toString() : null;
        }
        return parseJobIdResult(result, "addJobScheduler");
    }

    public int removeJobScheduler(StatefulRedisConnection<String, byte[]> connection,
                                  QueueKeys keys, String jobSchedulerId) {
        String[] redisKeys = new String[] {
            keys.getKey("repeat"),
            keys.getKey("delayed"),
            keys.getKey("events")
        };

        byte[] schedulerIdBytes = jobSchedulerId.getBytes(StandardCharsets.UTF_8);
        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);

        Long result = scriptManager.evalBinary(connection, LuaScript.REMOVE_JOB_SCHEDULER,
            ScriptOutputType.INTEGER, redisKeys, schedulerIdBytes, prefixBytes);

        return result != null ? result.intValue() : 1;
    }

    public Long moveJobsToWait(StatefulRedisConnection<String, byte[]> connection,
                               QueueKeys keys, String state, int count, Long timestamp) {
        String[] redisKeys = new String[] {
            keys.toKey(""),
            keys.getKey("events"),
            keys.getKey(state),
            keys.getKey("wait"),
            keys.getKey("paused"),
            keys.getKey("meta"),
            keys.getKey("active"),
            keys.getKey("marker")
        };

        byte[] countBytes = String.valueOf(count).getBytes(StandardCharsets.UTF_8);
        byte[] tsBytes = String.valueOf(timestamp != null ? timestamp : System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8);
        byte[] stateBytes = state.getBytes(StandardCharsets.UTF_8);

        return scriptManager.evalBinary(connection, LuaScript.MOVE_JOBS_TO_WAIT,
            ScriptOutputType.INTEGER, redisKeys, countBytes, tsBytes, stateBytes);
    }

    public List<Long> getCountsPerPriority(StatefulRedisConnection<String, byte[]> connection,
                                           QueueKeys keys, int... priorities) {
        String[] redisKeys = new String[] {
            keys.getKey("wait"),
            keys.getKey("prioritized")
        };

        byte[][] args = new byte[priorities.length][];
        for (int i = 0; i < priorities.length; i++) {
            args[i] = String.valueOf(priorities[i]).getBytes(StandardCharsets.UTF_8);
        }

        List<Object> rawList = scriptManager.evalBinary(connection, LuaScript.GET_COUNTS_PER_PRIORITY,
            ScriptOutputType.MULTI, redisKeys, args);

        List<Long> counts = new ArrayList<>();
        if (rawList != null) {
            for (Object obj : rawList) {
                if (obj instanceof Number n) {
                    counts.add(n.longValue());
                } else if (obj instanceof byte[] b) {
                    counts.add(Long.parseLong(new String(b, StandardCharsets.UTF_8)));
                } else {
                    counts.add(0L);
                }
            }
        }
        return counts;
    }

    public List<List<String>> getRanges(StatefulRedisConnection<String, byte[]> connection,
                                        QueueKeys keys, long start, long end, boolean asc, String... states) {
        String[] redisKeys = new String[] {
            keys.toKey("")
        };

        byte[][] args = new byte[3 + states.length][];
        args[0] = String.valueOf(start).getBytes(StandardCharsets.UTF_8);
        args[1] = String.valueOf(end).getBytes(StandardCharsets.UTF_8);
        args[2] = (asc ? "1" : "0").getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < states.length; i++) {
            args[3 + i] = states[i].getBytes(StandardCharsets.UTF_8);
        }

        List<Object> rawList = scriptManager.evalBinary(connection, LuaScript.GET_RANGES,
            ScriptOutputType.MULTI, redisKeys, args);

        List<List<String>> results = new ArrayList<>();
        if (rawList != null) {
            for (Object item : rawList) {
                List<String> subList = new ArrayList<>();
                if (item instanceof List<?> list) {
                    for (Object elem : list) {
                        if (elem instanceof byte[] b) {
                            subList.add(new String(b, StandardCharsets.UTF_8));
                        } else if (elem != null) {
                            subList.add(elem.toString());
                        }
                    }
                }
                results.add(subList);
            }
        }
        return results;
    }

    public List<Long> getDependencyCounts(StatefulRedisConnection<String, byte[]> connection,
                                          QueueKeys keys, String jobId, String... types) {
        String jobKey = keys.toJobKey(jobId);
        String[] redisKeys = new String[] {
            jobKey + ":processed",
            jobKey + ":dependencies",
            jobKey + ":failed",
            jobKey + ":unsuccessful"
        };

        byte[][] args = new byte[types.length][];
        for (int i = 0; i < types.length; i++) {
            args[i] = types[i].getBytes(StandardCharsets.UTF_8);
        }

        List<Object> rawList = scriptManager.evalBinary(connection, LuaScript.GET_DEPENDENCY_COUNTS,
            ScriptOutputType.MULTI, redisKeys, args);

        List<Long> counts = new ArrayList<>();
        if (rawList != null) {
            for (Object obj : rawList) {
                if (obj instanceof Number n) {
                    counts.add(n.longValue());
                } else if (obj instanceof byte[] b) {
                    counts.add(Long.parseLong(new String(b, StandardCharsets.UTF_8)));
                } else {
                    counts.add(0L);
                }
            }
        }
        return counts;
    }

    public boolean removeChildDependency(StatefulRedisConnection<String, byte[]> connection,
                                         QueueKeys keys, String jobId, String parentKey) {
        String[] redisKeys = new String[] {
            keys.toKey("")
        };

        byte[] jobKeyBytes = keys.toJobKey(jobId).getBytes(StandardCharsets.UTF_8);
        byte[] parentKeyBytes = parentKey.getBytes(StandardCharsets.UTF_8);

        Long result = scriptManager.evalBinary(connection, LuaScript.REMOVE_CHILD_DEPENDENCY,
            ScriptOutputType.INTEGER, redisKeys, jobKeyBytes, parentKeyBytes);

        return result != null && result == 0L;
    }

    public void removeUnprocessedChildren(StatefulRedisConnection<String, byte[]> connection,
                                          QueueKeys keys, String jobId) {
        String[] redisKeys = new String[] {
            keys.toJobKey(jobId),
            keys.getKey("meta")
        };

        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] jobIdBytes = jobId.getBytes(StandardCharsets.UTF_8);

        scriptManager.evalBinary(connection, LuaScript.REMOVE_UNPROCESSED_CHILDREN,
            ScriptOutputType.STATUS, redisKeys, prefixBytes, jobIdBytes);
    }

    public List<Object> getJobScheduler(StatefulRedisConnection<String, byte[]> connection,
                                        QueueKeys keys, String jobSchedulerId) {
        String[] redisKeys = new String[] {
            keys.getKey("repeat")
        };

        byte[] schedulerIdBytes = jobSchedulerId.getBytes(StandardCharsets.UTF_8);

        return scriptManager.evalBinary(connection, LuaScript.GET_JOB_SCHEDULER,
            ScriptOutputType.MULTI, redisKeys, schedulerIdBytes);
    }

    public boolean isMaxed(StatefulRedisConnection<String, byte[]> connection,
                           QueueKeys keys) {
        String[] redisKeys = new String[] {
            keys.getKey("meta"),
            keys.getKey("active")
        };

        Long result = scriptManager.evalBinary(connection, LuaScript.IS_MAXED,
            ScriptOutputType.INTEGER, redisKeys);

        return result != null && result == 1L;
    }

    public Long removeOrphanedJobs(StatefulRedisConnection<String, byte[]> connection,
                                   QueueKeys keys, List<String> candidateJobIds) {
        String[] redisKeys = new String[] {
            keys.toKey("")
        };

        String[] stateSuffixes = new String[] {
            "wait", "active", "delayed", "paused", "completed", "failed", "prioritized", "waiting-children"
        };
        String[] subKeySuffixes = new String[] {
            "logs", "dependencies", "processed", "failed", "unsuccessful", "lock"
        };

        List<byte[]> argsList = new ArrayList<>();
        argsList.add(String.valueOf(stateSuffixes.length).getBytes(StandardCharsets.UTF_8));
        for (String s : stateSuffixes) {
            argsList.add(s.getBytes(StandardCharsets.UTF_8));
        }
        argsList.add(String.valueOf(subKeySuffixes.length).getBytes(StandardCharsets.UTF_8));
        for (String sub : subKeySuffixes) {
            argsList.add(sub.getBytes(StandardCharsets.UTF_8));
        }
        for (String id : candidateJobIds) {
            argsList.add(id.getBytes(StandardCharsets.UTF_8));
        }

        byte[][] args = argsList.toArray(new byte[0][]);
        return scriptManager.evalBinary(connection, LuaScript.REMOVE_ORPHANED_JOBS,
            ScriptOutputType.INTEGER, redisKeys, args);
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

    @SuppressWarnings("unchecked")
    private Map<String, Object> resolveKeepJobs(String target, Map<String, Object> jobOpts) {
        if (jobOpts == null) {
            return Map.of("count", -1);
        }
        Object conf = "completed".equals(target) ? jobOpts.get("removeOnComplete") : jobOpts.get("removeOnFail");
        if (conf == null) {
            return Map.of("count", -1);
        }
        if (Boolean.TRUE.equals(conf)) {
            return Map.of("count", 0);
        }
        if (Boolean.FALSE.equals(conf)) {
            return Map.of("count", -1);
        }
        if (conf instanceof Number num) {
            return Map.of("count", num.intValue());
        }
        if (conf instanceof Map<?, ?> map) {
            Map<String, Object> result = new HashMap<>();
            map.forEach((k, v) -> result.put(String.valueOf(k), v));
            if (!result.containsKey("count") && !result.containsKey("age")) {
                result.put("count", -1);
            }
            return result;
        }
        return Map.of("count", -1);
    }

    @SuppressWarnings("unchecked")
    public List<Object> isFinished(StatefulRedisConnection<String, byte[]> connection,
                                  QueueKeys keys, String jobId, boolean fetchValue) {
        String[] redisKeys = new String[] {
            keys.getKey("completed"),
            keys.getKey("failed"),
            keys.toJobKey(jobId)
        };
        byte[] jobIdBytes = jobId.getBytes(StandardCharsets.UTF_8);
        byte[] fetchValueBytes = (fetchValue ? "1" : "0").getBytes(StandardCharsets.UTF_8);

        Object res = scriptManager.evalBinary(connection, LuaScript.IS_FINISHED,
            ScriptOutputType.MULTI, redisKeys, jobIdBytes, fetchValueBytes);
        if (res instanceof List<?> list) {
            return (List<Object>) list;
        } else if (res instanceof Number num) {
            return List.of(num);
        }
        return List.of();
    }

    public LuaScriptManager getScriptManager() {
        return scriptManager;
    }
}
