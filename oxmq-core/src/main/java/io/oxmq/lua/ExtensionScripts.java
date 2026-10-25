package io.oxmq.lua;

import io.lettuce.core.RedisException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.oxmq.client.QueueKeys;
import io.oxmq.exception.GroupMaxSizeExceededException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * High-performance Java bridge for invoking OxMQ Extension Lua scripts.
 * Encapsulates Redis key schemas and serialization for virtual queue groups.
 */
public class ExtensionScripts {

    private final LuaScriptManager scriptManager;

    public ExtensionScripts(LuaScriptManager scriptManager) {
        this.scriptManager = scriptManager;
    }

    public LuaScriptManager getScriptManager() {
        return scriptManager;
    }

    /**
     * Enqueues a job into a virtual group.
     */
    public String addGroupJob(StatefulRedisConnection<String, byte[]> connection,
                              QueueKeys keys, String customJobId, String jobName,
                              String jsonData, String optsJson, String groupId,
                              long maxSize, int priority, String deduplicationId,
                              long timestamp) {
        String[] redisKeys = new String[] {
            keys.toKey("groups:waiting"),
            keys.toKey("groups:" + groupId),
            keys.toKey("groups:" + groupId + ":prioritized"),
            keys.toKey("groups:paused"),
            keys.toKey("groups:maxed"),
            keys.toKey("groups:limited"),
            keys.getKey("id"),
            keys.getKey("events")
        };

        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] customIdBytes = (customJobId != null ? customJobId : "").getBytes(StandardCharsets.UTF_8);
        byte[] nameBytes = (jobName != null ? jobName : "").getBytes(StandardCharsets.UTF_8);
        byte[] tsBytes = String.valueOf(timestamp).getBytes(StandardCharsets.UTF_8);
        byte[] dataBytes = (jsonData != null ? jsonData : "{}").getBytes(StandardCharsets.UTF_8);
        byte[] optsBytes = (optsJson != null ? optsJson : "{}").getBytes(StandardCharsets.UTF_8);
        byte[] groupBytes = (groupId != null ? groupId : "").getBytes(StandardCharsets.UTF_8);
        byte[] maxSizeBytes = String.valueOf(maxSize).getBytes(StandardCharsets.UTF_8);
        byte[] prioBytes = String.valueOf(priority).getBytes(StandardCharsets.UTF_8);
        byte[] dedupBytes = (deduplicationId != null ? deduplicationId : "").getBytes(StandardCharsets.UTF_8);

        try {
            Object result = scriptManager.evalBinary(connection, ExtensionLuaScript.ADD_GROUP_JOB,
                    ScriptOutputType.VALUE, redisKeys,
                    prefixBytes, customIdBytes, nameBytes, tsBytes, dataBytes, optsBytes,
                    groupBytes, maxSizeBytes, prioBytes, dedupBytes);

            if (result instanceof byte[] b) {
                return new String(b, StandardCharsets.UTF_8);
            }
            return result != null ? result.toString() : null;
        } catch (RedisException e) {
            if (e.getMessage() != null && e.getMessage().contains("GROUP_MAX_SIZE_EXCEEDED")) {
                throw new GroupMaxSizeExceededException(groupId, maxSize);
            }
            throw e;
        }
    }

    /**
     * Pops the next available job across virtual groups using fair round-robin scheduling.
     */
    public List<Object> moveToActiveGroup(StatefulRedisConnection<String, byte[]> connection,
                                         QueueKeys keys, String workerId,
                                         long lockDurationMs, int maxGroupConcurrency,
                                         long nowTimestamp) {
        String[] redisKeys = new String[] {
            keys.toKey("groups:waiting"),
            keys.toKey("groups:active:count"),
            keys.toKey("groups:paused"),
            keys.toKey("groups:limited"),
            keys.toKey("groups:maxed"),
            keys.getKey("active"),
            keys.getKey("events")
        };

        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] workerBytes = workerId.getBytes(StandardCharsets.UTF_8);
        byte[] lockDurBytes = String.valueOf(lockDurationMs).getBytes(StandardCharsets.UTF_8);
        byte[] groupConcBytes = String.valueOf(maxGroupConcurrency).getBytes(StandardCharsets.UTF_8);
        byte[] nowBytes = String.valueOf(nowTimestamp).getBytes(StandardCharsets.UTF_8);

        return scriptManager.evalBinary(connection, ExtensionLuaScript.MOVE_TO_ACTIVE_GROUP,
                ScriptOutputType.MULTI, redisKeys,
                prefixBytes, workerBytes, lockDurBytes, groupConcBytes, nowBytes);
    }

    /**
     * Pops a batch of jobs with strict Group Affinity (all jobs from the exact same group).
     */
    public List<Object> moveToActiveBatchGroup(StatefulRedisConnection<String, byte[]> connection,
                                              QueueKeys keys, String workerId,
                                              long lockDurationMs, int maxBatchSize,
                                              int maxGroupConcurrency, long nowTimestamp) {
        String[] redisKeys = new String[] {
            keys.toKey("groups:waiting"),
            keys.toKey("groups:active:count"),
            keys.toKey("groups:paused"),
            keys.toKey("groups:limited"),
            keys.toKey("groups:maxed"),
            keys.getKey("active"),
            keys.getKey("events")
        };

        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] workerBytes = workerId.getBytes(StandardCharsets.UTF_8);
        byte[] lockDurBytes = String.valueOf(lockDurationMs).getBytes(StandardCharsets.UTF_8);
        byte[] batchSizeBytes = String.valueOf(maxBatchSize).getBytes(StandardCharsets.UTF_8);
        byte[] groupConcBytes = String.valueOf(maxGroupConcurrency).getBytes(StandardCharsets.UTF_8);
        byte[] nowBytes = String.valueOf(nowTimestamp).getBytes(StandardCharsets.UTF_8);

        return scriptManager.evalBinary(connection, ExtensionLuaScript.MOVE_TO_ACTIVE_BATCH_GROUP,
                ScriptOutputType.MULTI, redisKeys,
                prefixBytes, workerBytes, lockDurBytes, batchSizeBytes, groupConcBytes, nowBytes);
    }

    /**
     * Decrements the group's active count when a job finishes or fails.
     */
    public long decrementGroupActive(StatefulRedisConnection<String, byte[]> connection,
                                    QueueKeys keys, String groupId, int maxGroupConcurrency) {
        String[] redisKeys = new String[] {
            keys.toKey("groups:waiting"),
            keys.toKey("groups:active:count"),
            keys.toKey("groups:maxed"),
            keys.toKey("groups:paused")
        };

        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] groupBytes = (groupId != null ? groupId : "").getBytes(StandardCharsets.UTF_8);
        byte[] concBytes = String.valueOf(maxGroupConcurrency).getBytes(StandardCharsets.UTF_8);

        Long result = scriptManager.evalBinary(connection, ExtensionLuaScript.DECREMENT_GROUP_ACTIVE,
                ScriptOutputType.INTEGER, redisKeys,
                prefixBytes, groupBytes, concBytes);
        return result != null ? result : 0;
    }

    /**
     * Pauses a virtual group.
     */
    public boolean pauseGroup(StatefulRedisConnection<String, byte[]> connection,
                              QueueKeys keys, String groupId) {
        String[] redisKeys = new String[] {
            keys.toKey("groups:waiting"),
            keys.toKey("groups:paused"),
            keys.getKey("events")
        };

        byte[] groupBytes = (groupId != null ? groupId : "").getBytes(StandardCharsets.UTF_8);

        Long result = scriptManager.evalBinary(connection, ExtensionLuaScript.PAUSE_GROUP,
                ScriptOutputType.INTEGER, redisKeys, groupBytes);
        return result != null && result == 1;
    }

    /**
     * Resumes a paused virtual group.
     */
    public boolean resumeGroup(StatefulRedisConnection<String, byte[]> connection,
                               QueueKeys keys, String groupId) {
        String[] redisKeys = new String[] {
            keys.toKey("groups:waiting"),
            keys.toKey("groups:paused"),
            keys.toKey("groups:maxed"),
            keys.getKey("events")
        };

        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] groupBytes = (groupId != null ? groupId : "").getBytes(StandardCharsets.UTF_8);

        Long result = scriptManager.evalBinary(connection, ExtensionLuaScript.RESUME_GROUP,
                ScriptOutputType.INTEGER, redisKeys, prefixBytes, groupBytes);
        return result != null && result == 1;
    }

    /**
     * Deletes all pending jobs in a virtual group and cleans up indices.
     */
    public long deleteGroup(StatefulRedisConnection<String, byte[]> connection,
                            QueueKeys keys, String groupId) {
        String[] redisKeys = new String[] {
            keys.toKey("groups:waiting"),
            keys.toKey("groups:paused"),
            keys.toKey("groups:maxed"),
            keys.toKey("groups:limited"),
            keys.toKey("groups:active:count"),
            keys.getKey("events")
        };

        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] groupBytes = (groupId != null ? groupId : "").getBytes(StandardCharsets.UTF_8);

        Long result = scriptManager.evalBinary(connection, ExtensionLuaScript.DELETE_GROUP,
                ScriptOutputType.INTEGER, redisKeys, prefixBytes, groupBytes);
        return result != null ? result : 0;
    }

    /**
     * Sets a dynamic rate-limit duration on a virtual group.
     */
    public boolean setGroupRateLimit(StatefulRedisConnection<String, byte[]> connection,
                                     QueueKeys keys, String groupId, long durationMs) {
        String[] redisKeys = new String[] {
            keys.toKey("groups:waiting"),
            keys.toKey("groups:limited"),
            keys.getKey("events")
        };

        long expireTs = System.currentTimeMillis() + durationMs;
        byte[] groupBytes = (groupId != null ? groupId : "").getBytes(StandardCharsets.UTF_8);
        byte[] expireBytes = String.valueOf(expireTs).getBytes(StandardCharsets.UTF_8);
        byte[] durBytes = String.valueOf(durationMs).getBytes(StandardCharsets.UTF_8);

        Long result = scriptManager.evalBinary(connection, ExtensionLuaScript.SET_GROUP_RATE_LIMIT,
                ScriptOutputType.INTEGER, redisKeys, groupBytes, expireBytes, durBytes);
        return result != null && result == 1;
    }

    public record GroupEntry(String id, long count) {}

    /**
     * Retrieves groups by status ('waiting', 'limited', 'maxed', 'paused') with pagination.
     */
    public List<GroupEntry> getGroupsByStatus(StatefulRedisConnection<String, byte[]> connection,
                                             QueueKeys keys, String status, long start, long stop) {
        String targetKey;
        if ("waiting".equals(status)) {
            targetKey = keys.toKey("groups:waiting");
        } else if ("limited".equals(status)) {
            targetKey = keys.toKey("groups:limited");
        } else if ("maxed".equals(status)) {
            targetKey = keys.toKey("groups:maxed");
        } else if ("paused".equals(status)) {
            targetKey = keys.toKey("groups:paused");
        } else {
            return Collections.emptyList();
        }

        String[] redisKeys = new String[] { targetKey };
        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] statusBytes = status.getBytes(StandardCharsets.UTF_8);
        byte[] startBytes = String.valueOf(start).getBytes(StandardCharsets.UTF_8);
        byte[] stopBytes = String.valueOf(stop).getBytes(StandardCharsets.UTF_8);

        List<Object> raw = scriptManager.evalBinary(connection, ExtensionLuaScript.GET_GROUPS_BY_STATUS,
                ScriptOutputType.MULTI, redisKeys, prefixBytes, statusBytes, startBytes, stopBytes);

        if (raw == null || raw.isEmpty()) {
            return Collections.emptyList();
        }

        List<GroupEntry> entries = new ArrayList<>();
        for (Object item : raw) {
            if (item instanceof List<?> pair && pair.size() >= 2) {
                String id = pair.get(0) instanceof byte[] b ? new String(b, StandardCharsets.UTF_8) : pair.get(0).toString();
                String cntStr = pair.get(1) instanceof byte[] b ? new String(b, StandardCharsets.UTF_8) : pair.get(1).toString();
                long count = 0;
                try {
                    count = Long.parseLong(cntStr);
                } catch (NumberFormatException ignored) {}
                entries.add(new GroupEntry(id, count));
            }
        }
        return entries;
    }

    /**
     * Retrieves a paginated list of job IDs for a group.
     */
    public List<String> getGroupJobs(StatefulRedisConnection<String, byte[]> connection,
                                    QueueKeys keys, String groupId, long start, long stop) {
        String[] redisKeys = new String[] {
            keys.toKey("groups:" + groupId),
            keys.toKey("groups:" + groupId + ":prioritized")
        };

        byte[] prefixBytes = keys.toKey("").getBytes(StandardCharsets.UTF_8);
        byte[] startBytes = String.valueOf(start).getBytes(StandardCharsets.UTF_8);
        byte[] stopBytes = String.valueOf(stop).getBytes(StandardCharsets.UTF_8);

        List<Object> raw = scriptManager.evalBinary(connection, ExtensionLuaScript.GET_GROUP_JOBS,
                ScriptOutputType.MULTI, redisKeys, prefixBytes, startBytes, stopBytes);

        if (raw == null || raw.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> jobIds = new ArrayList<>(raw.size());
        for (Object item : raw) {
            if (item instanceof byte[] b) {
                jobIds.add(new String(b, StandardCharsets.UTF_8));
            } else if (item != null) {
                jobIds.add(item.toString());
            }
        }
        return jobIds;
    }
}
