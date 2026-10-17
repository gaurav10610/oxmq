package io.oxmq.backend;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.oxmq.client.QueueKeys;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.lua.BullScripts;
import io.oxmq.lua.LuaScriptManager;
import io.oxmq.model.JobDependencyCounts;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Redis and Lua-script implementation of {@link QueueBackend}.
 * Reuses official BullMQ Lua scripts directly on Redis via Lettuce.
 */
public class RedisQueueBackend implements QueueBackend {

    private final QueueKeys queueKeys;
    private final RedisConnectionManager connectionManager;
    private final boolean ownsConnectionManager;
    private final LuaScriptManager scriptManager;
    private final BullScripts bullScripts;

    public RedisQueueBackend(QueueKeys queueKeys, RedisConnectionManager connectionManager,
                             LuaScriptManager scriptManager, BullScripts bullScripts) {
        this(queueKeys, connectionManager, scriptManager, bullScripts, false);
    }

    public RedisQueueBackend(QueueKeys queueKeys, RedisConnectionManager connectionManager,
                             LuaScriptManager scriptManager, BullScripts bullScripts, boolean ownsConnectionManager) {
        this.queueKeys = Objects.requireNonNull(queueKeys, "queueKeys must not be null");
        this.connectionManager = Objects.requireNonNull(connectionManager, "connectionManager must not be null");
        this.scriptManager = scriptManager != null ? scriptManager : new LuaScriptManager();
        this.bullScripts = bullScripts != null ? bullScripts : new BullScripts(this.scriptManager);
        this.ownsConnectionManager = ownsConnectionManager;
    }

    public RedisQueueBackend(String prefix, String queueName, RedisConnectionManager connectionManager) {
        this(new QueueKeys(prefix != null ? prefix : "bull", queueName),
             connectionManager, new LuaScriptManager(), null, false);
    }

    private StatefulRedisConnection<String, byte[]> binaryConnection() {
        return connectionManager.getBinaryConnection();
    }

    private RedisCommands<String, String> syncCommands() {
        return connectionManager.getCommandConnection().sync();
    }

    @Override
    public String addStandardJob(String customId, String name, String jsonData, Map<String, Object> opts, long timestamp) {
        return bullScripts.addStandardJob(binaryConnection(), queueKeys, customId, name, jsonData, opts, timestamp);
    }

    @Override
    public String addDelayedJob(String customId, String name, String jsonData, Map<String, Object> opts, long timestamp, long delayMs) {
        return bullScripts.addDelayedJob(binaryConnection(), queueKeys, customId, name, jsonData, opts, timestamp, delayMs);
    }

    @Override
    public String addPrioritizedJob(String customId, String name, String jsonData, Map<String, Object> opts, long timestamp, int priority) {
        return bullScripts.addPrioritizedJob(binaryConnection(), queueKeys, customId, name, jsonData, opts, timestamp, priority);
    }

    @Override
    public String addParentJob(String customId, String name, String jsonData, Map<String, Object> opts, long timestamp) {
        return bullScripts.addParentJob(binaryConnection(), queueKeys, customId, name, jsonData, opts, timestamp);
    }

    @Override
    public List<Object> moveToActive(String token, int lockDurationMs, Map<String, Object> limiter, String workerName, long timestamp) {
        return bullScripts.moveToActive(binaryConnection(), queueKeys, token, lockDurationMs, limiter, workerName, timestamp);
    }

    @Override
    public Object moveToFinished(String jobId, String val, String propVal, String target, String token, boolean fetchNext, Map<String, Object> jobOpts) {
        return bullScripts.moveToFinished(binaryConnection(), queueKeys, jobId, val, propVal, target, token, fetchNext, jobOpts);
    }

    @Override
    public Object moveToDelayed(String jobId, String token, long delayMs, boolean skipAttempt) {
        return bullScripts.moveToDelayed(binaryConnection(), queueKeys, jobId, token, delayMs, skipAttempt);
    }

    @Override
    public int retryJob(String jobId, String token, boolean lifo) {
        return bullScripts.retryJob(binaryConnection(), queueKeys, jobId, token, lifo);
    }

    @Override
    public int reprocessJob(String jobId, String state, boolean lifo, boolean resetAttemptsMade, boolean resetAttemptsStarted) {
        return bullScripts.reprocessJob(binaryConnection(), queueKeys, jobId, state, lifo, resetAttemptsMade, resetAttemptsStarted);
    }

    @Override
    public Long moveJobsToWait(String state, int count, Long timestamp) {
        return bullScripts.moveJobsToWait(binaryConnection(), queueKeys, state, count, timestamp);
    }

    @Override
    public long promoteJobs(int count) {
        int batchCount = count > 0 ? count : 1000;
        long cursor;
        long iterations = 0;
        do {
            Long res = bullScripts.moveJobsToWait(binaryConnection(), queueKeys, "delayed", batchCount, Long.MAX_VALUE);
            cursor = res != null ? res : 0L;
            iterations++;
        } while (cursor == 1L);
        return iterations;
    }

    @Override
    public boolean extendLock(String jobId, String token, int durationMs) {
        return bullScripts.extendLock(binaryConnection(), queueKeys, jobId, token, durationMs);
    }

    @Override
    public List<Object> moveStalledJobsToWait(int maxStalledCount, int stalledIntervalMs) {
        return bullScripts.moveStalledJobsToWait(binaryConnection(), queueKeys, maxStalledCount, stalledIntervalMs);
    }

    @Override
    public int updateProgress(String jobId, String progressJson) {
        return bullScripts.updateProgress(binaryConnection(), queueKeys, jobId, progressJson);
    }

    @Override
    public int addLog(String jobId, String logRow, int keepLogs) {
        return bullScripts.addLog(binaryConnection(), queueKeys, jobId, logRow, keepLogs);
    }

    @Override
    public void pause(boolean pause) {
        bullScripts.pause(binaryConnection(), queueKeys, pause);
    }

    @Override
    public Long obliterate(int count, boolean force) {
        return bullScripts.obliterate(binaryConnection(), queueKeys, count, force);
    }

    @Override
    public Long cleanJobsInSet(String set, long graceMs, int limit) {
        return bullScripts.cleanJobsInSet(binaryConnection(), queueKeys, set, graceMs, limit);
    }

    @Override
    public int promote(String jobId) {
        return bullScripts.promote(binaryConnection(), queueKeys, jobId);
    }

    @Override
    public int changeDelay(String jobId, long delayMs) {
        return bullScripts.changeDelay(binaryConnection(), queueKeys, jobId, delayMs);
    }

    @Override
    public int changePriority(String jobId, int priority, boolean lifo) {
        return bullScripts.changePriority(binaryConnection(), queueKeys, jobId, priority, lifo);
    }

    @Override
    public int removeJob(String jobId, boolean removeChildren) {
        return bullScripts.removeJob(binaryConnection(), queueKeys, jobId, removeChildren);
    }

    @Override
    public int updateData(String jobId, String jsonData) {
        return bullScripts.updateData(binaryConnection(), queueKeys, jobId, jsonData);
    }

    @Override
    public void drain(boolean delayed) {
        bullScripts.drain(binaryConnection(), queueKeys, delayed);
    }

    @Override
    public String getState(String jobId) {
        return bullScripts.getState(binaryConnection(), queueKeys, jobId);
    }

    @Override
    public List<Object> isFinished(String jobId, boolean fetchValue) {
        return bullScripts.isFinished(binaryConnection(), queueKeys, jobId, fetchValue);
    }

    @Override
    public List<Long> getCounts(String... types) {
        return bullScripts.getCounts(binaryConnection(), queueKeys, types);
    }

    @Override
    public List<Long> getCountsPerPriority(int... priorities) {
        return bullScripts.getCountsPerPriority(binaryConnection(), queueKeys, priorities);
    }

    @Override
    public List<List<String>> getRanges(long start, long end, boolean asc, String... states) {
        return bullScripts.getRanges(binaryConnection(), queueKeys, start, end, asc, states);
    }

    @Override
    public JobDependencyCounts getDependencyCounts(String jobId) {
        List<Long> counts = bullScripts.getDependencyCounts(binaryConnection(), queueKeys, jobId,
            "processed", "unprocessed", "failed", "ignored");
        long processed = !counts.isEmpty() ? counts.get(0) : 0L;
        long unprocessed = counts.size() > 1 ? counts.get(1) : 0L;
        long failed = counts.size() > 2 ? counts.get(2) : 0L;
        long ignored = counts.size() > 3 ? counts.get(3) : 0L;
        return new JobDependencyCounts(processed, unprocessed, failed, ignored);
    }

    @Override
    public boolean removeChildDependency(String jobId, String parentKey) {
        return bullScripts.removeChildDependency(binaryConnection(), queueKeys, jobId, parentKey);
    }

    @Override
    public void removeUnprocessedChildren(String jobId) {
        bullScripts.removeUnprocessedChildren(binaryConnection(), queueKeys, jobId);
    }

    @Override
    public boolean removeDeduplicationKey(String deduplicationId, String jobId) {
        return bullScripts.removeDeduplicationKey(binaryConnection(), queueKeys, deduplicationId, jobId);
    }

    @Override
    public String addJobScheduler(String jobSchedulerId, long nextMillis, String jobName, String jsonData,
                                  Map<String, Object> opts, Long every, String pattern, String tz) {
        return bullScripts.addJobScheduler(binaryConnection(), queueKeys, jobSchedulerId, nextMillis, jobName,
            jsonData, opts, every, pattern, tz);
    }

    @Override
    public int removeJobScheduler(String jobSchedulerId) {
        return bullScripts.removeJobScheduler(binaryConnection(), queueKeys, jobSchedulerId);
    }

    @Override
    public List<Object> getJobScheduler(String jobSchedulerId) {
        return bullScripts.getJobScheduler(binaryConnection(), queueKeys, jobSchedulerId);
    }

    @Override
    public long getJobSchedulersCount() {
        Long card = syncCommands().zcard(queueKeys.getKey("repeat"));
        return card != null ? card : 0L;
    }

    @Override
    public boolean isMaxed() {
        return bullScripts.isMaxed(binaryConnection(), queueKeys);
    }

    @Override
    public Long removeOrphanedJobs(List<String> candidateJobIds) {
        return bullScripts.removeOrphanedJobs(binaryConnection(), queueKeys, candidateJobIds);
    }

    @Override
    public void trimEvents(long maxLength) {
        syncCommands().xtrim(queueKeys.getKey("events"), maxLength);
    }

    @Override
    public void close() {
        if (ownsConnectionManager && connectionManager != null) {
            connectionManager.close();
        }
    }

    public QueueKeys getQueueKeys() {
        return queueKeys;
    }

    public RedisConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public BullScripts getBullScripts() {
        return bullScripts;
    }
}
