package io.oxmq.backend;

import io.oxmq.model.JobDependencyCounts;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * Pluggable Queue Backend contract mirroring BullMQ v6's IQueueBackend.
 * Decouples Queue and Worker facades from specific datastore and driver implementations.
 *
 * @deprecated Since 1.8.0 for removal in 2.0.0. OxMQ natively uses high-throughput direct Lua script pipelines
 *             and Netty binary buffers via {@link io.oxmq.lua.BullScripts} and {@link io.oxmq.lua.ExtensionScripts}.
 */
@Deprecated(since = "1.8.0", forRemoval = true)
public interface QueueBackend extends Closeable {

    String addStandardJob(String customId, String name, String jsonData, Map<String, Object> opts, long timestamp);

    String addDelayedJob(String customId, String name, String jsonData, Map<String, Object> opts, long timestamp, long delayMs);

    String addPrioritizedJob(String customId, String name, String jsonData, Map<String, Object> opts, long timestamp, int priority);

    String addParentJob(String customId, String name, String jsonData, Map<String, Object> opts, long timestamp);

    List<Object> moveToActive(String token, int lockDurationMs, Map<String, Object> limiter, String workerName, long timestamp);

    Object moveToFinished(String jobId, String val, String propVal, String target, String token, boolean fetchNext, Map<String, Object> jobOpts);

    Object moveToDelayed(String jobId, String token, long delayMs, boolean skipAttempt);

    default Object moveToDelayed(String jobId, String token, long delayMs, boolean skipAttempt, Map<String, Object> fieldsToUpdate) {
        return moveToDelayed(jobId, token, delayMs, skipAttempt);
    }

    int retryJob(String jobId, String token, boolean lifo);

    default int retryJob(String jobId, String token, boolean lifo, Map<String, Object> fieldsToUpdate) {
        return retryJob(jobId, token, lifo);
    }

    int reprocessJob(String jobId, String state, boolean lifo, boolean resetAttemptsMade, boolean resetAttemptsStarted);

    Long moveJobsToWait(String state, int count, Long timestamp);

    long promoteJobs(int count);

    boolean extendLock(String jobId, String token, int durationMs);

    List<Object> moveStalledJobsToWait(int maxStalledCount, int stalledIntervalMs);

    int updateProgress(String jobId, String progressJson);

    int addLog(String jobId, String logRow, int keepLogs);

    void pause(boolean pause);

    Long obliterate(int count, boolean force);

    Long cleanJobsInSet(String set, long graceMs, int limit);

    int promote(String jobId);

    int changeDelay(String jobId, long delayMs);

    int changePriority(String jobId, int priority, boolean lifo);

    int removeJob(String jobId, boolean removeChildren);

    int updateData(String jobId, String jsonData);

    void drain(boolean delayed);

    String getState(String jobId);

    List<Object> isFinished(String jobId, boolean fetchValue);

    List<Long> getCounts(String... types);

    List<Long> getCountsPerPriority(int... priorities);

    List<List<String>> getRanges(long start, long end, boolean asc, String... states);

    JobDependencyCounts getDependencyCounts(String jobId);

    boolean removeChildDependency(String jobId, String parentKey);

    void removeUnprocessedChildren(String jobId);

    boolean removeDeduplicationKey(String deduplicationId, String jobId);

    String addJobScheduler(String jobSchedulerId, long nextMillis, String jobName, String jsonData,
                           Map<String, Object> opts, Long every, String pattern, String tz);

    int removeJobScheduler(String jobSchedulerId);

    List<Object> getJobScheduler(String jobSchedulerId);

    long getJobSchedulersCount();

    boolean isMaxed();

    void setGlobalConcurrency(int concurrency);

    Integer getGlobalConcurrency();

    boolean removeGlobalConcurrency();

    void setGlobalRateLimit(int max, long durationMillis);

    io.oxmq.model.GlobalRateLimit getGlobalRateLimit();

    boolean removeGlobalRateLimit();

    boolean removeRateLimitKey();

    Long removeOrphanedJobs(List<String> candidateJobIds);

    void trimEvents(long maxLength);

    List<Object> getJobs(int start, int end, boolean asc, int maxIterations, String... states);

    List<Object> paginate(String key, int pageStart, int pageEnd, String cursor, int offset, int maxIterations, boolean fetchJobs);

    List<Object> getMetrics(String type, int start, int end);

    Long getRateLimitTtl(int maxJobs);

    String getStateV2(String jobId);

    boolean isJobInList(String listKey, String jobId);

    boolean releaseLock(String jobId, String token);

    int moveToWaitingChildren(String jobId, String token, String childKey, long timestamp);

    int moveJobFromActiveToWait(String jobId, String token);

    int saveStacktrace(String jobId, String stacktrace, String failedReason);

    String updateJobScheduler(String jobSchedulerId, long nextMillis, String jsonData, Map<String, Object> opts, long timestamp);

    default String updateJobScheduler(String jobSchedulerId, long nextMillis, String jsonData, Map<String, Object> opts, long timestamp, String currentJobId) {
        return updateJobScheduler(jobSchedulerId, nextMillis, jsonData, opts, timestamp);
    }

    @Override
    void close();
}
