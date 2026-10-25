package io.oxmq;

import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import java.io.Closeable;
import java.util.List;

/**
 * Queue interface for producing and managing distributed background jobs.
 *
 * @param <T> Payload data type
 */
public interface Queue<T> extends Closeable {

    /**
     * Gets the queue name.
     */
    String getName();

    /**
     * Enqueues a job with default options.
     */
    Job<T> add(String name, T data);

    /**
     * Enqueues a job with custom options (delays, retries, deduplication).
     */
    Job<T> add(String name, T data, JobOptions opts);

    /**
     * Fetches a job by its unique ID.
     */
    Job<T> getJob(String jobId);

    /**
     * Pauses job consumption across the entire cluster.
     */
    void pause();

    /**
     * Resumes job consumption across the entire cluster.
     */
    void resume();

    /**
     * Checks if the queue is currently paused.
     */
    boolean isPaused();

    /**
     * Gets the count of jobs in the given state.
     */
    long count(JobState state);

    /**
     * Cleans expired completed or failed jobs.
     */
    long clean(long graceMs, int limit, JobState state);

    /**
     * Completely deletes the queue and all its associated jobs from Redis.
     */
    void obliterate();

    /**
     * Enqueues multiple jobs in a single batch.
     */
    List<Job<T>> addBulk(List<io.oxmq.model.JobRequest<T>> jobs);

    /**
     * Promotes a delayed job to the waiting state immediately.
     */
    void promote(String jobId);

    /**
     * Changes the delay of a job currently in the delayed state.
     */
    void changeDelay(String jobId, java.time.Duration delay);

    /**
     * Changes the priority of a waiting or prioritized job.
     */
    void changePriority(String jobId, int priority);

    /**
     * Retries a failed or completed job by moving it back to the waiting queue.
     */
    void retry(String jobId);

    /**
     * Removes a job and all its data from the queue.
     */
    boolean remove(String jobId);

    /**
     * Removes a job and optionally its children dependencies.
     */
    boolean remove(String jobId, boolean removeChildren);

    /**
     * Updates the payload data of an existing job.
     */
    void updateData(String jobId, T data);

    /**
     * Drains the queue, removing all waiting (and optionally delayed) jobs.
     */
    void drain(boolean delayed);

    /**
     * Retrieves the current lifecycle state of a job.
     */
    String getState(String jobId);

    /**
     * Gets a count breakdown across all job states (wait, active, delayed, completed, failed, paused).
     */
    java.util.Map<String, Long> getJobCounts();

    /**
     * Gets all log rows recorded for a job.
     */
    List<String> getJobLogs(String jobId);

    /**
     * Gets a slice of log rows recorded for a job.
     */
    List<String> getJobLogs(String jobId, int start, int end);

    /**
     * Clears all log entries for a job in Redis.
     *
     * @param jobId job identifier
     * @return true if logs were cleared
     */
    default boolean clearJobLogs(String jobId) {
        return false;
    }

    /**
     * Updates the progress percentage of an active job in Redis.
     * Mirrors BullMQ's queue.updateJobProgress(jobId, progress).
     *
     * @param jobId    job identifier
     * @param progress progress integer (0-100)
     */
    default void updateJobProgress(String jobId, int progress) {
    }

    /**
     * Appends a log line to a job.
     * Mirrors BullMQ's queue.addJobLog(jobId, logRow).
     *
     * @param jobId  job identifier
     * @param logRow log message string
     * @return total number of logs stored for this job
     */
    default long addJobLog(String jobId, String logRow) {
        return addJobLog(jobId, logRow, 0);
    }

    /**
     * Appends a log line to a job with optional max limit (keepLogs).
     * Mirrors BullMQ's queue.addJobLog(jobId, logRow, keepLogs).
     *
     * @param jobId    job identifier
     * @param logRow   log message string
     * @param keepLogs maximum number of logs to retain (0 = unlimited)
     * @return total number of logs stored for this job
     */
    default long addJobLog(String jobId, String logRow, int keepLogs) {
        return 0L;
    }

    /**
     * Clears a deduplication key, allowing a new job with the same deduplication ID to be added.
     */
    boolean removeDeduplicationKey(String deduplicationId);

    /**
     * Retrieves the Job ID that currently holds the active deduplication key lock,
     * or null if the deduplication key is not currently held.
     *
     * @param deduplicationId custom deduplication identifier
     * @return job ID holding the deduplication key, or null
     */
    default String getDeduplicationJobId(String deduplicationId) {
        return null;
    }

    /**
     * Upserts a recurring job scheduler (cron or fixed interval).
     */
    String upsertJobScheduler(String schedulerId, java.time.Duration every, String jobName, T data, JobOptions opts);

    /**
     * Upserts a recurring job scheduler using a standard cron expression.
     */
    String upsertJobScheduler(String schedulerId, String cronPattern, String jobName, T data, JobOptions opts);

    /**
     * Removes a recurring job scheduler and its next scheduled job.
     */
    boolean removeJobScheduler(String schedulerId);

    /**
     * Retries failed or completed jobs in bulk, moving them back to the wait queue and restoring DAG parent dependencies.
     */
    long retryJobs(io.oxmq.model.RetryJobsOptions options);

    /**
     * Retries failed or completed jobs in bulk with state, count, and timestamp limit.
     */
    long retryJobs(JobState state, int count, Long timestamp);

    /**
     * Promotes delayed jobs in bulk to wait status.
     */
    long promoteJobs(int count);

    /**
     * Retries a single job with options (resetAttemptsMade, resetAttemptsStarted).
     */
    void retry(String jobId, io.oxmq.model.RetryOptions options);

    /**
     * Trims the event stream to approximately maxLength.
     */
    void trimEvents(long maxLength);

    /**
     * Gets job counts breakdown per priority bucket.
     */
    java.util.Map<Integer, Long> getCountsPerPriority(int... priorities);

    /**
     * Gets paginated job IDs across multiple states in an atomic round-trip.
     */
    java.util.Map<JobState, List<String>> getRanges(JobState[] states, long start, long end, boolean asc);

    /**
     * Retrieves child dependency counts for a parent job in a DAG workflow.
     */
    io.oxmq.model.JobDependencyCounts getDependencyCounts(String jobId);

    /**
     * Retrieves detailed dependency breakdown (unprocessed child keys, processed results,
     * and failed reasons) for a parent job in a workflow DAG.
     * Mirrors BullMQ's job.getDependencies().
     *
     * @param jobId parent job identifier
     * @return JobDependencies record containing child keys and results
     */
    default io.oxmq.model.JobDependencies getDependencies(String jobId) {
        return new io.oxmq.model.JobDependencies(jobId, java.util.List.of(), java.util.Map.of(), java.util.Map.of());
    }

    /**
     * Retrieves the hierarchical workflow execution tree starting from this job as root.
     * Mirrors BullMQ's FlowProducer.getFlow().
     *
     * @param jobId parent/root job identifier
     * @return populated JobNode tree with children dependencies, or null
     */
    default io.oxmq.model.JobNode getFlow(String jobId) {
        return getFlow(jobId, 10);
    }

    /**
     * Retrieves the hierarchical workflow execution tree with a maximum recursion depth.
     *
     * @param jobId parent/root job identifier
     * @param maxDepth maximum recursion depth
     * @return populated JobNode tree with children dependencies, or null
     */
    default io.oxmq.model.JobNode getFlow(String jobId, int maxDepth) {
        return null;
    }

    /**
     * Removes a child dependency from its parent job.
     */
    boolean removeChildDependency(String jobId, String parentKey);

    /**
     * Removes unstarted children of a parent job.
     */
    void removeUnprocessedChildren(String jobId);

    /**
     * Retrieves recurring job scheduler metadata and next execution timestamp.
     */
    java.util.Map<String, Object> getJobScheduler(String schedulerId);

    /**
     * Gets the total count of registered recurring job schedulers.
     */
    long getJobSchedulersCount();

    /**
     * Checks if the queue has reached its concurrency limit.
     */
    boolean isMaxed();

    /**
     * Removes orphaned job keys not referenced in any active or completed state set.
     */
    long removeOrphanedJobs(List<String> candidateJobIds);

    /**
     * Retrieves the underlying storage backend.
     */
    io.oxmq.backend.QueueBackend getBackend();

    /**
     * Gets the lifecycle state of a job as a typed {@link JobState} enum.
     */
    JobState getJobState(String jobId);

    /**
     * Checks if a job has completed.
     */
    boolean isCompleted(String jobId);

    /**
     * Checks if a job has permanently failed.
     */
    boolean isFailed(String jobId);

    /**
     * Checks whether a job has finished (completed or failed) via the isFinished Lua script.
     */
    List<Object> isFinished(String jobId, boolean fetchValue);

    /**
     * Waits for a job to complete or fail, returning its result or throwing on failure.
     */
    Object waitUntilFinished(String jobId, java.time.Duration timeout) throws Exception;

    /**
     * Waits for a job to complete or fail using a custom QueueEvents listener.
     */
    Object waitUntilFinished(String jobId, QueueEvents queueEvents, java.time.Duration timeout) throws Exception;

    /**
     * Asynchronously waits for a job to complete or fail.
     */
    java.util.concurrent.CompletableFuture<Object> waitUntilFinishedAsync(String jobId);

    /**
     * Asynchronously waits for a job to complete or fail using a custom QueueEvents listener.
     */
    java.util.concurrent.CompletableFuture<Object> waitUntilFinishedAsync(String jobId, QueueEvents queueEvents);

    /**
     * Retrieves jobs for given states with range pagination and ordering.
     * Mirrors BullMQ's queue.getJobs(types, start, end, asc).
     */
    List<Job<T>> getJobs(List<JobState> states, int start, int end, boolean asc);

    /**
     * Retrieves waiting jobs within the specified range [start, end].
     */
    default List<Job<T>> getWaiting(int start, int end) {
        return getJobs(List.of(JobState.WAITING), start, end, true);
    }

    /**
     * Retrieves active jobs within the specified range [start, end].
     */
    default List<Job<T>> getActive(int start, int end) {
        return getJobs(List.of(JobState.ACTIVE), start, end, true);
    }

    /**
     * Retrieves delayed jobs within the specified range [start, end].
     */
    default List<Job<T>> getDelayed(int start, int end) {
        return getJobs(List.of(JobState.DELAYED), start, end, true);
    }

    /**
     * Retrieves completed jobs within the specified range [start, end].
     */
    default List<Job<T>> getCompleted(int start, int end) {
        return getJobs(List.of(JobState.COMPLETED), start, end, false);
    }

    /**
     * Retrieves failed jobs within the specified range [start, end].
     */
    default List<Job<T>> getFailed(int start, int end) {
        return getJobs(List.of(JobState.FAILED), start, end, false);
    }

    /**
     * Retrieves jobs waiting for children within the specified range [start, end].
     */
    default List<Job<T>> getWaitingChildren(int start, int end) {
        return getJobs(List.of(JobState.WAITING_CHILDREN), start, end, true);
    }

    /**
     * Retrieves prioritized jobs within the specified range [start, end].
     */
    default List<Job<T>> getPrioritized(int start, int end) {
        return getJobs(List.of(JobState.PRIORITIZED), start, end, true);
    }

    /**
     * Retrieves paused jobs within the specified range [start, end].
     */
    default List<Job<T>> getPaused(int start, int end) {
        return getJobs(List.of(JobState.PAUSED), start, end, true);
    }

    /**
     * Gets job counts breakdown for the specified states.
     */
    java.util.Map<JobState, Long> getJobCounts(JobState... states);

    /**
     * Retrieves the remaining time-to-live (in ms) of the queue rate limiter.
     */
    Long getRateLimitTtl();

    /**
     * Sets the global concurrency limit for this queue across all workers in the cluster.
     * Enforced atomically by BullMQ's {@code moveToActive-11.lua}.
     *
     * @param concurrency maximum concurrent active jobs across the cluster
     */
    void setGlobalConcurrency(int concurrency);

    /**
     * Retrieves the global concurrency limit configured on this queue, or null if not set.
     */
    Integer getGlobalConcurrency();

    /**
     * Removes the global concurrency limit from this queue.
     *
     * @return true if the limit was removed, false if none was set
     */
    boolean removeGlobalConcurrency();

    /**
     * Sets the global rate limit for this queue across all workers in the cluster.
     * Enforced atomically by BullMQ's {@code moveToActive-11.lua}.
     *
     * @param max      maximum jobs processed per duration window
     * @param duration time window duration
     */
    default void setGlobalRateLimit(int max, java.time.Duration duration) {
        if (duration == null) {
            throw new IllegalArgumentException("duration must not be null");
        }
        setGlobalRateLimit(max, duration.toMillis());
    }

    /**
     * Sets the global rate limit for this queue across all workers in the cluster.
     * Enforced atomically by BullMQ's {@code moveToActive-11.lua}.
     *
     * @param max            maximum jobs processed per duration window
     * @param durationMillis time window duration in milliseconds
     */
    void setGlobalRateLimit(int max, long durationMillis);

    /**
     * Retrieves the global rate limit settings configured on this queue, or null if not set.
     */
    io.oxmq.model.GlobalRateLimit getGlobalRateLimit();

    /**
     * Removes the global rate limit settings from this queue.
     *
     * @return true if rate limit was removed, false if none was set
     */
    boolean removeGlobalRateLimit();

    /**
     * Removes the active rate limit token bucket key from Redis.
     *
     * @return true if the rate limit key was deleted, false if absent
     */
    boolean removeRateLimitKey();

    /**
     * Pauses processing for a virtual group.
     *
     * @param groupId virtual group identifier
     * @return true if group was paused, false if already paused
     */
    default boolean pauseGroup(String groupId) {
        return false;
    }

    /**
     * Resumes processing for a paused virtual group.
     *
     * @param groupId virtual group identifier
     * @return true if group was resumed, false if not paused
     */
    default boolean resumeGroup(String groupId) {
        return false;
    }

    /**
     * Deletes all pending jobs in a virtual group and cleans up group indices.
     *
     * @param groupId virtual group identifier
     * @return number of deleted pending jobs
     */
    default long deleteGroup(String groupId) {
        return 0;
    }

    /**
     * Applies a temporary rate-limit / throttling window to a virtual group.
     *
     * @param groupId virtual group identifier
     * @param duration duration of the rate limit
     * @return true if rate limit was applied
     */
    default boolean setGroupRateLimit(String groupId, java.time.Duration duration) {
        return false;
    }

    /**
     * Returns the remaining time-to-live (in milliseconds) for a rate-limited virtual group.
     * Returns 0 if the group is not currently rate-limited.
     *
     * @param groupId virtual group identifier
     * @return remaining TTL in milliseconds, or 0 if not limited
     */
    default long getGroupRateLimitTtl(String groupId) {
        return 0;
    }

    /**
     * Retrieves unique virtual group identifiers with pagination.
     *
     * @param start starting index (0-based)
     * @param stop ending index (inclusive, or -1 for all)
     * @return list of group IDs
     */
    default List<String> getGroups(long start, long stop) {
        return List.of();
    }

    /**
     * Retrieves all unique virtual group identifiers in this queue.
     *
     * @return list of all group IDs
     */
    default List<String> getGroups() {
        return getGroups(0, -1);
    }

    /**
     * Retrieves a breakdown map of pending job counts across groups.
     *
     * @param limit maximum number of groups to inspect
     * @return map of groupId to total pending job count
     */
    default java.util.Map<String, Long> getGroupsJobsCount(int limit) {
        return java.util.Map.of();
    }

    /**
     * Retrieves a breakdown map of pending job counts across all groups (default limit 1000).
     *
     * @return map of groupId to total pending job count
     */
    default java.util.Map<String, Long> getGroupsJobsCount() {
        return getGroupsJobsCount(1000);
    }

    /**
     * Self-heals and repairs a desynchronized virtual group stuck in the maxed state
     * by reconciling active counts against actual running jobs in the active set.
     *
     * @param groupId virtual group identifier
     * @return true if group was repaired
     */
    default boolean repairGroup(String groupId) {
        return false;
    }

    /**
     * Audits and repairs all groups currently in the maxed state.
     *
     * @return number of groups repaired
     */
    default int repairMaxedGroups() {
        return 0;
    }

    /**
     * Retrieves virtual groups by status ('waiting', 'limited', 'maxed', 'paused') with pagination.
     */
    default List<io.oxmq.lua.ExtensionScripts.GroupEntry> getGroupsByStatus(String status, long start, long stop) {
        return List.of();
    }

    /**
     * Retrieves paginated jobs belonging to a virtual group.
     */
    default List<Job<T>> getGroupJobs(String groupId, long start, long stop) {
        return List.of();
    }

    /**
     * Returns total pending jobs in a virtual group.
     */
    default long getGroupJobsCount(String groupId) {
        return 0;
    }

    /**
     * Returns active (currently processing) jobs count for a virtual group.
     */
    default long getGroupActiveCount(String groupId) {
        return 0;
    }

    /**
     * Returns job counts breakdown per priority for a specific virtual group.
     * Priority 0 corresponds to non-prioritized FIFO jobs.
     *
     * @param groupId virtual group identifier
     * @param priorities list of priorities to inspect
     * @return map of priority to job count
     */
    default java.util.Map<Integer, Long> getCountsPerPriorityForGroup(String groupId, java.util.List<Integer> priorities) {
        return java.util.Map.of();
    }

    /**
     * Sets a dynamic concurrency limit override for a specific virtual group.
     * Overrides the worker's default group concurrency for this group in Redis.
     *
     * @param groupId virtual group identifier
     * @param concurrency maximum concurrent active jobs for this group
     */
    default void setGroupConcurrency(String groupId, int concurrency) {
    }

    /**
     * Retrieves the specific concurrency limit override for a virtual group, or null if using default.
     *
     * @param groupId virtual group identifier
     * @return configured concurrency override or null
     */
    default Integer getGroupConcurrency(String groupId) {
        return null;
    }

    /**
     * Removes the specific concurrency limit override for a virtual group.
     *
     * @param groupId virtual group identifier
     * @return true if an override was removed
     */
    default boolean removeGroupConcurrency(String groupId) {
        return false;
    }

    /**
     * Cancels an active or waiting job cluster-wide.
     * If the job is active on any worker node in the cluster, sends a cancellation signal
     * via Redis control channel to immediately interrupt its virtual thread and cancel its token.
     *
     * @param jobId job identifier
     * @return true if cancellation request was dispatched
     */
    default boolean cancelJob(String jobId) {
        return cancelJob(jobId, "User requested cancellation");
    }

    /**
     * Cancels an active or waiting job cluster-wide with a specified reason.
     *
     * @param jobId job identifier
     * @param reason human-readable cancellation reason
     * @return true if cancellation request was dispatched
     */
    default boolean cancelJob(String jobId, String reason) {
        return false;
    }

    @Override
    void close();
}
