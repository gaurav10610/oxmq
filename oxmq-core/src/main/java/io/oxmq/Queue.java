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
     * Clears a deduplication key, allowing a new job with the same deduplication ID to be added.
     */
    boolean removeDeduplicationKey(String deduplicationId);

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

    @Override
    void close();
}
