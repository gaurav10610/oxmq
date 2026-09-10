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
     * Removes a recurring job scheduler and its next scheduled job.
     */
    boolean removeJobScheduler(String schedulerId);

    @Override
    void close();
}
