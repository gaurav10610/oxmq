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

    @Override
    void close();
}
