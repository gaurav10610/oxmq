package io.oxmq;

import java.io.Closeable;

/**
 * Worker interface for consuming and executing background jobs.
 *
 * @param <T> Payload data type
 */
public interface Worker<T> extends Closeable {

    /**
     * Starts the worker polling and execution loop.
     */
    void start();

    /**
     * Pauses the worker from acquiring new jobs.
     */
    void pause();

    /**
     * Resumes job acquisition.
     */
    void resume();

    /**
     * Checks if the worker is actively running.
     */
    boolean isRunning();

    /**
     * Checks if the worker is currently paused.
     */
    boolean isPaused();

    /**
     * Cancels an actively executing job on this worker, signaling cooperative interruption.
     *
     * @param jobId the ID of the job to cancel
     * @return true if the job was running on this worker and cancelled, false otherwise
     */
    default boolean cancelJob(String jobId) {
        return cancelJob(jobId, "User requested cancellation");
    }

    /**
     * Cancels an actively executing job on this worker with a custom cancellation reason.
     *
     * @param jobId the ID of the job to cancel
     * @param reason the cancellation reason
     * @return true if the job was running on this worker and cancelled, false otherwise
     */
    default boolean cancelJob(String jobId, String reason) {
        return false;
    }

    /**
     * Cancels all actively executing jobs on this worker.
     *
     * @return number of jobs cancelled
     */
    default int cancelAllJobs() {
        return cancelAllJobs("Worker cancellation requested");
    }

    /**
     * Cancels all actively executing jobs on this worker with a custom reason.
     *
     * @param reason the cancellation reason
     * @return number of jobs cancelled
     */
    default int cancelAllJobs(String reason) {
        return 0;
    }

    /**
     * Checks if a job with the given ID is currently running on this worker.
     */
    default boolean isJobActive(String jobId) {
        return false;
    }

    /**
     * Returns the count of jobs currently executing on this worker.
     */
    default int getActiveJobCount() {
        return 0;
    }

    /**
     * Gracefully stops the worker, waiting for active jobs to complete.
     */
    @Override
    void close();
}
