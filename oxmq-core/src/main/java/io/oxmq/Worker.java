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
     * Gracefully stops the worker, waiting for active jobs to complete.
     */
    @Override
    void close();
}
