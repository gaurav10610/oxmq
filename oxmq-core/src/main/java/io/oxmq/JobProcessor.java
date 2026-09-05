package io.oxmq;

import io.oxmq.model.Job;

/**
 * Functional interface for processing OxMQ background jobs.
 *
 * @param <T> Payload data type
 * @param <R> Return value type
 */
@FunctionalInterface
public interface JobProcessor<T, R> {

    /**
     * Executes the background job logic.
     *
     * @param job Job instance containing metadata, payload, and progress/log handles
     * @return Execution return value (will be serialized and stored in Redis)
     * @throws Exception Any exception triggering retries or failure transitions
     */
    R process(Job<T> job) throws Exception;
}
