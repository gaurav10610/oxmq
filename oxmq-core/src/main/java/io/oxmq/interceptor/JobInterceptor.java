package io.oxmq.interceptor;

import io.oxmq.model.Job;
import java.time.Duration;

/**
 * Service Provider Interface (SPI) for intercepting background job execution in OxMQ workers.
 * <p>
 * Allows developing non-intrusive middleware such as multi-tenant group limiters,
 * external API 429 circuit breakers, dynamic quota governors, audit loggers,
 * and distributed tracing without modifying core queue scripts or Lua routines.
 */
public interface JobInterceptor {

    /**
     * Invoked immediately before a job is dispatched to its {@link io.oxmq.JobProcessor}.
     *
     * @param job the job about to be processed
     * @return decision whether to proceed, delay (re-queue with skipAttempt), or reject
     */
    default InterceptorDecision beforeProcess(Job<?> job) {
        return InterceptorDecision.proceed();
    }

    /**
     * Invoked immediately after a job processor completes successfully.
     *
     * @param job the completed job
     * @param result the returned value from processor
     * @param duration total execution duration
     */
    default void afterProcess(Job<?> job, Object result, Duration duration) {}

    /**
     * Invoked when an unhandled exception or error occurs during job execution.
     *
     * @param job the job that encountered an error
     * @param error the caught exception or error
     * @param duration execution duration until failure
     * @return decision on how to handle the error (bubble up, delay, or trigger cooldown)
     */
    default ErrorDecision onError(Job<?> job, Throwable error, Duration duration) {
        return ErrorDecision.bubbleUp();
    }
}
