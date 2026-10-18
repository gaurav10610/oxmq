package io.oxmq.exception;

/**
 * Thrown when a job is rejected by an interceptor before execution.
 */
public class JobRejectedException extends RuntimeException {

    public JobRejectedException(String message) {
        super(message);
    }

    public JobRejectedException(String message, Throwable cause) {
        super(message, cause);
    }
}
