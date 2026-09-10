package io.oxmq.exception;

/**
 * Exception that immediately aborts job retries and moves the job directly to failed state,
 * matching BullMQ's UnrecoverableError behavior.
 */
public class UnrecoverableError extends RuntimeException {

    public UnrecoverableError(String message) {
        super(message);
    }

    public UnrecoverableError(String message, Throwable cause) {
        super(message, cause);
    }

    public UnrecoverableError(Throwable cause) {
        super(cause);
    }
}
