package io.oxmq.exception;

import java.time.Duration;

/**
 * Thrown when a running job exceeds its configured execution timeout ({@code jobTimeout} / {@code ttl}).
 * <p>
 * Extends {@link JobCancelledException} to participate in cancellation handling and cooperative interruption.
 */
public class JobTimeoutException extends JobCancelledException {

    private final Duration timeout;

    public JobTimeoutException(Duration timeout) {
        super("Job execution timed out after " + (timeout != null ? timeout.toMillis() : 0) + " ms");
        this.timeout = timeout;
    }

    public JobTimeoutException(String message, Duration timeout) {
        super(message);
        this.timeout = timeout;
    }

    public Duration getTimeout() {
        return timeout;
    }
}
