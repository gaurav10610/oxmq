package io.oxmq.exception;

import java.time.Duration;

/**
 * Exception thrown from a job processor to postpone job execution.
 * <p>
 * Conforms to BullMQ {@code DelayedError}: moving the active job to delayed state
 * without incrementing {@code attemptsMade} (skipping failure attempt count).
 */
public class DelayedException extends RuntimeException {

    public static final String DEFAULT_MESSAGE = "oxmq:movedToDelayed";

    private final long delayMs;

    public DelayedException(long delayMs) {
        super(DEFAULT_MESSAGE);
        this.delayMs = Math.max(0, delayMs);
    }

    public DelayedException(Duration delay) {
        this(delay != null ? delay.toMillis() : 0);
    }

    public DelayedException(String message, long delayMs) {
        super(message);
        this.delayMs = Math.max(0, delayMs);
    }

    public DelayedException(String message, Duration delay) {
        this(message, delay != null ? delay.toMillis() : 0);
    }

    public long getDelayMs() {
        return delayMs;
    }
}
