package io.oxmq.exception;

import java.time.Duration;

/**
 * Exception thrown from a job processor when encountering an external rate limit (e.g. HTTP 429).
 * <p>
 * Conforms to BullMQ {@code RateLimitError}: postpones the current job to delayed state without
 * incrementing {@code attemptsMade} (skipping failure attempt count), allowing the queue
 * to back off until the external rate limit window resets.
 */
public class RateLimitException extends DelayedException {

    public static final String DEFAULT_MESSAGE = "oxmq:rateLimitExceeded";

    public RateLimitException(long delayMs) {
        super(DEFAULT_MESSAGE, delayMs);
    }

    public RateLimitException(Duration delay) {
        super(DEFAULT_MESSAGE, delay != null ? delay.toMillis() : 0);
    }

    public RateLimitException(String message, long delayMs) {
        super(message, delayMs);
    }

    public RateLimitException(String message, Duration delay) {
        super(message, delay != null ? delay.toMillis() : 0);
    }
}
