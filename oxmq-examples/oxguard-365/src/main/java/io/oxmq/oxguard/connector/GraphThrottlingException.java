package io.oxmq.oxguard.connector;

/**
 * Thrown when Microsoft Graph returns HTTP 429 Too Many Requests or HTTP 503.
 */
public class GraphThrottlingException extends RuntimeException {

    private final long retryAfterSeconds;

    public GraphThrottlingException(long retryAfterSeconds, String message) {
        super(message);
        this.retryAfterSeconds = Math.max(1, retryAfterSeconds);
    }

    public long getRetryAfterSeconds() {
        return retryAfterSeconds;
    }

    public long getRetryAfterMillis() {
        return retryAfterSeconds * 1000L;
    }
}
