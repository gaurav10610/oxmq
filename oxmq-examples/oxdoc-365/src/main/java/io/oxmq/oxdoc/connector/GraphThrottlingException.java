package io.oxmq.oxdoc.connector;

import io.oxmq.exception.RateLimitCooldownException;
import java.time.Duration;

/**
 * Thrown when Microsoft Graph returns HTTP 429 Too Many Requests or HTTP 503.
 * Seamlessly integrates with OxMQ RateLimitSentinelPlugin to trigger distributed cooldown.
 */
public class GraphThrottlingException extends RateLimitCooldownException {

    private final long retryAfterSeconds;

    public GraphThrottlingException(long retryAfterSeconds, String tenantId, String message) {
        super(Duration.ofSeconds(Math.max(1, retryAfterSeconds)), tenantId, message);
        this.retryAfterSeconds = Math.max(1, retryAfterSeconds);
    }

    public GraphThrottlingException(long retryAfterSeconds, String message) {
        this(retryAfterSeconds, "tenant-mid-2500", message);
    }

    public long getRetryAfterSeconds() {
        return retryAfterSeconds;
    }

    public long getRetryAfterMillis() {
        return retryAfterSeconds * 1000L;
    }
}
