package io.oxmq.exception;

import java.time.Duration;

/**
 * Thrown when an external upstream service responds with HTTP 429 Too Many Requests
 * or HTTP 503 Service Unavailable, signaling that a rate-limit cooldown must be enforced
 * for a specific tenant or group key across all distributed workers.
 */
public class RateLimitCooldownException extends RuntimeException {

    private final Duration cooldownDuration;
    private final String groupKey;

    public RateLimitCooldownException(Duration cooldownDuration, String groupKey) {
        super("Rate limit cooldown triggered for group [" + groupKey + "] duration: " + (cooldownDuration != null ? cooldownDuration.toMillis() + "ms" : "default"));
        this.cooldownDuration = cooldownDuration != null ? cooldownDuration : Duration.ofSeconds(3);
        this.groupKey = groupKey;
    }

    public RateLimitCooldownException(Duration cooldownDuration, String groupKey, String message) {
        super(message);
        this.cooldownDuration = cooldownDuration != null ? cooldownDuration : Duration.ofSeconds(3);
        this.groupKey = groupKey;
    }

    public Duration getCooldownDuration() {
        return cooldownDuration;
    }

    public String getGroupKey() {
        return groupKey;
    }
}
