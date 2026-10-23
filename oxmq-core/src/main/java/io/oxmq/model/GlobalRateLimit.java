package io.oxmq.model;

import java.time.Duration;

/**
 * Immutable record representing global queue-level rate limiting settings configured in Redis.
 * Corresponds to BullMQ's {@code setGlobalRateLimit(max, duration)} stored in {@code bull:<queue>:meta}.
 *
 * @param max            maximum number of jobs allowed to be processed within the duration window
 * @param durationMillis duration of the rate limit sliding window in milliseconds
 */
public record GlobalRateLimit(int max, long durationMillis) {

    public GlobalRateLimit {
        if (max <= 0) {
            throw new IllegalArgumentException("max must be greater than 0");
        }
        if (durationMillis <= 0) {
            throw new IllegalArgumentException("durationMillis must be greater than 0");
        }
    }

    /**
     * Creates a {@link GlobalRateLimit} with the specified maximum jobs and duration.
     */
    public static GlobalRateLimit of(int max, Duration duration) {
        if (duration == null) {
            throw new IllegalArgumentException("duration must not be null");
        }
        return new GlobalRateLimit(max, duration.toMillis());
    }

    /**
     * Returns the rate limit duration as a {@link Duration}.
     */
    public Duration duration() {
        return Duration.ofMillis(durationMillis);
    }
}
