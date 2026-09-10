package io.oxmq.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.time.Duration;

/**
 * Strategy for calculating retry delay on job failure.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = BackoffStrategy.Fixed.class, name = "fixed"),
    @JsonSubTypes.Type(value = BackoffStrategy.Exponential.class, name = "exponential")
})
public interface BackoffStrategy {

    /**
     * Calculates the retry delay in milliseconds for the given attempt.
     *
     * @param attemptsMade number of attempts already made (1-based)
     * @return delay in milliseconds
     */
    long calculateDelayMs(int attemptsMade);

    /**
     * Fixed backoff with constant delay.
     */
    record Fixed(@JsonProperty("delay") long delayMs) implements BackoffStrategy {
        public Fixed(Duration duration) {
            this(duration.toMillis());
        }

        @Override
        public long calculateDelayMs(int attemptsMade) {
            return Math.max(0, delayMs);
        }
    }

    /**
     * Exponential backoff: delay = initialDelayMs * 2^(attemptsMade - 1), capped at maxDelayMs.
     */
    record Exponential(@JsonProperty("delay") long initialDelayMs,
                        @JsonProperty("maxDelay") @JsonAlias("maxDelayMs") long maxDelayMs) implements BackoffStrategy {
        public Exponential(long initialDelayMs) {
            this(initialDelayMs, 3600_000L);
        }
        public Exponential(Duration initialDuration) {
            this(initialDuration.toMillis(), 3600_000L); // default 1 hr max
        }

        public Exponential(Duration initialDuration, Duration maxDuration) {
            this(initialDuration.toMillis(), maxDuration.toMillis());
        }

        @Override
        public long calculateDelayMs(int attemptsMade) {
            int exponent = Math.max(0, attemptsMade - 1);
            if (exponent >= 30) {
                return maxDelayMs;
            }
            long delay = initialDelayMs * (1L << exponent);
            return Math.min(delay, maxDelayMs);
        }
    }

    static BackoffStrategy fixed(Duration duration) {
        return new Fixed(duration);
    }

    static BackoffStrategy fixed(long delayMs) {
        return new Fixed(delayMs);
    }

    static BackoffStrategy exponential(Duration initialDuration) {
        return new Exponential(initialDuration);
    }

    static BackoffStrategy exponential(Duration initialDuration, Duration maxDuration) {
        return new Exponential(initialDuration, maxDuration);
    }

    static BackoffStrategy exponential(long initialDelayMs, long maxDelayMs) {
        return new Exponential(initialDelayMs, maxDelayMs);
    }
}
