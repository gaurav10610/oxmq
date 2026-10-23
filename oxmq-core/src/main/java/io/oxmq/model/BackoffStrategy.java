package io.oxmq.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.time.Duration;

/**
 * Strategy for calculating retry delay on job failure.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = BackoffStrategy.Custom.class)
@JsonSubTypes({
    @JsonSubTypes.Type(value = BackoffStrategy.Fixed.class, name = "fixed"),
    @JsonSubTypes.Type(value = BackoffStrategy.Exponential.class, name = "exponential"),
    @JsonSubTypes.Type(value = BackoffStrategy.Custom.class, name = "custom")
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
     * Pluggable named custom backoff strategy resolved at runtime via the Worker's backoff registry.
     */
    @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
    record Custom(@JsonProperty("type") @com.fasterxml.jackson.annotation.JsonAlias("name") String name) implements BackoffStrategy {
        public Custom {
            if (name == null || name.isBlank()) {
                name = "custom";
            }
        }

        @Override
        public long calculateDelayMs(int attemptsMade) {
            return 0; // Evaluated dynamically by the worker's registered backoff strategy function
        }
    }

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

    static BackoffStrategy custom(String name) {
        return new Custom(name);
    }
}
