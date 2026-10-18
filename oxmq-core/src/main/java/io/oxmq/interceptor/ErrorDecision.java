package io.oxmq.interceptor;

import java.time.Duration;

/**
 * Result returned by {@link JobInterceptor#onError(io.oxmq.model.Job, Throwable, Duration)}.
 * Allows plugins to intercept exceptions (such as HTTP 429 rate limit triggers)
 * and trigger cooldown or delay before normal failure routing kicks in.
 *
 * @param action the error handling action
 * @param delayMs delay in milliseconds if action is DELAY or COOLDOWN
 * @param skipAttempt whether to skip incrementing attemptsMade when delayed
 * @param groupKey the associated rate-limit group key if applicable
 */
public record ErrorDecision(Action action, long delayMs, boolean skipAttempt, String groupKey) {

    public enum Action {
        BUBBLE_UP,
        DELAY,
        COOLDOWN
    }

    public static ErrorDecision bubbleUp() {
        return new ErrorDecision(Action.BUBBLE_UP, 0, false, null);
    }

    public static ErrorDecision delay(long delayMs, boolean skipAttempt) {
        return new ErrorDecision(Action.DELAY, Math.max(1, delayMs), skipAttempt, null);
    }

    public static ErrorDecision cooldown(Duration duration, String groupKey) {
        return new ErrorDecision(Action.COOLDOWN, duration != null ? duration.toMillis() : 1000, true, groupKey);
    }

    public boolean isBubbleUp() {
        return action == Action.BUBBLE_UP;
    }

    public boolean isDelay() {
        return action == Action.DELAY;
    }

    public boolean isCooldown() {
        return action == Action.COOLDOWN;
    }
}
