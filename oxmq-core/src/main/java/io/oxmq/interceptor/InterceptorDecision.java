package io.oxmq.interceptor;

/**
 * Result returned by {@link JobInterceptor#beforeProcess(io.oxmq.model.Job)}.
 * Specifies whether to proceed with execution, delay the job, or reject it.
 *
 * @param action the interceptor action
 * @param delayMs delay in milliseconds if action is DELAY
 * @param skipAttempt whether to skip incrementing attemptsMade when delayed
 * @param reason rejection reason if action is REJECT
 */
public record InterceptorDecision(Action action, long delayMs, boolean skipAttempt, String reason) {

    public enum Action {
        PROCEED,
        DELAY,
        REJECT
    }

    public static InterceptorDecision proceed() {
        return new InterceptorDecision(Action.PROCEED, 0, false, null);
    }

    public static InterceptorDecision delay(long delayMs) {
        return delay(delayMs, true);
    }

    public static InterceptorDecision delay(long delayMs, boolean skipAttempt) {
        return new InterceptorDecision(Action.DELAY, Math.max(1, delayMs), skipAttempt, null);
    }

    public static InterceptorDecision reject(String reason) {
        return new InterceptorDecision(Action.REJECT, 0, false, reason);
    }

    public boolean isProceed() {
        return action == Action.PROCEED;
    }

    public boolean isDelay() {
        return action == Action.DELAY;
    }

    public boolean isReject() {
        return action == Action.REJECT;
    }
}
