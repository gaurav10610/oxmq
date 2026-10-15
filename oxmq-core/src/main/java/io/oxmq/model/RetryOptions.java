package io.oxmq.model;

/**
 * Options for retrying a failed or completed job.
 * Conforms to BullMQ {@code RetryOptions}.
 */
public class RetryOptions {

    private final boolean resetAttemptsMade;
    private final boolean resetAttemptsStarted;

    public RetryOptions(boolean resetAttemptsMade, boolean resetAttemptsStarted) {
        this.resetAttemptsMade = resetAttemptsMade;
        this.resetAttemptsStarted = resetAttemptsStarted;
    }

    public static RetryOptions defaults() {
        return new RetryOptions(false, false);
    }

    public static RetryOptions resetAttempts() {
        return new RetryOptions(true, true);
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean isResetAttemptsMade() {
        return resetAttemptsMade;
    }

    public boolean isResetAttemptsStarted() {
        return resetAttemptsStarted;
    }

    public static class Builder {
        private boolean resetAttemptsMade = false;
        private boolean resetAttemptsStarted = false;

        public Builder resetAttemptsMade(boolean resetAttemptsMade) {
            this.resetAttemptsMade = resetAttemptsMade;
            return this;
        }

        public Builder resetAttemptsStarted(boolean resetAttemptsStarted) {
            this.resetAttemptsStarted = resetAttemptsStarted;
            return this;
        }

        public RetryOptions build() {
            return new RetryOptions(resetAttemptsMade, resetAttemptsStarted);
        }
    }
}
