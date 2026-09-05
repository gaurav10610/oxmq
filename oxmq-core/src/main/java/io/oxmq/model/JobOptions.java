package io.oxmq.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Duration;

/**
 * Configuration options for job execution, retries, delays, and retention.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobOptions {

    private String jobId;
    private long delayMs = 0;
    private int attempts = 1;
    private BackoffStrategy backoff;
    private boolean removeOnComplete = false;
    private boolean removeOnFail = false;
    private int priority = 0;
    private String parentKey;

    public JobOptions() {}

    public static Builder builder() {
        return new Builder();
    }

    public static JobOptions defaults() {
        return new JobOptions();
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public long getDelayMs() {
        return delayMs;
    }

    public void setDelayMs(long delayMs) {
        this.delayMs = delayMs;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public BackoffStrategy getBackoff() {
        return backoff;
    }

    public void setBackoff(BackoffStrategy backoff) {
        this.backoff = backoff;
    }

    public boolean isRemoveOnComplete() {
        return removeOnComplete;
    }

    public void setRemoveOnComplete(boolean removeOnComplete) {
        this.removeOnComplete = removeOnComplete;
    }

    public boolean isRemoveOnFail() {
        return removeOnFail;
    }

    public void setRemoveOnFail(boolean removeOnFail) {
        this.removeOnFail = removeOnFail;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public String getParentKey() {
        return parentKey;
    }

    public void setParentKey(String parentKey) {
        this.parentKey = parentKey;
    }

    public static class Builder {
        private final JobOptions options = new JobOptions();

        public Builder jobId(String jobId) {
            options.setJobId(jobId);
            return this;
        }

        public Builder delay(Duration duration) {
            options.setDelayMs(duration.toMillis());
            return this;
        }

        public Builder delayMs(long delayMs) {
            options.setDelayMs(delayMs);
            return this;
        }

        public Builder attempts(int attempts) {
            options.setAttempts(Math.max(1, attempts));
            return this;
        }

        public Builder fixedBackoff(Duration duration) {
            options.setBackoff(BackoffStrategy.fixed(duration));
            return this;
        }

        public Builder fixedBackoff(long delayMs) {
            options.setBackoff(BackoffStrategy.fixed(delayMs));
            return this;
        }

        public Builder exponentialBackoff(Duration initialDuration) {
            options.setBackoff(BackoffStrategy.exponential(initialDuration));
            return this;
        }

        public Builder exponentialBackoff(Duration initialDuration, Duration maxDuration) {
            options.setBackoff(BackoffStrategy.exponential(initialDuration, maxDuration));
            return this;
        }

        public Builder exponentialBackoff(long initialDelayMs, long maxDelayMs) {
            options.setBackoff(BackoffStrategy.exponential(initialDelayMs, maxDelayMs));
            return this;
        }

        public Builder backoff(BackoffStrategy backoff) {
            options.setBackoff(backoff);
            return this;
        }

        public Builder removeOnComplete(boolean removeOnComplete) {
            options.setRemoveOnComplete(removeOnComplete);
            return this;
        }

        public Builder removeOnFail(boolean removeOnFail) {
            options.setRemoveOnFail(removeOnFail);
            return this;
        }

        public Builder priority(int priority) {
            options.setPriority(priority);
            return this;
        }

        public Builder parentKey(String parentKey) {
            options.setParentKey(parentKey);
            return this;
        }

        public JobOptions build() {
            return options;
        }
    }
}
