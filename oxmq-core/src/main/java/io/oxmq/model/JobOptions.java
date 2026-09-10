package io.oxmq.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration options for job execution, retries, delays, retention, deduplication, and workflows.
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
    private boolean lifo = false;
    private String deduplicationId;
    private long deduplicationTtlMs = 0;
    private int keepLogs = 0;
    private boolean failParentOnFailure = false;
    private boolean removeDependencyOnFailure = false;
    private boolean continueParentOnFailure = false;
    private boolean ignoreDependencyOnFailure = false;
    private String groupKey;

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

    public boolean isLifo() {
        return lifo;
    }

    public void setLifo(boolean lifo) {
        this.lifo = lifo;
    }

    public String getDeduplicationId() {
        return deduplicationId;
    }

    public void setDeduplicationId(String deduplicationId) {
        this.deduplicationId = deduplicationId;
    }

    public long getDeduplicationTtlMs() {
        return deduplicationTtlMs;
    }

    public void setDeduplicationTtlMs(long deduplicationTtlMs) {
        this.deduplicationTtlMs = deduplicationTtlMs;
    }

    public int getKeepLogs() {
        return keepLogs;
    }

    public void setKeepLogs(int keepLogs) {
        this.keepLogs = keepLogs;
    }

    public boolean isFailParentOnFailure() {
        return failParentOnFailure;
    }

    public void setFailParentOnFailure(boolean failParentOnFailure) {
        this.failParentOnFailure = failParentOnFailure;
    }

    public boolean isRemoveDependencyOnFailure() {
        return removeDependencyOnFailure;
    }

    public void setRemoveDependencyOnFailure(boolean removeDependencyOnFailure) {
        this.removeDependencyOnFailure = removeDependencyOnFailure;
    }

    public boolean isContinueParentOnFailure() {
        return continueParentOnFailure;
    }

    public void setContinueParentOnFailure(boolean continueParentOnFailure) {
        this.continueParentOnFailure = continueParentOnFailure;
    }

    public boolean isIgnoreDependencyOnFailure() {
        return ignoreDependencyOnFailure;
    }

    public void setIgnoreDependencyOnFailure(boolean ignoreDependencyOnFailure) {
        this.ignoreDependencyOnFailure = ignoreDependencyOnFailure;
    }

    public String getGroupKey() {
        return groupKey;
    }

    public void setGroupKey(String groupKey) {
        this.groupKey = groupKey;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        if (jobId != null) map.put("jobId", jobId);
        map.put("delay", delayMs);
        map.put("attempts", attempts);
        map.put("removeOnComplete", removeOnComplete);
        map.put("removeOnFail", removeOnFail);
        map.put("priority", priority);
        map.put("lifo", lifo);
        if (parentKey != null) map.put("parentKey", parentKey);
        if (deduplicationId != null && !deduplicationId.isBlank()) {
            map.put("deduplicationId", deduplicationId);
            if (deduplicationTtlMs > 0) {
                map.put("deduplication", Map.of("id", deduplicationId, "ttl", deduplicationTtlMs));
            } else {
                map.put("deduplication", Map.of("id", deduplicationId));
            }
        }
        if (keepLogs > 0) map.put("keepLogs", keepLogs);
        if (failParentOnFailure) map.put("failParentOnFailure", true);
        if (removeDependencyOnFailure) map.put("removeDependencyOnFailure", true);
        if (continueParentOnFailure) map.put("continueParentOnFailure", true);
        if (ignoreDependencyOnFailure) map.put("ignoreDependencyOnFailure", true);
        if (groupKey != null) map.put("groupKey", groupKey);
        if (backoff != null) {
            if (backoff instanceof BackoffStrategy.Exponential exp) {
                map.put("backoff", Map.of("type", "exponential", "delay", exp.initialDelayMs()));
            } else if (backoff instanceof BackoffStrategy.Fixed fixed) {
                map.put("backoff", Map.of("type", "fixed", "delay", fixed.delayMs()));
            } else {
                map.put("backoff", Map.of("type", "fixed", "delay", backoff.calculateDelayMs(1)));
            }
        }
        return map;
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

        public Builder lifo(boolean lifo) {
            options.setLifo(lifo);
            return this;
        }

        public Builder deduplication(String deduplicationId) {
            options.setDeduplicationId(deduplicationId);
            return this;
        }

        public Builder deduplicationId(String deduplicationId) {
            options.setDeduplicationId(deduplicationId);
            return this;
        }

        public Builder deduplication(String deduplicationId, Duration ttl) {
            options.setDeduplicationId(deduplicationId);
            options.setDeduplicationTtlMs(ttl.toMillis());
            return this;
        }

        public Builder deduplicationTtl(Duration ttl) {
            options.setDeduplicationTtlMs(ttl.toMillis());
            return this;
        }

        public Builder keepLogs(int keepLogs) {
            options.setKeepLogs(keepLogs);
            return this;
        }

        public Builder failParentOnFailure(boolean failParentOnFailure) {
            options.setFailParentOnFailure(failParentOnFailure);
            return this;
        }

        public Builder removeDependencyOnFailure(boolean removeDependencyOnFailure) {
            options.setRemoveDependencyOnFailure(removeDependencyOnFailure);
            return this;
        }

        public Builder continueParentOnFailure(boolean continueParentOnFailure) {
            options.setContinueParentOnFailure(continueParentOnFailure);
            return this;
        }

        public Builder ignoreDependencyOnFailure(boolean ignoreDependencyOnFailure) {
            options.setIgnoreDependencyOnFailure(ignoreDependencyOnFailure);
            return this;
        }

        public Builder groupKey(String groupKey) {
            options.setGroupKey(groupKey);
            return this;
        }

        public JobOptions build() {
            return options;
        }
    }
}
