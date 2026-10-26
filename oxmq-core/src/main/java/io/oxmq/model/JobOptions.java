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
    private KeepJobs keepCompletedJobs;
    private KeepJobs keepFailedJobs;
    private int priority = 0;
    private String parentKey;
    private boolean lifo = false;
    private String deduplicationId;
    private long deduplicationTtlMs = 0;
    private boolean keepLastIfActive = false;
    private int keepLogs = 0;
    private boolean failParentOnFailure = false;
    private boolean removeDependencyOnFailure = false;
    private boolean continueParentOnFailure = false;
    private boolean ignoreDependencyOnFailure = false;
    private String groupKey;
    private GroupOptions group;
    private long jobTimeoutMs = 0;
    private String traceparent;
    private String tracestate;
    private Map<String, Object> telemetry;

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
        if (removeOnComplete) {
            this.keepCompletedJobs = KeepJobs.removeImmediately();
        } else {
            this.keepCompletedJobs = null;
        }
    }

    @com.fasterxml.jackson.annotation.JsonIgnore
    public KeepJobs getKeepCompletedJobs() {
        return keepCompletedJobs;
    }

    public void setKeepCompletedJobs(KeepJobs keepCompletedJobs) {
        this.keepCompletedJobs = keepCompletedJobs;
        if (keepCompletedJobs != null && keepCompletedJobs.getCount() == 0 && keepCompletedJobs.getAgeSeconds() == 0) {
            this.removeOnComplete = true;
        }
    }

    @com.fasterxml.jackson.annotation.JsonSetter("removeOnComplete")
    public void setRemoveOnCompleteJson(Object value) {
        if (value instanceof Boolean b) {
            setRemoveOnComplete(b);
        } else if (value instanceof Number n) {
            this.removeOnComplete = false;
            this.keepCompletedJobs = KeepJobs.count(n.intValue());
        } else if (value instanceof Map<?, ?> m) {
            this.removeOnComplete = false;
            int count = m.containsKey("count") ? ((Number) m.get("count")).intValue() : -1;
            long age = m.containsKey("age") ? ((Number) m.get("age")).longValue() : 0;
            int limit = m.containsKey("limit") ? ((Number) m.get("limit")).intValue() : 1000;
            this.keepCompletedJobs = new KeepJobs(count, age, limit);
        }
    }

    @com.fasterxml.jackson.annotation.JsonGetter("removeOnComplete")
    public Object getRemoveOnCompleteJson() {
        if (keepCompletedJobs != null && (keepCompletedJobs.getCount() > 0 || keepCompletedJobs.getAgeSeconds() > 0)) {
            return keepCompletedJobs.toMap();
        }
        return removeOnComplete;
    }

    public boolean isRemoveOnFail() {
        return removeOnFail;
    }

    public void setRemoveOnFail(boolean removeOnFail) {
        this.removeOnFail = removeOnFail;
        if (removeOnFail) {
            this.keepFailedJobs = KeepJobs.removeImmediately();
        } else {
            this.keepFailedJobs = null;
        }
    }

    @com.fasterxml.jackson.annotation.JsonIgnore
    public KeepJobs getKeepFailedJobs() {
        return keepFailedJobs;
    }

    public void setKeepFailedJobs(KeepJobs keepFailedJobs) {
        this.keepFailedJobs = keepFailedJobs;
        if (keepFailedJobs != null && keepFailedJobs.getCount() == 0 && keepFailedJobs.getAgeSeconds() == 0) {
            this.removeOnFail = true;
        }
    }

    @com.fasterxml.jackson.annotation.JsonSetter("removeOnFail")
    public void setRemoveOnFailJson(Object value) {
        if (value instanceof Boolean b) {
            setRemoveOnFail(b);
        } else if (value instanceof Number n) {
            this.removeOnFail = false;
            this.keepFailedJobs = KeepJobs.count(n.intValue());
        } else if (value instanceof Map<?, ?> m) {
            this.removeOnFail = false;
            int count = m.containsKey("count") ? ((Number) m.get("count")).intValue() : -1;
            long age = m.containsKey("age") ? ((Number) m.get("age")).longValue() : 0;
            int limit = m.containsKey("limit") ? ((Number) m.get("limit")).intValue() : 1000;
            this.keepFailedJobs = new KeepJobs(count, age, limit);
        }
    }

    @com.fasterxml.jackson.annotation.JsonGetter("removeOnFail")
    public Object getRemoveOnFailJson() {
        if (keepFailedJobs != null && (keepFailedJobs.getCount() > 0 || keepFailedJobs.getAgeSeconds() > 0)) {
            return keepFailedJobs.toMap();
        }
        return removeOnFail;
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

    public boolean isKeepLastIfActive() {
        return keepLastIfActive;
    }

    public void setKeepLastIfActive(boolean keepLastIfActive) {
        this.keepLastIfActive = keepLastIfActive;
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
        if (groupKey != null && (group == null || !groupKey.equals(group.getId()))) {
            this.group = new GroupOptions(groupKey);
        }
    }

    public GroupOptions getGroup() {
        return group;
    }

    public void setGroup(GroupOptions group) {
        this.group = group;
        if (group != null) {
            this.groupKey = group.getId();
        }
    }

    public JobOptions group(String groupId) {
        setGroupKey(groupId);
        return this;
    }

    public JobOptions group(GroupOptions group) {
        setGroup(group);
        return this;
    }

    public long getJobTimeoutMs() {
        return jobTimeoutMs;
    }

    public void setJobTimeoutMs(long jobTimeoutMs) {
        this.jobTimeoutMs = jobTimeoutMs;
    }

    @com.fasterxml.jackson.annotation.JsonSetter("jobTimeout")
    public void setJobTimeout(Object value) {
        if (value instanceof Number n) {
            this.jobTimeoutMs = n.longValue();
        }
    }

    @com.fasterxml.jackson.annotation.JsonSetter("ttl")
    public void setTtl(Object value) {
        if (value instanceof Number n) {
            this.jobTimeoutMs = n.longValue();
        }
    }

    public String getTraceparent() {
        return traceparent;
    }

    public void setTraceparent(String traceparent) {
        this.traceparent = traceparent;
    }

    public String getTracestate() {
        return tracestate;
    }

    public void setTracestate(String tracestate) {
        this.tracestate = tracestate;
    }

    public Map<String, Object> getTelemetry() {
        return telemetry;
    }

    public void setTelemetry(Map<String, Object> telemetry) {
        this.telemetry = telemetry;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>(24);
        if (jobId != null) map.put("jobId", jobId);
        map.put("delay", delayMs);
        map.put("attempts", attempts);
        if (keepCompletedJobs != null && (keepCompletedJobs.getCount() > 0 || keepCompletedJobs.getAgeSeconds() > 0)) {
            map.put("removeOnComplete", keepCompletedJobs.toMap());
        } else {
            map.put("removeOnComplete", removeOnComplete);
        }
        if (keepFailedJobs != null && (keepFailedJobs.getCount() > 0 || keepFailedJobs.getAgeSeconds() > 0)) {
            map.put("removeOnFail", keepFailedJobs.toMap());
        } else {
            map.put("removeOnFail", removeOnFail);
        }
        map.put("priority", priority);
        map.put("lifo", lifo);
        if (parentKey != null) map.put("parentKey", parentKey);
        if (deduplicationId != null && !deduplicationId.isBlank()) {
            map.put("deduplicationId", deduplicationId);
            Map<String, Object> dedupMap = new HashMap<>();
            dedupMap.put("id", deduplicationId);
            if (deduplicationTtlMs > 0 && !keepLastIfActive) {
                dedupMap.put("ttl", deduplicationTtlMs);
            }
            if (keepLastIfActive) {
                dedupMap.put("keepLastIfActive", true);
            }
            map.put("deduplication", dedupMap);
        }
        if (keepLogs > 0) map.put("keepLogs", keepLogs);
        if (failParentOnFailure) map.put("failParentOnFailure", true);
        if (removeDependencyOnFailure) map.put("removeDependencyOnFailure", true);
        if (continueParentOnFailure) map.put("continueParentOnFailure", true);
        if (ignoreDependencyOnFailure) map.put("ignoreDependencyOnFailure", true);
        if (groupKey != null) map.put("groupKey", groupKey);
        if (group != null) {
            Map<String, Object> groupMap = new HashMap<>();
            groupMap.put("id", group.getId());
            if (group.getMaxSize() > 0) groupMap.put("maxSize", group.getMaxSize());
            if (group.getPriority() > 0) groupMap.put("priority", group.getPriority());
            map.put("group", groupMap);
        }
        if (jobTimeoutMs > 0) {
            map.put("jobTimeout", jobTimeoutMs);
            map.put("ttl", jobTimeoutMs);
        }
        if (traceparent != null) {
            map.put("traceparent", traceparent);
            if (telemetry == null) {
                Map<String, Object> tel = new HashMap<>();
                tel.put("traceparent", traceparent);
                if (tracestate != null) tel.put("tracestate", tracestate);
                io.oxmq.tracing.TraceContext parsed = io.oxmq.tracing.TraceContext.parse(traceparent, tracestate);
                if (parsed != null) {
                    tel.put("traceId", parsed.traceId());
                    tel.put("spanId", parsed.spanId());
                }
                map.put("telemetry", tel);
            }
        }
        if (tracestate != null) map.put("tracestate", tracestate);
        if (telemetry != null) map.put("telemetry", telemetry);
        if (backoff != null) {
            if (backoff instanceof BackoffStrategy.Exponential exp) {
                map.put("backoff", Map.of("type", "exponential", "delay", exp.initialDelayMs()));
            } else if (backoff instanceof BackoffStrategy.Fixed fixed) {
                map.put("backoff", Map.of("type", "fixed", "delay", fixed.delayMs()));
            } else if (backoff instanceof BackoffStrategy.Custom custom) {
                map.put("backoff", Map.of("type", custom.name()));
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

        public Builder removeOnComplete(int keepCount) {
            options.setRemoveOnComplete(false);
            options.setKeepCompletedJobs(KeepJobs.count(keepCount));
            return this;
        }

        public Builder removeOnComplete(KeepJobs keepJobs) {
            options.setKeepCompletedJobs(keepJobs);
            return this;
        }

        public Builder removeOnFail(boolean removeOnFail) {
            options.setRemoveOnFail(removeOnFail);
            return this;
        }

        public Builder removeOnFail(int keepCount) {
            options.setRemoveOnFail(false);
            options.setKeepFailedJobs(KeepJobs.count(keepCount));
            return this;
        }

        public Builder removeOnFail(KeepJobs keepJobs) {
            options.setKeepFailedJobs(keepJobs);
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

        public Builder deduplication(String deduplicationId, boolean keepLastIfActive) {
            options.setDeduplicationId(deduplicationId);
            options.setKeepLastIfActive(keepLastIfActive);
            return this;
        }

        public Builder deduplication(DeduplicationOptions dedup) {
            if (dedup != null) {
                options.setDeduplicationId(dedup.getId());
                options.setDeduplicationTtlMs(dedup.getTtlMs());
                options.setKeepLastIfActive(dedup.isKeepLastIfActive());
            }
            return this;
        }

        public Builder keepLastIfActive(boolean keepLastIfActive) {
            options.setKeepLastIfActive(keepLastIfActive);
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

        public Builder group(String groupId) {
            if (groupId != null) {
                options.setGroup(GroupOptions.of(groupId));
            }
            return this;
        }

        public Builder group(String groupId, long maxSize) {
            if (groupId != null) {
                options.setGroup(GroupOptions.of(groupId, maxSize));
            }
            return this;
        }

        public Builder group(GroupOptions group) {
            options.setGroup(group);
            return this;
        }

        public Builder jobTimeout(Duration timeout) {
            options.setJobTimeoutMs(timeout != null ? timeout.toMillis() : 0);
            return this;
        }

        public Builder jobTimeoutMs(long jobTimeoutMs) {
            options.setJobTimeoutMs(jobTimeoutMs);
            return this;
        }

        public Builder ttl(Duration ttl) {
            options.setJobTimeoutMs(ttl != null ? ttl.toMillis() : 0);
            return this;
        }

        public Builder ttl(long ttlMs) {
            options.setJobTimeoutMs(ttlMs);
            return this;
        }

        public Builder traceparent(String traceparent) {
            options.setTraceparent(traceparent);
            return this;
        }

        public Builder tracestate(String tracestate) {
            options.setTracestate(tracestate);
            return this;
        }

        public Builder telemetry(Map<String, Object> telemetry) {
            options.setTelemetry(telemetry);
            return this;
        }

        public JobOptions build() {
            return options;
        }
    }
}
