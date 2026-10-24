package io.oxmq.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.oxmq.QueueEvents;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Represents a distributed job unit in OxMQ.
 *
 * @param <T> Payload data type
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Job<T> {

    private String id;
    private String name;
    private T data;
    private JobOptions opts = new JobOptions();
    private JobState state;
    private int progress = 0;
    private int attemptsMade = 0;
    private int attemptsStarted = 0;
    private long timestamp = System.currentTimeMillis();
    private Long processedOn;
    private Long finishedOn;
    private Object returnvalue;
    private String failedReason;
    private String parentKey;
    private String repeatJobKey;
    private String queueName;
    private Map<String, Object> childrenValues;

    @JsonIgnore
    private transient BiConsumer<Integer, Object> progressUpdater;

    @JsonIgnore
    private transient Consumer<String> logAppender;

    @JsonIgnore
    private transient Supplier<Map<String, Object>> childrenValuesSupplier;

    @JsonIgnore
    private transient Supplier<Map<String, String>> ignoredChildrenFailuresSupplier;

    private Map<String, String> ignoredChildrenFailures;

    @JsonIgnore
    private transient volatile boolean discarded = false;

    @JsonIgnore
    private transient volatile boolean cancelled = false;

    @JsonIgnore
    private transient volatile String cancelReason;

    @JsonIgnore
    private transient CancellationToken cancellationToken = new CancellationToken();

    public Job() {}

    public Job(String id, String name, T data, JobOptions opts) {
        this.id = id;
        this.name = name;
        this.data = data;
        this.opts = opts != null ? opts : new JobOptions();
    }

    /**
     * Updates real-time progress percentage (0-100) and syncs to Redis.
     */
    public void updateProgress(int percentage) {
        updateProgress(percentage, null);
    }

    /**
     * Updates real-time progress percentage (0-100) and structured payload.
     */
    public void updateProgress(int percentage, Object payload) {
        this.progress = Math.min(100, Math.max(0, percentage));
        if (progressUpdater != null) {
            progressUpdater.accept(this.progress, payload);
        }
    }

    /**
     * Appends a log line to this job's log list in Redis.
     */
    public void log(String message) {
        if (logAppender != null && message != null) {
            logAppender.accept(message);
        }
    }

    // Getters and Setters

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public JobOptions getOpts() {
        return opts;
    }

    public void setOpts(JobOptions opts) {
        this.opts = opts;
    }

    public int getProgress() {
        return progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    public int getAttemptsMade() {
        return attemptsMade;
    }

    public void setAttemptsMade(int attemptsMade) {
        this.attemptsMade = attemptsMade;
    }

    public int getAttemptsStarted() {
        return attemptsStarted;
    }

    public void setAttemptsStarted(int attemptsStarted) {
        this.attemptsStarted = attemptsStarted;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getProcessedOn() {
        return processedOn;
    }

    public void setProcessedOn(Long processedOn) {
        this.processedOn = processedOn;
    }

    public Long getFinishedOn() {
        return finishedOn;
    }

    public void setFinishedOn(Long finishedOn) {
        this.finishedOn = finishedOn;
    }

    public Object getReturnvalue() {
        return returnvalue;
    }

    public void setReturnvalue(Object returnvalue) {
        this.returnvalue = returnvalue;
    }

    public String getFailedReason() {
        return failedReason;
    }

    public void setFailedReason(String failedReason) {
        this.failedReason = failedReason;
    }

    public String getParentKey() {
        return parentKey;
    }

    public void setParentKey(String parentKey) {
        this.parentKey = parentKey;
    }

    public String getRepeatJobKey() {
        return repeatJobKey;
    }

    public void setRepeatJobKey(String repeatJobKey) {
        this.repeatJobKey = repeatJobKey;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public Map<String, Object> getChildrenValues() {
        if (childrenValues == null && childrenValuesSupplier != null) {
            childrenValues = childrenValuesSupplier.get();
        }
        return childrenValues;
    }

    public void setChildrenValues(Map<String, Object> childrenValues) {
        this.childrenValues = childrenValues;
    }

    public void setChildrenValuesSupplier(Supplier<Map<String, Object>> childrenValuesSupplier) {
        this.childrenValuesSupplier = childrenValuesSupplier;
    }

    /**
     * Flags this actively executing job so that if processing fails, it will NOT be retried
     * even if attempts remain. Mirrors BullMQ's job.discard().
     */
    public void discard() {
        this.discarded = true;
    }

    public boolean isDiscarded() {
        return discarded;
    }

    /**
     * In a parent-child DAG workflow where the parent has ignoreDependencyOnFailure set to true,
     * returns a map of failed child job keys to their respective failure reasons.
     * Mirrors BullMQ's job.getIgnoredChildrenFailures().
     */
    public Map<String, String> getIgnoredChildrenFailures() {
        if (ignoredChildrenFailures == null && ignoredChildrenFailuresSupplier != null) {
            ignoredChildrenFailures = ignoredChildrenFailuresSupplier.get();
        }
        return ignoredChildrenFailures != null ? ignoredChildrenFailures : Map.of();
    }

    public void setIgnoredChildrenFailures(Map<String, String> ignoredChildrenFailures) {
        this.ignoredChildrenFailures = ignoredChildrenFailures;
    }

    public void setIgnoredChildrenFailuresSupplier(Supplier<Map<String, String>> ignoredChildrenFailuresSupplier) {
        this.ignoredChildrenFailuresSupplier = ignoredChildrenFailuresSupplier;
    }

    public String getTraceparent() {
        if (opts != null && opts.getTraceparent() != null) {
            return opts.getTraceparent();
        }
        if (opts != null && opts.getTelemetry() != null && opts.getTelemetry().get("traceparent") != null) {
            return opts.getTelemetry().get("traceparent").toString();
        }
        return null;
    }

    public String getTracestate() {
        if (opts != null && opts.getTracestate() != null) {
            return opts.getTracestate();
        }
        if (opts != null && opts.getTelemetry() != null && opts.getTelemetry().get("tracestate") != null) {
            return opts.getTelemetry().get("tracestate").toString();
        }
        return null;
    }

    /**
     * Resolves the W3C TraceContext representation for this job.
     */
    public io.oxmq.tracing.TraceContext getTraceContext() {
        return io.oxmq.tracing.TraceContext.extract(this);
    }

    public void setProgressUpdater(BiConsumer<Integer, Object> progressUpdater) {
        this.progressUpdater = progressUpdater;
    }

    public void setLogAppender(Consumer<String> logAppender) {
        this.logAppender = logAppender;
    }

    public JobState getState() {
        return state;
    }

    public void setState(JobState state) {
        this.state = state;
    }

    public boolean isCompleted() {
        return state == JobState.COMPLETED;
    }

    public boolean isFailed() {
        return state == JobState.FAILED;
    }

    public boolean isActive() {
        return state == JobState.ACTIVE;
    }

    public boolean isWaiting() {
        return state == JobState.WAITING;
    }

    public boolean isDelayed() {
        return state == JobState.DELAYED;
    }

    public boolean isWaitingChildren() {
        return state == JobState.WAITING_CHILDREN;
    }

    /**
     * Waits for this job to complete or fail, returning its result or throwing on failure.
     */
    public Object waitUntilFinished(QueueEvents queueEvents, Duration timeout) throws Exception {
        if (isCompleted()) {
            return returnvalue;
        }
        if (isFailed()) {
            throw new RuntimeException(failedReason != null ? failedReason : "Job " + id + " failed");
        }
        return queueEvents.waitUntilFinished(this.id, timeout);
    }

    /**
     * Asynchronously waits for this job to complete or fail.
     */
    public CompletableFuture<Object> waitUntilFinishedAsync(QueueEvents queueEvents) {
        if (isCompleted()) {
            return CompletableFuture.completedFuture(returnvalue);
        }
        if (isFailed()) {
            return CompletableFuture.failedFuture(new RuntimeException(failedReason != null ? failedReason : "Job " + id + " failed"));
        }
        return queueEvents.waitUntilFinishedAsync(this.id);
    }

    public boolean isCancelled() {
        return cancelled || Thread.currentThread().isInterrupted();
    }

    public String getCancelReason() {
        if (cancelReason != null) {
            return cancelReason;
        }
        return cancellationToken != null ? cancellationToken.getReason() : null;
    }

    public void setCancelled(boolean cancelled, String cancelReason) {
        this.cancelled = cancelled;
        this.cancelReason = cancelReason;
        if (cancelled) {
            getCancellationToken().cancel(cancelReason);
        }
    }

    public CancellationToken getCancellationToken() {
        if (cancellationToken == null) {
            cancellationToken = new CancellationToken();
        }
        return cancellationToken;
    }

    @Override
    public String toString() {
        return "Job{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", queueName='" + queueName + '\'' +
                ", attemptsMade=" + attemptsMade +
                ", progress=" + progress +
                '}';
    }
}
