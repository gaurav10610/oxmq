package io.oxmq.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.oxmq.QueueEvents;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
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
    private String groupKey;
    private String queueName;
    private Map<String, Object> childrenValues;
    private String checkpointJson;
    private String checkpointStep;

    @JsonIgnore
    private transient BiConsumer<Integer, Object> progressUpdater;

    @JsonIgnore
    private transient BiConsumer<String, Object> checkpointSaver;

    @JsonIgnore
    private transient Consumer<java.util.List<FlowJob>> childSpawner;

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

    @JsonIgnore
    private transient volatile Throwable batchFailureReason;

    @JsonIgnore
    private transient Consumer<T> dataUpdater;

    @JsonIgnore
    private transient Runnable promoter;

    @JsonIgnore
    private transient Consumer<Duration> delayChanger;

    @JsonIgnore
    private transient Consumer<Integer> priorityChanger;

    @JsonIgnore
    private transient Runnable retrier;

    @JsonIgnore
    private transient Supplier<Boolean> remover;

    @JsonIgnore
    private transient Supplier<JobState> stateSupplier;

    @JsonIgnore
    private transient Supplier<JobDependencies> dependenciesSupplier;

    @JsonIgnore
    private transient Function<Integer, JobNode> flowSupplier;

    @JsonIgnore
    private transient Supplier<Boolean> logCleaner;

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

    public String getGroupKey() {
        if (groupKey != null) {
            return groupKey;
        }
        return opts != null ? opts.getGroupKey() : null;
    }

    public void setGroupKey(String groupKey) {
        this.groupKey = groupKey;
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
        if (stateSupplier != null) {
            JobState fetched = stateSupplier.get();
            if (fetched != null) {
                this.state = fetched;
                return fetched;
            }
        }
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

    /**
     * Convenient alias for {@link #getCancellationToken()}.
     */
    public CancellationToken cancellationToken() {
        return getCancellationToken();
    }

    /**
     * Marks this specific job as failed within a batch execution.
     * Non-failed jobs in the same batch will still complete successfully.
     *
     * @param error the failure reason or exception
     */
    public void setAsFailed(Throwable error) {
        this.batchFailureReason = error != null ? error : new RuntimeException("Job marked as failed");
    }

    /**
     * Returns true if this job was marked as failed via {@link #setAsFailed(Throwable)}.
     */
    private List<Job<T>> batch;

    /**
     * Returns all jobs belonging to the current batch when this job is processed by a batch worker.
     * Mirrors BullMQ Pro's job.getBatch().
     *
     * @return list of jobs in the batch, or an empty list if this job is not part of a batch
     */
    public List<Job<T>> getBatch() {
        return batch != null ? batch : List.of();
    }

    public void setBatch(List<Job<T>> batch) {
        this.batch = batch;
    }

    public boolean isMarkedFailed() {
        return batchFailureReason != null;
    }

    /**
     * Returns the failure reason passed to {@link #setAsFailed(Throwable)}, if any.
     */
    public Throwable getBatchFailureReason() {
        return batchFailureReason;
    }

    /**
     * Saves an execution checkpoint with intermediate state in Redis.
     * If the job fails or is retried, the next attempt can retrieve this state via {@link #getCheckpoint(Class)}.
     *
     * @param state the state object to persist
     */
    public void checkpoint(Object state) {
        checkpoint(null, state);
    }

    /**
     * Saves an execution checkpoint with a step name and intermediate state in Redis.
     *
     * @param stepName logical stage/step name (e.g. "download", "transcode")
     * @param state the state object to persist
     */
    public void checkpoint(String stepName, Object state) {
        this.checkpointStep = stepName;
        if (checkpointSaver != null && state != null) {
            checkpointSaver.accept(stepName, state);
        }
    }

    /**
     * Retrieves the persisted checkpoint state deserialized into the specified target class.
     * Returns null if no checkpoint has been saved for this job.
     */
    @SuppressWarnings("unchecked")
    public <S> S getCheckpoint(Class<S> type) {
        if (checkpointJson == null || checkpointJson.isBlank() || type == null) {
            return null;
        }
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = io.oxmq.util.JsonUtils.mapper();
            return mapper.readValue(checkpointJson, type);
        } catch (Exception e) {
            return null;
        }
    }

    public String getCheckpointStep() {
        return checkpointStep;
    }

    public void setCheckpointStep(String checkpointStep) {
        this.checkpointStep = checkpointStep;
    }

    public String getCheckpointJson() {
        return checkpointJson;
    }

    public void setCheckpointJson(String checkpointJson) {
        this.checkpointJson = checkpointJson;
    }

    public void setCheckpointSaver(BiConsumer<String, Object> checkpointSaver) {
        this.checkpointSaver = checkpointSaver;
    }

    /**
     * Sets the spawner callback used for dynamic runtime child dependency ingestion.
     */
    public void setChildSpawner(Consumer<java.util.List<FlowJob>> childSpawner) {
        this.childSpawner = childSpawner;
    }

    /**
     * Dynamically spawns child jobs from inside this active job and transitions this parent job
     * into the {@code waiting-children} state.
     *
     * @param children the child jobs to schedule
     * @throws io.oxmq.exception.WaitingChildrenException automatically thrown to yield the active worker thread
     */
    public void addChildren(java.util.List<FlowJob> children) {
        if (childSpawner == null) {
            throw new IllegalStateException("Dynamic child spawning is only available during active job execution");
        }
        childSpawner.accept(children);
    }

    /**
     * Dynamically spawns a single child job from inside this active job and transitions this parent job
     * into the {@code waiting-children} state.
     *
     * @param child the child job to schedule
     * @throws io.oxmq.exception.WaitingChildrenException automatically thrown to yield the active worker thread
     */
    public void addChild(FlowJob child) {
        if (child != null) {
            addChildren(java.util.List.of(child));
        }
    }

    public void setDataUpdater(Consumer<T> dataUpdater) {
        this.dataUpdater = dataUpdater;
    }

    /**
     * Updates the underlying job payload in Redis and in-memory.
     * Mirrors BullMQ's job.updateData(data).
     *
     * @param data new payload data
     */
    public void updateData(T data) {
        this.data = data;
        if (dataUpdater != null) {
            dataUpdater.accept(data);
        }
    }

    public void setPromoter(Runnable promoter) {
        this.promoter = promoter;
    }

    /**
     * Promotes this delayed job to waiting state so it can be processed immediately.
     * Mirrors BullMQ's job.promote().
     */
    public void promote() {
        if (promoter != null) {
            promoter.run();
        }
    }

    public void setDelayChanger(Consumer<Duration> delayChanger) {
        this.delayChanger = delayChanger;
    }

    /**
     * Changes the delay for this job in Redis.
     * Mirrors BullMQ's job.changeDelay(delay).
     */
    public void changeDelay(Duration delay) {
        if (delayChanger != null && delay != null) {
            delayChanger.accept(delay);
        }
    }

    public void setPriorityChanger(Consumer<Integer> priorityChanger) {
        this.priorityChanger = priorityChanger;
    }

    /**
     * Changes the priority for this job in Redis.
     * Mirrors BullMQ's job.changePriority(priority).
     */
    public void changePriority(int priority) {
        if (priorityChanger != null) {
            priorityChanger.accept(priority);
        }
    }

    public void setRetrier(Runnable retrier) {
        this.retrier = retrier;
    }

    /**
     * Retries this job from failed or completed state.
     * Mirrors BullMQ's job.retry().
     */
    public void retry() {
        if (retrier != null) {
            retrier.run();
        }
    }

    public void setRemover(Supplier<Boolean> remover) {
        this.remover = remover;
    }

    /**
     * Completely removes this job from Redis.
     * Mirrors BullMQ's job.remove().
     */
    public boolean remove() {
        if (remover != null) {
            return Boolean.TRUE.equals(remover.get());
        }
        return false;
    }

    public void setStateSupplier(Supplier<JobState> stateSupplier) {
        this.stateSupplier = stateSupplier;
    }

    public void setDependenciesSupplier(Supplier<JobDependencies> dependenciesSupplier) {
        this.dependenciesSupplier = dependenciesSupplier;
    }

    /**
     * Retrieves child dependency breakdown (unprocessed, processed, failed) for a parent job in a DAG flow.
     * Mirrors BullMQ's job.getDependencies().
     */
    public JobDependencies getDependencies() {
        if (dependenciesSupplier != null) {
            return dependenciesSupplier.get();
        }
        return new JobDependencies(id, List.of(), Map.of(), Map.of());
    }

    public void setFlowSupplier(Function<Integer, JobNode> flowSupplier) {
        this.flowSupplier = flowSupplier;
    }

    /**
     * Retrieves the full workflow tree starting from this job as root.
     * Mirrors BullMQ's FlowProducer.getFlow().
     */
    public JobNode getFlow() {
        return getFlow(10);
    }

    /**
     * Retrieves the workflow tree starting from this job as root up to a max depth.
     */
    public JobNode getFlow(int maxDepth) {
        if (flowSupplier != null) {
            return flowSupplier.apply(maxDepth);
        }
        return null;
    }

    public void setLogCleaner(Supplier<Boolean> logCleaner) {
        this.logCleaner = logCleaner;
    }

    /**
     * Clears all log entries for this job in Redis.
     * Mirrors BullMQ's job.clearLogs().
     */
    public boolean clearLogs() {
        if (logCleaner != null) {
            return Boolean.TRUE.equals(logCleaner.get());
        }
        return false;
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
