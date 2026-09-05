package io.oxmq.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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
    private int progress = 0;
    private int attemptsMade = 0;
    private long timestamp = System.currentTimeMillis();
    private Long processedOn;
    private Long finishedOn;
    private Object returnvalue;
    private String failedReason;
    private String parentKey;
    private String queueName;
    private Map<String, Object> childrenValues;

    @JsonIgnore
    private transient BiConsumer<Integer, Object> progressUpdater;

    @JsonIgnore
    private transient Consumer<String> logAppender;

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
        this.progress = Math.clamp(percentage, 0, 100);
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

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public Map<String, Object> getChildrenValues() {
        return childrenValues;
    }

    public void setChildrenValues(Map<String, Object> childrenValues) {
        this.childrenValues = childrenValues;
    }

    public void setProgressUpdater(BiConsumer<Integer, Object> progressUpdater) {
        this.progressUpdater = progressUpdater;
    }

    public void setLogAppender(Consumer<String> logAppender) {
        this.logAppender = logAppender;
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
