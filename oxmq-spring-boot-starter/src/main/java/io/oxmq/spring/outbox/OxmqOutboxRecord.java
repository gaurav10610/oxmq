package io.oxmq.spring.outbox;

import java.time.Instant;
import java.util.UUID;

/**
 * Represents a persistent outbox record stored in an RDBMS transaction alongside business data.
 */
public class OxmqOutboxRecord {

    public enum Status {
        PENDING,
        PUBLISHED,
        FAILED
    }

    private String id;
    private String queueName;
    private String jobName;
    private String payloadJson;
    private String optionsJson;
    private String groupKey;
    private Status status = Status.PENDING;
    private int attempts = 0;
    private Instant createdAt = Instant.now();
    private Instant publishedAt;
    private String lastError;

    public OxmqOutboxRecord() {
        this.id = UUID.randomUUID().toString();
    }

    public OxmqOutboxRecord(String queueName, String jobName, String payloadJson, String optionsJson, String groupKey) {
        this();
        this.queueName = queueName;
        this.jobName = jobName;
        this.payloadJson = payloadJson;
        this.optionsJson = optionsJson;
        this.groupKey = groupKey;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getPayloadJson() {
        return payloadJson;
    }

    public void setPayloadJson(String payloadJson) {
        this.payloadJson = payloadJson;
    }

    public String getOptionsJson() {
        return optionsJson;
    }

    public void setOptionsJson(String optionsJson) {
        this.optionsJson = optionsJson;
    }

    public String getGroupKey() {
        return groupKey;
    }

    public void setGroupKey(String groupKey) {
        this.groupKey = groupKey;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public void setPublishedAt(Instant publishedAt) {
        this.publishedAt = publishedAt;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }
}
