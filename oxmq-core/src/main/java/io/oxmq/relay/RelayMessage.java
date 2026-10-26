package io.oxmq.relay;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.oxmq.util.JsonUtils;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents a distributed message routed through {@link OxmqRelay}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RelayMessage {

    private String messageId;
    private String topic;
    private String payloadJson;
    private long timestamp;
    private String senderNodeId;
    private boolean retained;

    public RelayMessage() {}

    public RelayMessage(String topic, String payloadJson, long timestamp, String senderNodeId, boolean retained) {
        this(UUID.randomUUID().toString(), topic, payloadJson, timestamp, senderNodeId, retained);
    }

    public RelayMessage(String messageId, String topic, String payloadJson, long timestamp, String senderNodeId, boolean retained) {
        this.messageId = messageId != null ? messageId : UUID.randomUUID().toString();
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.payloadJson = payloadJson;
        this.timestamp = timestamp;
        this.senderNodeId = senderNodeId;
        this.retained = retained;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getPayloadJson() {
        return payloadJson;
    }

    public void setPayloadJson(String payloadJson) {
        this.payloadJson = payloadJson;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getSenderNodeId() {
        return senderNodeId;
    }

    public void setSenderNodeId(String senderNodeId) {
        this.senderNodeId = senderNodeId;
    }

    public boolean isRetained() {
        return retained;
    }

    public void setRetained(boolean retained) {
        this.retained = retained;
    }

    /**
     * Deserializes the message payload JSON into the specified target type.
     */
    public <T> T getPayload(Class<T> type) {
        if (payloadJson == null || payloadJson.isBlank() || type == null) {
            return null;
        }
        if (type == String.class) {
            return type.cast(payloadJson);
        }
        try {
            return JsonUtils.mapper().readValue(payloadJson, type);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize relay payload to " + type.getSimpleName(), e);
        }
    }

    @Override
    public String toString() {
        return "RelayMessage{" +
                "messageId='" + messageId + '\'' +
                ", topic='" + topic + '\'' +
                ", timestamp=" + timestamp +
                ", senderNodeId='" + senderNodeId + '\'' +
                ", retained=" + retained +
                '}';
    }
}
