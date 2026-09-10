package io.oxmq.client;

import java.util.HashMap;
import java.util.Map;

/**
 * Handles exact BullMQ Redis key mapping and namespaces.
 */
public class QueueKeys {

    private final String prefix;
    private final String queueName;
    private final String qualifiedName;
    private final Map<String, String> keyMap = new HashMap<>();

    public QueueKeys(String prefix, String queueName) {
        this.prefix = prefix != null && !prefix.isBlank() ? prefix : "bull";
        this.queueName = queueName;
        this.qualifiedName = this.prefix + ":" + this.queueName;
        initializeKeys();
    }

    public QueueKeys(String queueName) {
        this("bull", queueName);
    }

    private void initializeKeys() {
        String[] types = {
            "", "active", "wait", "waiting-children", "paused", "completed",
            "failed", "delayed", "repeat", "stalled", "limiter", "prioritized",
            "id", "stalled-check", "meta", "pc", "events", "marker", "de"
        };
        for (String type : types) {
            keyMap.put(type, toKey(type));
        }
    }

    public String toKey(String type) {
        if (type == null || type.isEmpty()) {
            return qualifiedName + ":";
        }
        return qualifiedName + ":" + type;
    }

    public String getKey(String type) {
        return keyMap.getOrDefault(type, toKey(type));
    }

    public String toJobKey(String jobId) {
        return qualifiedName + ":" + jobId;
    }

    public String toJobLockKey(String jobId) {
        return qualifiedName + ":" + jobId + ":lock";
    }

    public String toJobLogsKey(String jobId) {
        return qualifiedName + ":" + jobId + ":logs";
    }

    public String toJobDependenciesKey(String jobId) {
        return qualifiedName + ":" + jobId + ":dependencies";
    }

    public String getPrefix() {
        return prefix;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getQualifiedName() {
        return qualifiedName;
    }
}
