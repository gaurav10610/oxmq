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

    public static String resolvePrefix(String explicitPrefix) {
        if (explicitPrefix != null && !explicitPrefix.isBlank()) {
            return explicitPrefix.trim();
        }
        String envPrefix = System.getenv("OXMQ_PREFIX");
        if (envPrefix != null && !envPrefix.isBlank()) {
            return envPrefix.trim();
        }
        String sysPrefix = System.getProperty("oxmq.prefix");
        if (sysPrefix != null && !sysPrefix.isBlank()) {
            return sysPrefix.trim();
        }
        return "bull";
    }

    public static boolean isClusterHashTagEnabled() {
        String env = System.getenv("OXMQ_CLUSTER_HASH_TAGS");
        if (env != null && !env.isBlank()) {
            return Boolean.parseBoolean(env.trim());
        }
        String sysProp = System.getProperty("oxmq.cluster-hash-tags");
        if (sysProp != null && !sysProp.isBlank()) {
            return Boolean.parseBoolean(sysProp.trim());
        }
        return false;
    }

    public QueueKeys(String prefix, String queueName) {
        this(prefix, queueName, isClusterHashTagEnabled());
    }

    public QueueKeys(String queueName) {
        this(null, queueName, isClusterHashTagEnabled());
    }

    public QueueKeys(String prefix, String queueName, boolean useHashTag) {
        this.prefix = resolvePrefix(prefix);
        this.queueName = queueName;
        if (useHashTag || isClusterHashTagEnabled()) {
            if (this.prefix.contains("{") || (this.queueName != null && this.queueName.contains("{"))) {
                this.qualifiedName = this.prefix + ":" + this.queueName;
            } else {
                this.qualifiedName = "{" + this.prefix + ":" + this.queueName + "}";
            }
        } else {
            this.qualifiedName = this.prefix + ":" + this.queueName;
        }
        initializeKeys();
    }

    public static QueueKeys clusterTagged(String prefix, String queueName) {
        return new QueueKeys(prefix, queueName, true);
    }

    public boolean isClusterTagged() {
        return qualifiedName.startsWith("{") && qualifiedName.contains("}");
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
