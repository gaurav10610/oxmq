package io.oxmq.spring;

import io.oxmq.client.QueueKeys;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.env.Environment;

/**
 * Configuration properties for OxMQ in Spring Boot.
 */
@ConfigurationProperties(prefix = "oxmq")
public class OxmqProperties {

    /**
     * Service-level namespace prefix for all queues, workers, and events.
     * When left unspecified, automatically defaults to ${spring.application.name} if configured,
     * or "bull" for standard default wire-compatibility.
     */
    private String prefix;

    /**
     * Redis connection properties.
     */
    private Redis redis = new Redis();

    /**
     * Default concurrency for worker listeners when not specified.
     */
    private int defaultConcurrency = 20;

    /**
     * Whether to use Java 21 Virtual Threads by default.
     */
    private boolean virtualThreads = true;

    /**
     * Maximum approximate number of events retained in the Redis stream (bull:<queue>:events).
     * Defaults to BullMQ standard 10,000. Lower values (e.g. 1000) significantly reduce Redis RAM usage.
     */
    private int maxLenEvents = -1;

    /**
     * Default job retention options for all queues created via OxmqTemplate / OxmqQueue.
     */
    private DefaultJobOptions defaultJobOptions = new DefaultJobOptions();

    /**
     * Whether to wrap queue keys in Redis Cluster hash-tags ({prefix:queueName}) to guarantee
     * single-slot collocation on Redis Cluster and avoid CROSSSLOT errors.
     */
    private boolean clusterHashTags = false;

    /**
     * Whether Micrometer metrics are enabled.
     */
    private boolean metricsEnabled = true;

    /**
     * W3C OpenTelemetry distributed tracing and MDC propagation configuration.
     */
    private Tracing tracing = new Tracing();

    public boolean isClusterHashTags() {
        return clusterHashTags;
    }

    public void setClusterHashTags(boolean clusterHashTags) {
        this.clusterHashTags = clusterHashTags;
    }

    public int getMaxLenEvents() {
        return maxLenEvents;
    }

    public void setMaxLenEvents(int maxLenEvents) {
        this.maxLenEvents = maxLenEvents;
    }

    public DefaultJobOptions getDefaultJobOptions() {
        return defaultJobOptions;
    }

    public void setDefaultJobOptions(DefaultJobOptions defaultJobOptions) {
        this.defaultJobOptions = defaultJobOptions;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    /**
     * Resolves the effective service namespace prefix following the strict precedence hierarchy:
     * <ol>
     *   <li>Explicit {@code oxmq.prefix} property</li>
     *   <li>{@code spring.application.name} if configured in Environment</li>
     *   <li>{@code OXMQ_PREFIX} environment variable / {@code -Doxmq.prefix} system property</li>
     *   <li>Fallback default {@code "bull"}</li>
     * </ol>
     */
    public String resolvePrefix(Environment env) {
        if (prefix != null && !prefix.isBlank()) {
            return prefix.trim();
        }
        if (env != null) {
            String appName = env.getProperty("spring.application.name");
            if (appName != null && !appName.isBlank()) {
                return appName.trim();
            }
        }
        return QueueKeys.resolvePrefix(null);
    }

    public Redis getRedis() {
        return redis;
    }

    public void setRedis(Redis redis) {
        this.redis = redis;
    }

    public int getDefaultConcurrency() {
        return defaultConcurrency;
    }

    public void setDefaultConcurrency(int defaultConcurrency) {
        this.defaultConcurrency = defaultConcurrency;
    }

    public boolean isVirtualThreads() {
        return virtualThreads;
    }

    public void setVirtualThreads(boolean virtualThreads) {
        this.virtualThreads = virtualThreads;
    }

    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    public void setMetricsEnabled(boolean metricsEnabled) {
        this.metricsEnabled = metricsEnabled;
    }

    public static class Redis {
        /**
         * Redis URI connection string (e.g., redis://localhost:6379 or rediss://...).
         */
        private String uri = "redis://localhost:6379";
        private String host;
        private int port = 6379;
        private String password;

        public String getUri() {
            if (host != null && !host.isBlank()) {
                if (password != null && !password.isBlank()) {
                    return "redis://:" + password + "@" + host + ":" + port;
                }
                return "redis://" + host + ":" + port;
            }
            return uri;
        }

        public void setUri(String uri) {
            this.uri = uri;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    public static class DefaultJobOptions {
        private Integer removeOnCompleteCount;
        private Long removeOnCompleteAgeSeconds;
        private Integer removeOnFailCount;
        private Long removeOnFailAgeSeconds;

        public Integer getRemoveOnCompleteCount() {
            return removeOnCompleteCount;
        }

        public void setRemoveOnCompleteCount(Integer removeOnCompleteCount) {
            this.removeOnCompleteCount = removeOnCompleteCount;
        }

        public Long getRemoveOnCompleteAgeSeconds() {
            return removeOnCompleteAgeSeconds;
        }

        public void setRemoveOnCompleteAgeSeconds(Long removeOnCompleteAgeSeconds) {
            this.removeOnCompleteAgeSeconds = removeOnCompleteAgeSeconds;
        }

        public Integer getRemoveOnFailCount() {
            return removeOnFailCount;
        }

        public void setRemoveOnFailCount(Integer removeOnFailCount) {
            this.removeOnFailCount = removeOnFailCount;
        }

        public Long getRemoveOnFailAgeSeconds() {
            return removeOnFailAgeSeconds;
        }

        public void setRemoveOnFailAgeSeconds(Long removeOnFailAgeSeconds) {
            this.removeOnFailAgeSeconds = removeOnFailAgeSeconds;
        }

        public io.oxmq.model.JobOptions toJobOptions() {
            if (removeOnCompleteCount == null && removeOnCompleteAgeSeconds == null
                    && removeOnFailCount == null && removeOnFailAgeSeconds == null) {
                return null;
            }
            io.oxmq.model.JobOptions.Builder b = io.oxmq.model.JobOptions.builder();
            if (removeOnCompleteCount != null || removeOnCompleteAgeSeconds != null) {
                int count = removeOnCompleteCount != null ? removeOnCompleteCount : 0;
                long age = removeOnCompleteAgeSeconds != null ? removeOnCompleteAgeSeconds : 0;
                b.removeOnComplete(io.oxmq.model.KeepJobs.of(count, java.time.Duration.ofSeconds(age)));
            }
            if (removeOnFailCount != null || removeOnFailAgeSeconds != null) {
                int count = removeOnFailCount != null ? removeOnFailCount : 0;
                long age = removeOnFailAgeSeconds != null ? removeOnFailAgeSeconds : 0;
                b.removeOnFail(io.oxmq.model.KeepJobs.of(count, java.time.Duration.ofSeconds(age)));
            }
            return b.build();
        }
    }

    public Tracing getTracing() {
        return tracing;
    }

    public void setTracing(Tracing tracing) {
        this.tracing = tracing;
    }

    public static class Tracing {
        /**
         * Whether automatic W3C traceparent extraction and SLF4J MDC binding is enabled.
         */
        private boolean enabled = true;

        /**
         * SLF4J MDC key name for trace ID. Defaults to "traceId".
         */
        private String traceIdKey = "traceId";

        /**
         * SLF4J MDC key name for span ID. Defaults to "spanId".
         */
        private String spanIdKey = "spanId";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getTraceIdKey() {
            return traceIdKey;
        }

        public void setTraceIdKey(String traceIdKey) {
            this.traceIdKey = traceIdKey;
        }

        public String getSpanIdKey() {
            return spanIdKey;
        }

        public void setSpanIdKey(String spanIdKey) {
            this.spanIdKey = spanIdKey;
        }
    }
}
