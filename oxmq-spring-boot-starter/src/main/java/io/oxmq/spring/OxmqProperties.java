package io.oxmq.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for OxMQ in Spring Boot.
 */
@ConfigurationProperties(prefix = "oxmq")
public class OxmqProperties {

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
     * Whether Micrometer metrics are enabled.
     */
    private boolean metricsEnabled = true;

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

        public String getUri() {
            return uri;
        }

        public void setUri(String uri) {
            this.uri = uri;
        }
    }
}
