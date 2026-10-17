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
     * Whether Micrometer metrics are enabled.
     */
    private boolean metricsEnabled = true;

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
}
