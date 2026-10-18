package io.oxmq.oxguard.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.exception.RateLimitCooldownException;
import io.oxmq.model.Job;
import io.oxmq.oxguard.model.TenantProfile;
import io.oxmq.oxguard.model.TenantScopedPayload;
import io.oxmq.plugin.GroupRateLimiterPlugin;
import io.oxmq.plugin.PriorityQuotaGovernorPlugin;
import io.oxmq.plugin.RateLimitSentinelPlugin;
import java.time.Duration;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Automatically wires OxMQ Enterprise Plugins into the Spring ApplicationContext.
 * Because OxMQ's OxmqListenerAnnotationBeanPostProcessor auto-discovers all JobInterceptor beans,
 * these plugins are immediately attached to all single-job and multi-job @OxmqListener workers.
 */
@Configuration
public class OxGuardPluginConfiguration {

    private static final Logger log = LoggerFactory.getLogger(OxGuardPluginConfiguration.class);

    public static String extractTenantId(Job<?> job) {
        if (job == null) return "tenant-mid-2500";
        Object data = job.getData();
        if (data instanceof TenantScopedPayload tsp) {
            return tsp.tenantId();
        }
        if (data instanceof Map<?, ?> map) {
            Object tid = map.get("tenantId");
            if (tid != null) return tid.toString();
        }
        return "tenant-mid-2500";
    }

    @Bean
    public GroupRateLimiterPlugin groupRateLimiterPlugin(
            RedisConnectionManager connectionManager,
            MeterRegistry meterRegistry
    ) {
        log.info("Configuring native OxMQ GroupRateLimiterPlugin for multi-tenant token bucket isolation...");
        return GroupRateLimiterPlugin.builder()
                .connectionManager(connectionManager)
                .keyPrefix("oxmq:limiter:tenant")
                .groupKeyResolver(OxGuardPluginConfiguration::extractTenantId)
                .dynamicLimit(tenantId -> TenantProfile.getByIdOrDefault(tenantId).burstLimitRUsPerSec())
                .windowDuration(Duration.ofSeconds(1))
                .retryDelayMs(150)
                .meterRegistry(meterRegistry)
                .build();
    }

    @Bean
    public RateLimitSentinelPlugin rateLimitSentinelPlugin(
            RedisConnectionManager connectionManager,
            MeterRegistry meterRegistry
    ) {
        log.info("Configuring native OxMQ RateLimitSentinelPlugin for external 429/503 circuit-breaker isolation...");
        return RateLimitSentinelPlugin.builder()
                .connectionManager(connectionManager)
                .keyPrefix("oxmq:sentinel:tenant")
                .groupKeyResolver(OxGuardPluginConfiguration::extractTenantId)
                .errorMatcher(t -> t instanceof RateLimitCooldownException)
                .meterRegistry(meterRegistry)
                .build();
    }

    @Bean
    public PriorityQuotaGovernorPlugin priorityQuotaGovernorPlugin(
            GroupRateLimiterPlugin rateLimiter,
            MeterRegistry meterRegistry,
            @Value("${oxguard.rate-limit.headroom-yield-threshold:0.25}") double yieldThreshold
    ) {
        log.info("Configuring native OxMQ PriorityQuotaGovernorPlugin (headroom threshold: {}%)...", Math.round(yieldThreshold * 100));
        return PriorityQuotaGovernorPlugin.builder()
                .rateLimiter(rateLimiter)
                .groupKeyResolver(OxGuardPluginConfiguration::extractTenantId)
                .headroomYieldThreshold(yieldThreshold)
                .priorityThreshold(1) // Priority 1 (forward webhook events) is strictly protected!
                .yieldDelayMs(200)
                .meterRegistry(meterRegistry)
                .build();
    }
}
