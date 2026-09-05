package io.oxmq.spring;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.micrometer.core.instrument.MeterRegistry;
import io.oxmq.OxmqQueue;
import io.oxmq.lua.LuaScriptManager;
import io.oxmq.metrics.OxmqMetrics;
import io.oxmq.serializer.JacksonJobSerializer;
import io.oxmq.serializer.JobSerializer;
import io.oxmq.spring.actuator.OxmqHealthIndicator;
import io.oxmq.spring.postprocessor.OxmqListenerAnnotationBeanPostProcessor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot 3 Auto-configuration for OxMQ.
 */
@AutoConfiguration
@ConditionalOnClass(OxmqQueue.class)
@EnableConfigurationProperties(OxmqProperties.class)
public class OxmqAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public RedisClient oxmqRedisClient(OxmqProperties properties) {
        String uri = properties.getRedis().getUri();
        return RedisClient.create(RedisURI.create(uri));
    }

    @Bean
    @ConditionalOnMissingBean
    public LuaScriptManager oxmqLuaScriptManager() {
        return new LuaScriptManager();
    }

    @Bean
    @ConditionalOnMissingBean
    public JobSerializer oxmqJobSerializer() {
        return new JacksonJobSerializer();
    }

    @Bean
    @ConditionalOnMissingBean
    public OxmqMetrics oxmqMetrics(ObjectProvider<MeterRegistry> meterRegistryProvider, OxmqProperties properties) {
        if (!properties.isMetricsEnabled()) {
            return new OxmqMetrics(null);
        }
        MeterRegistry registry = meterRegistryProvider.getIfAvailable();
        return new OxmqMetrics(registry);
    }

    @Bean
    @ConditionalOnMissingBean
    public OxmqListenerAnnotationBeanPostProcessor oxmqListenerAnnotationBeanPostProcessor(
            RedisClient redisClient,
            LuaScriptManager scriptManager,
            JobSerializer serializer,
            OxmqMetrics metrics,
            OxmqProperties properties) {
        return new OxmqListenerAnnotationBeanPostProcessor(redisClient, scriptManager, serializer, metrics, properties);
    }

    @Bean
    @ConditionalOnClass(HealthIndicator.class)
    @ConditionalOnMissingBean(name = "oxmqHealthIndicator")
    public OxmqHealthIndicator oxmqHealthIndicator(RedisClient redisClient,
                                                   OxmqListenerAnnotationBeanPostProcessor postProcessor) {
        return new OxmqHealthIndicator(redisClient, postProcessor);
    }
}
