package io.oxmq.spring;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.micrometer.core.instrument.MeterRegistry;
import io.oxmq.FlowProducer;
import io.oxmq.OxmqQueue;
import io.oxmq.client.RedisConnectionManager;
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
import org.springframework.core.env.Environment;

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
        if (properties.isClusterHashTags()) {
            System.setProperty("oxmq.cluster-hash-tags", "true");
        }
        String uri = properties.getRedis().getUri();
        return RedisClient.create(RedisURI.create(uri));
    }

    @Bean
    @ConditionalOnMissingBean
    public RedisConnectionManager oxmqConnectionManager(RedisClient redisClient) {
        return new RedisConnectionManager(redisClient);
    }

    @Bean
    @ConditionalOnMissingBean
    public FlowProducer oxmqFlowProducer(RedisConnectionManager connectionManager,
                                        LuaScriptManager scriptManager,
                                        JobSerializer serializer,
                                        OxmqProperties properties,
                                        org.springframework.core.env.Environment environment) {
        String effectivePrefix = properties.resolvePrefix(environment);
        return new FlowProducer(connectionManager, scriptManager, serializer, effectivePrefix);
    }

    @Bean
    @ConditionalOnMissingBean
    public OxmqTemplate oxmqTemplate(RedisClient redisClient,
                                     RedisConnectionManager connectionManager,
                                     LuaScriptManager scriptManager,
                                     JobSerializer serializer,
                                     OxmqMetrics metrics,
                                     FlowProducer flowProducer,
                                     OxmqProperties properties,
                                     Environment environment) {
        String effectivePrefix = properties.resolvePrefix(environment);
        io.oxmq.model.JobOptions defaultOpts = properties.getDefaultJobOptions() != null ? properties.getDefaultJobOptions().toJobOptions() : null;
        int maxLenEvents = properties.getMaxLenEvents();
        return new OxmqTemplate(redisClient, connectionManager, scriptManager, serializer, metrics, flowProducer, effectivePrefix, defaultOpts, maxLenEvents);
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
    public static OxmqListenerAnnotationBeanPostProcessor oxmqListenerAnnotationBeanPostProcessor() {
        return new OxmqListenerAnnotationBeanPostProcessor();
    }

    @Bean
    @ConditionalOnClass(HealthIndicator.class)
    @ConditionalOnMissingBean(name = "oxmqHealthIndicator")
    public OxmqHealthIndicator oxmqHealthIndicator(RedisClient redisClient,
                                                   OxmqListenerAnnotationBeanPostProcessor postProcessor,
                                                   OxmqProperties properties,
                                                   Environment environment) {
        String effectivePrefix = properties.resolvePrefix(environment);
        return new OxmqHealthIndicator(redisClient, postProcessor, effectivePrefix);
    }
}
