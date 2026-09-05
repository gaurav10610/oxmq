package io.oxmq.spring;

import io.oxmq.lua.LuaScriptManager;
import io.oxmq.metrics.OxmqMetrics;
import io.oxmq.serializer.JobSerializer;
import io.oxmq.spring.postprocessor.OxmqListenerAnnotationBeanPostProcessor;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class OxmqAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(OxmqAutoConfiguration.class));

    @Test
    void testDefaultAutoConfigurationLoadsBeans() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(LuaScriptManager.class);
            assertThat(context).hasSingleBean(JobSerializer.class);
            assertThat(context).hasSingleBean(OxmqMetrics.class);
            assertThat(context).hasSingleBean(OxmqListenerAnnotationBeanPostProcessor.class);
        });
    }

    @Test
    void testCustomPropertiesBinding() {
        contextRunner.withPropertyValues(
                "oxmq.redis.uri=redis://custom-host:6380",
                "oxmq.default-concurrency=100",
                "oxmq.virtual-threads=false"
        ).run(context -> {
            OxmqProperties props = context.getBean(OxmqProperties.class);
            assertThat(props.getRedis().getUri()).isEqualTo("redis://custom-host:6380");
            assertThat(props.getDefaultConcurrency()).isEqualTo(100);
            assertThat(props.isVirtualThreads()).isFalse();
        });
    }
}
