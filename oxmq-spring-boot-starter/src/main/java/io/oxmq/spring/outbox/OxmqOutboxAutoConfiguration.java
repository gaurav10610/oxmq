package io.oxmq.spring.outbox;

import io.oxmq.serializer.JobSerializer;
import io.oxmq.spring.OxmqAutoConfiguration;
import io.oxmq.spring.OxmqTemplate;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

@AutoConfiguration(after = OxmqAutoConfiguration.class)
public class OxmqOutboxAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(OxmqOutboxRepository.class)
    public OxmqOutboxRepository inMemoryOxmqOutboxRepository() {
        return new InMemoryOxmqOutboxRepository();
    }

    @Bean
    @ConditionalOnMissingBean(OxmqOutboxService.class)
    public OxmqOutboxService oxmqOutboxService(OxmqOutboxRepository repository, JobSerializer serializer) {
        return new DefaultOxmqOutboxService(repository, serializer);
    }

    @Bean
    @ConditionalOnMissingBean(OxmqOutboxPublisher.class)
    @ConditionalOnProperty(prefix = "oxmq.outbox", name = "enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnBean({OxmqOutboxRepository.class, OxmqTemplate.class})
    public OxmqOutboxPublisher oxmqOutboxPublisher(OxmqOutboxRepository repository,
                                                   OxmqTemplate template,
                                                   JobSerializer serializer) {
        return new OxmqOutboxPublisher(repository, template, serializer);
    }
}
