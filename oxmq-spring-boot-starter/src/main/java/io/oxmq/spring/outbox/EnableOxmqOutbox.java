package io.oxmq.spring.outbox;

import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Enables OxMQ Transactional Outbox pattern support in Spring Boot applications.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(OxmqOutboxAutoConfiguration.class)
public @interface EnableOxmqOutbox {
}
