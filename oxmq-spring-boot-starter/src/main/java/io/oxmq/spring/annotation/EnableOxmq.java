package io.oxmq.spring.annotation;

import io.oxmq.spring.OxmqAutoConfiguration;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

/**
 * Enables OxMQ background job processing and scans for {@link OxmqListener} methods.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(OxmqAutoConfiguration.class)
public @interface EnableOxmq {
}
