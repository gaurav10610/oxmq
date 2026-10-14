package io.oxmq.spring.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that marks a method on a class-level {@link OxmqListener} as a handler
 * for a specific job payload type or default fallback.
 *
 * <p>Follows the Spring standard multi-method listener design pattern (like {@code @KafkaHandler}).
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OxmqHandler {

    /**
     * Optional job name to match (e.g. "payment", "process-invoice").
     * If blank, matching is performed by payload class type.
     */
    String name() default "";

    /**
     * Whether this method is the default handler when no other method matches.
     */
    boolean isDefault() default false;
}
