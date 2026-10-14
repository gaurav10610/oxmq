package io.oxmq.spring.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Binds a listener method parameter to a Job metadata header or attribute.
 *
 * <p>Supported header values include:
 * <ul>
 *     <li>{@code "id"} or {@code "jobId"} - Job ID (String)</li>
 *     <li>{@code "name"} or {@code "jobName"} - Job Name (String)</li>
 *     <li>{@code "timestamp"} - Enqueue timestamp in millis (long / Long)</li>
 *     <li>{@code "attemptsMade"} - Number of attempts made so far (int / Integer)</li>
 *     <li>{@code "queue"} or {@code "queueName"} - Name of the queue (String)</li>
 *     <li>{@code "parentKey"} - Parent job Redis key if part of a DAG flow (String)</li>
 * </ul>
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Header {

    /**
     * The name of the header or metadata attribute to bind.
     */
    String value() default "";

    /**
     * Whether the header is required. Defaults to false.
     */
    boolean required() default false;
}
