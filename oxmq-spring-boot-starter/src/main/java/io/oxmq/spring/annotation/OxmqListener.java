package io.oxmq.spring.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as an OxMQ background job listener.
 * The method can accept {@code Job<T>} or the raw payload object {@code T}.
 */
@Target({ElementType.METHOD, ElementType.TYPE, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OxmqListener {

    /**
     * Custom namespace prefix for this queue.
     * When left empty, defaults to the service-level namespace configured in {@code oxmq.prefix} or {@code spring.application.name}.
     * Supports property placeholders, e.g. {@code "${app.queue.prefix}"}.
     */
    String prefix() default "";

    /**
     * The name of the queue to listen to. Supports property placeholders, e.g. {@code "${app.queue.name}"}.
     */
    String queue();

    /**
     * Max concurrent jobs to process. Defaults to 20.
     */
    int concurrency() default 20;

    /**
     * Property placeholder for concurrency, e.g. {@code "${app.concurrency:20}"}.
     */
    String concurrencyString() default "";

    /**
     * Whether to execute jobs on Java 21 Virtual Threads (Loom). Defaults to true.
     */
    boolean virtualThreads() default true;

    /**
     * Lock duration in milliseconds. Defaults to 30,000 ms (30 seconds).
     */
    long lockDurationMs() default 30_000;

    /**
     * Polling interval in milliseconds. Defaults to 50 ms.
     */
    long pollIntervalMs() default 50;

    /**
     * Sliding window rate limit max jobs (0 for unlimited).
     */
    int rateLimitMax() default 0;

    /**
     * Sliding window duration in milliseconds.
     */
    long rateLimitDurationMs() default 0;
}
