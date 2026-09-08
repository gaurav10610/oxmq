package io.oxmq.spring.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Shorthand alias annotation for {@link OxmqListener}.
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OxListener {

    /**
     * The name of the queue to listen to.
     */
    String queue() default "";

    /**
     * Max concurrent jobs to process on Virtual Threads. Defaults to 20.
     */
    int concurrency() default 20;

    /**
     * Whether to execute jobs on Java 21 Virtual Threads (Loom). Defaults to true.
     */
    boolean virtualThreads() default true;

    /**
     * Alias for virtualThreads. Defaults to true.
     */
    boolean useVirtualThreads() default true;

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
