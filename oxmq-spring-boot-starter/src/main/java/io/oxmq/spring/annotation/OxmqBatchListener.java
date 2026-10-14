package io.oxmq.spring.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as an OxMQ batch job listener.
 * The method can accept {@code List<Job<T>>} or {@code List<T>}.
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OxmqBatchListener {

    /**
     * The name of the queue to listen to. Supports property placeholders, e.g. {@code "${app.queue.name}"}.
     */
    String queue();

    /**
     * Number of jobs to fetch and process in each batch chunk. Defaults to 10.
     */
    int batchSize() default 10;

    /**
     * Property placeholder for batch size, e.g. {@code "${app.batch.size:10}"}.
     */
    String batchSizeString() default "";

    /**
     * Whether to execute jobs on Virtual Threads if available (Java 21+). Defaults to true.
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
}
