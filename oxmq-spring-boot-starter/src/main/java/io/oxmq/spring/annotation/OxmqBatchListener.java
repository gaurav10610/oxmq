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
     * Number of jobs to fetch and process in each batch chunk. Defaults to 10.
     */
    int batchSize() default 10;

    /**
     * Property placeholder for batch size, e.g. {@code "${app.batch.size:10}"}.
     */
    String batchSizeString() default "";

    /**
     * Maximum cumulative payload bytes for a single batch before dispatching.
     * Defaults to 0 (unlimited byte size, bounded only by batchSize).
     */
    long maxBatchBytes() default 0;

    /**
     * Property placeholder for maxBatchBytes, e.g. {@code "${app.batch.max-bytes:1048576}"}.
     */
    String maxBatchBytesString() default "";

    /**
     * Whether to execute jobs on Virtual Threads if available (Java 21+). Defaults to true.
     */
    boolean virtualThreads() default true;

    /**
     * Lock duration in milliseconds. Defaults to 30,000 ms (30 seconds).
     */
    long lockDurationMs() default 30_000;

    /**
     * Maximum number of concurrent batches processed simultaneously. Defaults to 1.
     */
    int concurrency() default 1;

    /**
     * Property placeholder for concurrency, e.g. {@code "${app.batch.concurrency:1}"}.
     */
    String concurrencyString() default "";

    /**
     * Polling interval in milliseconds. Defaults to 50 ms.
     */
    long pollIntervalMs() default 50;

    /**
     * Minimum batch size to accumulate before dispatching, unless batchTimeoutMs is exceeded.
     * Defaults to 1.
     */
    int minBatchSize() default 1;

    /**
     * Property placeholder for minBatchSize, e.g. {@code "${app.batch.min-size:5}"}.
     */
    String minBatchSizeString() default "";

    /**
     * Maximum accumulation duration in milliseconds before dispatching even if batchSize has not been reached.
     * Defaults to 0 (immediate dispatch once available).
     */
    long batchTimeoutMs() default 0;

    /**
     * Property placeholder for batchTimeoutMs, e.g. {@code "${app.batch.timeout:200}"}.
     */
    String batchTimeoutMsString() default "";

    /**
     * Whether all jobs in a single batch must belong to the exact same virtual group (group affinity).
     * Defaults to false.
     */
    boolean groupAffinity() default false;

    /**
     * Property placeholder for groupAffinity, e.g. {@code "${app.batch.group-affinity:true}"}.
     */
    String groupAffinityString() default "";

    /**
     * Maximum concurrency per virtual group (0 = unlimited).
     */
    int groupConcurrency() default 0;

    /**
     * Property placeholder for groupConcurrency, e.g. {@code "${app.batch.group-concurrency:2}"}.
     */
    String groupConcurrencyString() default "";
}
