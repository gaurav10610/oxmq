package io.oxmq.concurrent;

/**
 * Strategy for thread dispatching in OxMQ workers.
 *
 * @deprecated Since 1.8.0 for removal in 2.0.0. Dispatching is streamlined via Virtual Threads on Java 21+
 *             with automatic platform thread fallback in {@link io.oxmq.concurrent.DispatcherExecutorFactory}.
 */
@Deprecated(since = "1.8.0", forRemoval = true)
public enum ExecutionStrategy {
    /**
     * Automatically use Virtual Threads if supported by the JVM runtime (Java 21+);
     * otherwise, fall back gracefully to platform threads (Java 17).
     */
    AUTO,

    /**
     * Strictly use Java 21+ Virtual Threads. Fails on Java 17.
     */
    VIRTUAL_THREADS,

    /**
     * Always use platform threads, even on Java 21+.
     */
    PLATFORM_THREADS
}
