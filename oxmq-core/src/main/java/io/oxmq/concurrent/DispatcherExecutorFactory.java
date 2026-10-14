package io.oxmq.concurrent;

import io.oxmq.util.VirtualThreadSupport;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory pattern implementation for creating worker dispatch executors.
 * Supports auto-detection of Java 21+ Virtual Threads with graceful fallback to Java 17 platform threads.
 */
public final class DispatcherExecutorFactory {

    private static final Logger log = LoggerFactory.getLogger(DispatcherExecutorFactory.class);

    private DispatcherExecutorFactory() {}

    /**
     * Creates an {@link ExecutorService} based on the preferred strategy and thread prefix.
     *
     * @param strategy         the execution strategy
     * @param threadNamePrefix prefix for worker threads
     * @return configured ExecutorService
     */
    public static ExecutorService createExecutor(ExecutionStrategy strategy, String threadNamePrefix) {
        if (strategy == null) {
            strategy = ExecutionStrategy.AUTO;
        }

        switch (strategy) {
            case VIRTUAL_THREADS:
                if (!VirtualThreadSupport.isSupported()) {
                    throw new UnsupportedOperationException(
                            "ExecutionStrategy.VIRTUAL_THREADS requested, but Virtual Threads are not available on this JVM runtime (< Java 21)");
                }
                return VirtualThreadSupport.createVirtualThreadExecutor();

            case PLATFORM_THREADS:
                return VirtualThreadSupport.createDispatcherExecutor(false, threadNamePrefix);

            case AUTO:
            default:
                boolean vThreadSupported = VirtualThreadSupport.isSupported();
                if (log.isDebugEnabled()) {
                    log.debug("Creating worker dispatcher with AUTO strategy (Virtual Threads supported: {})", vThreadSupported);
                }
                return VirtualThreadSupport.createDispatcherExecutor(vThreadSupported, threadNamePrefix);
        }
    }

    /**
     * Creates an {@link ExecutorService} given the boolean flag {@code useVirtualThreads}.
     * If {@code useVirtualThreads} is true:
     * - On Java 21+: Uses Virtual Threads.
     * - On Java 17: Falls back gracefully to platform threads and logs an info/debug message.
     */
    public static ExecutorService createExecutor(boolean useVirtualThreads, String threadNamePrefix) {
        if (useVirtualThreads) {
            if (VirtualThreadSupport.isSupported()) {
                return VirtualThreadSupport.createVirtualThreadExecutor();
            } else {
                log.info("Virtual Threads requested for worker '{}', but not supported by JVM (< Java 21). Falling back to platform threads.",
                        threadNamePrefix);
                return VirtualThreadSupport.createDispatcherExecutor(false, threadNamePrefix);
            }
        } else {
            return VirtualThreadSupport.createDispatcherExecutor(false, threadNamePrefix);
        }
    }
}
