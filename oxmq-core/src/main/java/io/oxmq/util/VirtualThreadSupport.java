package io.oxmq.util;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility providing runtime detection and seamless abstraction for Java 21 Virtual Threads
 * while maintaining backward compatibility with Java 17 LTS.
 *
 * Follows the Factory and Strategy patterns for thread dispatching.
 */
public final class VirtualThreadSupport {

    private static final Logger log = LoggerFactory.getLogger(VirtualThreadSupport.class);

    private static final boolean VIRTUAL_THREADS_AVAILABLE;
    private static final MethodHandle VIRTUAL_EXECUTOR_HANDLE;
    private static final MethodHandle IS_VIRTUAL_HANDLE;

    static {
        boolean available = false;
        MethodHandle executorHandle = null;
        MethodHandle isVirtualHandle = null;

        try {
            MethodHandles.Lookup lookup = MethodHandles.publicLookup();
            // Lookup Executors.newVirtualThreadPerTaskExecutor() introduced in Java 21
            executorHandle = lookup.findStatic(
                    Executors.class,
                    "newVirtualThreadPerTaskExecutor",
                    MethodType.methodType(ExecutorService.class)
            );

            // Lookup Thread.isVirtual() introduced in Java 21
            isVirtualHandle = lookup.findVirtual(
                    Thread.class,
                    "isVirtual",
                    MethodType.methodType(boolean.class)
            );

            available = true;
            log.debug("Java 21+ Virtual Threads runtime detected and enabled in OxMQ.");
        } catch (Throwable t) {
            available = false;
            executorHandle = null;
            isVirtualHandle = null;
            log.debug("Virtual Threads not available on this JVM runtime (Java < 21). Falling back to platform threads.");
        }

        VIRTUAL_THREADS_AVAILABLE = available;
        VIRTUAL_EXECUTOR_HANDLE = executorHandle;
        IS_VIRTUAL_HANDLE = isVirtualHandle;
    }

    private VirtualThreadSupport() {}

    /**
     * Returns true if Virtual Threads (Project Loom) are available on the current JVM runtime.
     */
    public static boolean isSupported() {
        return VIRTUAL_THREADS_AVAILABLE;
    }

    /**
     * Checks whether the specified thread is a Virtual Thread.
     * Returns false on Java 17 or if the thread is a platform thread.
     */
    public static boolean isVirtual(Thread thread) {
        if (thread == null) return false;
        if (IS_VIRTUAL_HANDLE != null) {
            try {
                return (boolean) IS_VIRTUAL_HANDLE.invokeExact(thread);
            } catch (Throwable ignored) {
                // Fallback check
            }
        }
        return thread.getClass().getName().contains("VirtualThread");
    }

    /**
     * Creates a new Virtual Thread per task executor if supported by the JVM runtime.
     *
     * @throws UnsupportedOperationException if virtual threads are not supported
     */
    public static ExecutorService createVirtualThreadExecutor() {
        if (!VIRTUAL_THREADS_AVAILABLE || VIRTUAL_EXECUTOR_HANDLE == null) {
            throw new UnsupportedOperationException("Virtual Threads are not supported on this JVM runtime (Java < 21)");
        }
        try {
            return (ExecutorService) VIRTUAL_EXECUTOR_HANDLE.invokeExact();
        } catch (Throwable t) {
            throw new IllegalStateException("Failed to instantiate Virtual Thread executor", t);
        }
    }

    /**
     * Creates an appropriate {@link ExecutorService} for task dispatching.
     * If {@code preferVirtual} is true and virtual threads are supported on the JVM,
     * a Virtual Thread per task executor is returned.
     * Otherwise, an optimized cached platform thread pool is created with descriptive daemon threads.
     *
     * @param preferVirtual     whether to prefer virtual threads if available
     * @param threadNamePrefix  prefix for thread names
     * @return an {@link ExecutorService} instance
     */
    public static ExecutorService createDispatcherExecutor(boolean preferVirtual, String threadNamePrefix) {
        if (preferVirtual && isSupported()) {
            try {
                return createVirtualThreadExecutor();
            } catch (Throwable t) {
                log.warn("Could not create virtual thread executor despite capability check, falling back to platform threads", t);
            }
        }

        AtomicLong threadCounter = new AtomicLong(1);
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable, threadNamePrefix + "-" + threadCounter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };

        return Executors.newCachedThreadPool(threadFactory);
    }
}
