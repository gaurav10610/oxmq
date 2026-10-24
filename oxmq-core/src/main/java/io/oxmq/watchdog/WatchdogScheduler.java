package io.oxmq.watchdog;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared daemon scheduler for OxMQ background watchdog timers (lock renewal, stalled job checking).
 * Eliminates dedicated platform thread pool proliferation across distributed workers.
 */
public final class WatchdogScheduler {

    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger(1);

    private static final ScheduledExecutorService SHARED = Executors.newScheduledThreadPool(
            Math.max(2, Math.min(4, Runtime.getRuntime().availableProcessors())),
            r -> {
                Thread t = new Thread(r, "oxmq-watchdog-" + THREAD_COUNTER.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
    );

    private WatchdogScheduler() {}

    public static ScheduledExecutorService sharedScheduler() {
        return SHARED;
    }
}
