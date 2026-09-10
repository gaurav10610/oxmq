package io.oxmq.watchdog;

import io.lettuce.core.api.StatefulRedisConnection;
import io.oxmq.client.QueueKeys;
import io.oxmq.lua.BullScripts;
import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background heartbeat service that periodically extends Redis locks for long-running active jobs
 * using BullMQ extendLock Lua script.
 */
public class LockExtender implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(LockExtender.class);

    private final StatefulRedisConnection<String, byte[]> connection;
    private final BullScripts bullScripts;
    private final QueueKeys queueKeys;
    private final String token;
    private final long lockDurationMs;
    private final Set<String> activeJobIds = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService scheduler;

    public LockExtender(StatefulRedisConnection<String, byte[]> connection, BullScripts bullScripts,
                        QueueKeys queueKeys, String token, long lockDurationMs) {
        this.connection = connection;
        this.bullScripts = bullScripts != null ? bullScripts : new BullScripts();
        this.queueKeys = queueKeys;
        this.token = token;
        this.lockDurationMs = lockDurationMs;

        long intervalMs = Math.max(1000, lockDurationMs / 2);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "oxmq-lock-extender-" + queueKeys.getQueueName());
            t.setDaemon(true);
            return t;
        });
        this.scheduler.scheduleWithFixedDelay(this::extendAllLocks, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void registerJob(String jobId) {
        activeJobIds.add(jobId);
    }

    public void unregisterJob(String jobId) {
        activeJobIds.remove(jobId);
    }

    private void extendAllLocks() {
        for (String jobId : activeJobIds) {
            try {
                boolean extended = bullScripts.extendLock(connection, queueKeys, jobId, token, (int) lockDurationMs);
                if (!extended) {
                    log.warn("Failed to extend lock for job {} in queue {}", jobId, queueKeys.getQueueName());
                    activeJobIds.remove(jobId);
                } else {
                    log.trace("Extended lock for job {} for {} ms", jobId, lockDurationMs);
                }
            } catch (Exception e) {
                log.warn("Error extending lock for job {} in queue {}", jobId, queueKeys.getQueueName(), e);
            }
        }
    }

    @Override
    public void close() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
