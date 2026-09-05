package io.oxmq.watchdog;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.oxmq.lua.LuaScript;
import io.oxmq.lua.LuaScriptManager;
import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background heartbeat service that periodically extends Redis locks for long-running active jobs.
 */
public class LockExtender implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(LockExtender.class);

    private final StatefulRedisConnection<String, String> connection;
    private final LuaScriptManager scriptManager;
    private final String queuePrefix;
    private final String token;
    private final long lockDurationMs;
    private final Set<String> activeJobIds = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService scheduler;

    public LockExtender(StatefulRedisConnection<String, String> connection, LuaScriptManager scriptManager,
                        String queuePrefix, String token, long lockDurationMs) {
        this.connection = connection;
        this.scriptManager = scriptManager;
        this.queuePrefix = queuePrefix;
        this.token = token;
        this.lockDurationMs = lockDurationMs;

        long intervalMs = Math.max(1000, lockDurationMs / 2);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "oxmq-lock-extender-" + queuePrefix);
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
                String lockKey = queuePrefix + ":" + jobId + ":lock";
                Long result = scriptManager.eval(connection, LuaScript.EXTEND_LOCK, ScriptOutputType.INTEGER,
                        new String[]{lockKey},
                        token,
                        String.valueOf(lockDurationMs)
                );
                if (result == null || result == 0L) {
                    log.warn("Failed to extend lock for job {} in queue {}", jobId, queuePrefix);
                    activeJobIds.remove(jobId);
                } else {
                    log.trace("Extended lock for job {} for {} ms", jobId, lockDurationMs);
                }
            } catch (Exception e) {
                log.warn("Error extending lock for job {} in queue {}", jobId, queuePrefix, e);
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
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
        activeJobIds.clear();
    }
}
