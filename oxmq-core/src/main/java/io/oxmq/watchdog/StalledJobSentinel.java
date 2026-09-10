package io.oxmq.watchdog;

import io.lettuce.core.api.StatefulRedisConnection;
import io.oxmq.client.QueueKeys;
import io.oxmq.lua.BullScripts;
import io.oxmq.metrics.OxmqMetrics;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watchdog service that scans for stalled active jobs whose worker lock has expired,
 * re-queueing or failing them using BullMQ moveStalledJobsToWait Lua script.
 */
public class StalledJobSentinel implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(StalledJobSentinel.class);

    private final StatefulRedisConnection<String, byte[]> connection;
    private final BullScripts bullScripts;
    private final QueueKeys queueKeys;
    private final OxmqMetrics metrics;
    private final ScheduledExecutorService scheduler;
    private final int maxStalledCount;
    private final long checkIntervalMs;

    public StalledJobSentinel(StatefulRedisConnection<String, byte[]> connection, BullScripts bullScripts,
                              QueueKeys queueKeys, OxmqMetrics metrics, long checkIntervalMs, int maxStalledCount) {
        this.connection = connection;
        this.bullScripts = bullScripts != null ? bullScripts : new BullScripts();
        this.queueKeys = queueKeys;
        this.metrics = metrics;
        this.maxStalledCount = maxStalledCount;
        this.checkIntervalMs = checkIntervalMs;

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "oxmq-stalled-sentinel-" + queueKeys.getQueueName());
            t.setDaemon(true);
            return t;
        });
        this.scheduler.scheduleWithFixedDelay(this::checkStalledJobs, checkIntervalMs, checkIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void checkStalledJobs() {
        try {
            List<Object> result = bullScripts.moveStalledJobsToWait(connection, queueKeys, maxStalledCount, (int) checkIntervalMs);
            if (result != null && result.size() >= 2) {
                @SuppressWarnings("unchecked")
                List<Object> failedJobs = (List<Object>) result.get(0);
                @SuppressWarnings("unchecked")
                List<Object> stalledJobs = (List<Object>) result.get(1);

                if (failedJobs != null && !failedJobs.isEmpty()) {
                    for (Object id : failedJobs) {
                        log.warn("Job {} on queue {} exceeded max stalled attempts and was moved to failed", id, queueKeys.getQueueName());
                    }
                }
                if (stalledJobs != null && !stalledJobs.isEmpty()) {
                    for (Object id : stalledJobs) {
                        log.info("Job {} on queue {} stalled and was re-queued to wait", id, queueKeys.getQueueName());
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Error checking stalled jobs in queue {}", queueKeys.getQueueName(), e);
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
