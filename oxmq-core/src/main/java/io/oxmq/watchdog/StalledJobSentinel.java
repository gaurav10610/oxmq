package io.oxmq.watchdog;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.oxmq.metrics.OxmqMetrics;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watchdog service that scans for orphaned/hung active jobs whose worker lock has expired,
 * re-queueing them or failing them safely.
 */
public class StalledJobSentinel implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(StalledJobSentinel.class);

    private final StatefulRedisConnection<String, String> connection;
    private final String queuePrefix;
    private final String queueName;
    private final OxmqMetrics metrics;
    private final ScheduledExecutorService scheduler;
    private final int maxStalledCount;

    public StalledJobSentinel(StatefulRedisConnection<String, String> connection, String queuePrefix,
                              String queueName, OxmqMetrics metrics, long checkIntervalMs, int maxStalledCount) {
        this.connection = connection;
        this.queuePrefix = queuePrefix;
        this.queueName = queueName;
        this.metrics = metrics;
        this.maxStalledCount = maxStalledCount;

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "oxmq-stalled-sentinel-" + queueName);
            t.setDaemon(true);
            return t;
        });
        this.scheduler.scheduleWithFixedDelay(this::checkStalledJobs, checkIntervalMs, checkIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void checkStalledJobs() {
        try {
            RedisCommands<String, String> sync = connection.sync();
            String activeKey = queuePrefix + ":active";
            String waitKey = queuePrefix + ":wait";
            String failedKey = queuePrefix + ":failed";

            List<String> activeJobs = sync.lrange(activeKey, 0, -1);
            if (activeJobs == null || activeJobs.isEmpty()) {
                return;
            }

            for (String jobId : activeJobs) {
                String lockKey = queuePrefix + ":" + jobId + ":lock";
                String lockExists = sync.get(lockKey);

                if (lockExists == null) {
                    // Lock has expired! Worker is dead or stalled.
                    String jobKey = queuePrefix + ":" + jobId;
                    String stalledCountStr = sync.hget(jobKey, "stalledCount");
                    int stalledCount = stalledCountStr != null ? Integer.parseInt(stalledCountStr) : 0;
                    stalledCount++;

                    sync.hset(jobKey, "stalledCount", String.valueOf(stalledCount));
                    sync.lrem(activeKey, 0, jobId);

                    if (stalledCount <= maxStalledCount) {
                        log.warn("Job {} in queue {} stalled (count {}). Re-queueing to wait list.", jobId, queueName, stalledCount);
                        sync.lpush(waitKey, jobId);
                        if (metrics != null) {
                            metrics.recordJobStalled(queueName);
                        }
                    } else {
                        log.error("Job {} in queue {} exceeded max stalled count ({}). Moving to failed.", jobId, queueName, maxStalledCount);
                        sync.hmset(jobKey, java.util.Map.of(
                                "failedReason", "Job stalled more than " + maxStalledCount + " times",
                                "finishedOn", String.valueOf(System.currentTimeMillis())
                        ));
                        sync.zadd(failedKey, System.currentTimeMillis(), jobId);
                        if (metrics != null) {
                            metrics.recordJobFailed(queueName, null, "JobStalledException");
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Error scanning stalled jobs for queue {}", queueName, e);
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
    }
}
