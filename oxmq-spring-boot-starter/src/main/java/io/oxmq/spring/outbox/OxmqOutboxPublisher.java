package io.oxmq.spring.outbox;

import io.oxmq.OxmqQueue;
import io.oxmq.model.JobOptions;
import io.oxmq.serializer.JobSerializer;
import io.oxmq.spring.OxmqTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * High-performance background publisher that drains pending outbox records
 * and publishes them to Redis via OxMQ.
 *
 * <p>Guarantees At-Least-Once Delivery: If the application crashes before Redis ack,
 * the record remains PENDING and will be picked up upon restart.
 */
public class OxmqOutboxPublisher implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OxmqOutboxPublisher.class);

    private final OxmqOutboxRepository repository;
    private final OxmqTemplate template;
    private final JobSerializer serializer;
    private final int batchSize;
    private final long pollIntervalMs;
    private final Duration purgeRetention;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ScheduledExecutorService executor;
    private long lastPurgeTimestamp = 0;

    public OxmqOutboxPublisher(OxmqOutboxRepository repository,
                               OxmqTemplate template,
                               JobSerializer serializer) {
        this(repository, template, serializer, 50, 100, Duration.ofDays(1));
    }

    public OxmqOutboxPublisher(OxmqOutboxRepository repository,
                               OxmqTemplate template,
                               JobSerializer serializer,
                               int batchSize,
                               long pollIntervalMs,
                               Duration purgeRetention) {
        this.repository = Objects.requireNonNull(repository, "repository must not be null");
        this.template = Objects.requireNonNull(template, "template must not be null");
        this.serializer = Objects.requireNonNull(serializer, "serializer must not be null");
        this.batchSize = Math.max(1, batchSize);
        this.pollIntervalMs = Math.max(10, pollIntervalMs);
        this.purgeRetention = purgeRetention != null ? purgeRetention : Duration.ofDays(1);
    }

    @Override
    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Starting OxMQ Outbox Publisher (batchSize={}, pollInterval={}ms)", batchSize, pollIntervalMs);
            executor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "oxmq-outbox-publisher");
                t.setDaemon(true);
                return t;
            });
            executor.scheduleWithFixedDelay(this::publishCycle, 0, pollIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Stopping OxMQ Outbox Publisher");
            if (executor != null) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        // Start after queues and connections are ready (phase Integer.MAX_VALUE - 100)
        return Integer.MAX_VALUE - 100;
    }

    /**
     * Executes one drain iteration: reads pending records and publishes them.
     * Can also be called directly in unit tests for synchronous verification.
     */
    public int publishCycle() {
        try {
            List<OxmqOutboxRecord> pending = repository.findPending(batchSize);
            if (pending == null || pending.isEmpty()) {
                checkPeriodicPurge();
                return 0;
            }

            int publishedCount = 0;
            for (OxmqOutboxRecord record : pending) {
                try {
                    publishSingleRecord(record);
                    repository.markPublished(record.getId());
                    publishedCount++;
                } catch (Exception e) {
                    log.warn("Failed to publish outbox record [{}] to queue '{}': {}",
                            record.getId(), record.getQueueName(), e.getMessage());
                    repository.markFailed(record.getId(), e.getMessage());
                }
            }

            checkPeriodicPurge();
            return publishedCount;
        } catch (Throwable t) {
            log.warn("Unexpected error in OxMQ outbox publish cycle", t);
            return 0;
        }
    }

    @SuppressWarnings("unchecked")
    private void publishSingleRecord(OxmqOutboxRecord record) {
        JobOptions options = null;
        if (record.getOptionsJson() != null && !record.getOptionsJson().isBlank()) {
            try {
                options = serializer.deserialize(record.getOptionsJson(), JobOptions.class);
            } catch (Exception e) {
                log.debug("Failed to deserialize options for outbox record {}: {}", record.getId(), e.getMessage());
            }
        }
        if (options == null) {
            options = JobOptions.defaults();
        }

        if (record.getGroupKey() != null && !record.getGroupKey().isBlank()) {
            options.group(record.getGroupKey());
        }

        OxmqQueue<String> queue = template.getQueue(record.getQueueName(), String.class);
        queue.add(record.getJobName() != null ? record.getJobName() : "default",
                record.getPayloadJson() != null ? record.getPayloadJson() : "{}",
                options);
    }

    private void checkPeriodicPurge() {
        long now = System.currentTimeMillis();
        // Purge at most once every hour
        if (now - lastPurgeTimestamp > TimeUnit.HOURS.toMillis(1)) {
            lastPurgeTimestamp = now;
            try {
                long purged = repository.purgePublishedOlderThan(purgeRetention);
                if (purged > 0) {
                    log.info("Purged {} published outbox records older than {}", purged, purgeRetention);
                }
            } catch (Exception e) {
                log.debug("Outbox purge error: {}", e.getMessage());
            }
        }
    }
}
