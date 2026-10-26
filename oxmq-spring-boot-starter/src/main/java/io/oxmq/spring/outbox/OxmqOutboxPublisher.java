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
    private final java.util.concurrent.atomic.AtomicLong currentDelayMs = new java.util.concurrent.atomic.AtomicLong(100);
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
            currentDelayMs.set(pollIntervalMs);
            executor.schedule(this::runAdaptiveCycle, 0, TimeUnit.MILLISECONDS);
        }
    }

    private void runAdaptiveCycle() {
        if (!running.get()) return;
        try {
            int count = publishCycle();
            if (count > 0) {
                currentDelayMs.set(pollIntervalMs);
                if (running.get() && executor != null && !executor.isShutdown()) {
                    executor.schedule(this::runAdaptiveCycle, 0, TimeUnit.MILLISECONDS);
                }
            } else {
                long next = Math.min(1000, Math.max(pollIntervalMs, currentDelayMs.get() * 2));
                currentDelayMs.set(next);
                if (running.get() && executor != null && !executor.isShutdown()) {
                    executor.schedule(this::runAdaptiveCycle, next, TimeUnit.MILLISECONDS);
                }
            }
        } catch (Throwable t) {
            if (running.get() && executor != null && !executor.isShutdown()) {
                executor.schedule(this::runAdaptiveCycle, pollIntervalMs, TimeUnit.MILLISECONDS);
            }
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
            // Group records by queue name to enable pipelined addBulk per queue
            java.util.Map<String, List<OxmqOutboxRecord>> groupedByQueue = new java.util.LinkedHashMap<>();
            for (OxmqOutboxRecord record : pending) {
                groupedByQueue.computeIfAbsent(record.getQueueName(), k -> new java.util.ArrayList<>()).add(record);
            }

            for (java.util.Map.Entry<String, List<OxmqOutboxRecord>> entry : groupedByQueue.entrySet()) {
                String qName = entry.getKey();
                List<OxmqOutboxRecord> records = entry.getValue();
                publishedCount += publishBatchForQueue(qName, records);
            }

            checkPeriodicPurge();
            return publishedCount;
        } catch (Throwable t) {
            log.warn("Unexpected error in OxMQ outbox publish cycle", t);
            return 0;
        }
    }

    private int publishBatchForQueue(String queueName, List<OxmqOutboxRecord> records) {
        if (records.size() == 1) {
            OxmqOutboxRecord record = records.get(0);
            try {
                publishSingleRecord(record);
                repository.markPublished(record.getId());
                return 1;
            } catch (Exception e) {
                log.warn("Failed to publish outbox record [{}] to queue '{}': {}",
                        record.getId(), record.getQueueName(), e.getMessage());
                repository.markFailed(record.getId(), e.getMessage());
                return 0;
            }
        }

        // Multiple records for the same queue: use pipelined addBulk
        try {
            List<io.oxmq.model.JobRequest<String>> requests = new java.util.ArrayList<>(records.size());
            List<String> ids = new java.util.ArrayList<>(records.size());
            for (OxmqOutboxRecord record : records) {
                JobOptions options = parseOptions(record);
                requests.add(new io.oxmq.model.JobRequest<>(
                        record.getJobName() != null ? record.getJobName() : "default",
                        record.getPayloadJson() != null ? record.getPayloadJson() : "{}",
                        options));
                ids.add(record.getId());
            }

            OxmqQueue<String> queue = template.getQueue(queueName, String.class);
            queue.addBulk(requests);
            repository.markAllPublished(ids);
            return records.size();
        } catch (Exception batchEx) {
            log.warn("Batch publishing via addBulk failed for queue '{}', falling back to individual delivery: {}",
                    queueName, batchEx.getMessage());
            int success = 0;
            for (OxmqOutboxRecord record : records) {
                try {
                    publishSingleRecord(record);
                    repository.markPublished(record.getId());
                    success++;
                } catch (Exception e) {
                    log.warn("Failed to publish outbox record [{}] to queue '{}': {}",
                            record.getId(), record.getQueueName(), e.getMessage());
                    repository.markFailed(record.getId(), e.getMessage());
                }
            }
            return success;
        }
    }

    private JobOptions parseOptions(OxmqOutboxRecord record) {
        String optsJson = record.getOptionsJson();
        JobOptions options = null;
        if (optsJson != null && !optsJson.isBlank() && !"{}".equals(optsJson)) {
            try {
                options = serializer.deserialize(optsJson, JobOptions.class);
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
        return options;
    }

    private void publishSingleRecord(OxmqOutboxRecord record) {
        JobOptions options = parseOptions(record);
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
