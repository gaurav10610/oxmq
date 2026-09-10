package io.oxmq;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.oxmq.client.QueueKeys;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.lua.BullScripts;
import io.oxmq.lua.LuaScriptManager;
import io.oxmq.metrics.OxmqMetrics;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.serializer.JacksonJobSerializer;
import io.oxmq.serializer.JobSerializer;
import io.oxmq.watchdog.LockExtender;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-throughput batch worker engine for consuming chunks of jobs atomically.
 * Uses BullMQ moveToActive and moveToFinished scripts.
 *
 * @param <T> Payload data type
 */
public class OxmqBatchWorker<T> implements Worker<T> {

    private static final Logger log = LoggerFactory.getLogger(OxmqBatchWorker.class);

    private final String queueName;
    private final String prefix;
    private final QueueKeys queueKeys;
    private final String workerId;
    private final BatchJobProcessor<T, ?> processor;
    private final Class<T> payloadClass;
    private final int batchSize;
    private final boolean useVirtualThreads;
    private final long lockDurationMs;
    private final long drainTimeoutMs;
    private final long pollIntervalMs;

    private final RedisConnectionManager connectionManager;
    private final LuaScriptManager scriptManager;
    private final BullScripts bullScripts;
    private final JobSerializer serializer;
    private final OxmqMetrics metrics;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean paused = new AtomicBoolean(false);

    private ExecutorService dispatcherExecutor;
    private Thread pollerThread;
    private LockExtender lockExtender;

    private OxmqBatchWorker(Builder<T> builder) {
        this.queueName = Objects.requireNonNull(builder.queueName, "queueName must not be null");
        this.prefix = "bull:" + queueName;
        this.queueKeys = new QueueKeys("bull", queueName);
        this.workerId = "batch-worker:" + UUID.randomUUID();
        this.processor = Objects.requireNonNull(builder.processor, "processor must not be null");
        this.payloadClass = builder.payloadClass;
        this.batchSize = Math.max(1, builder.batchSize);
        this.useVirtualThreads = builder.useVirtualThreads;
        this.lockDurationMs = builder.lockDurationMs;
        this.drainTimeoutMs = builder.drainTimeoutMs;
        this.pollIntervalMs = builder.pollIntervalMs;

        this.connectionManager = Objects.requireNonNull(builder.connectionManager, "connectionManager must not be null");
        this.scriptManager = builder.scriptManager != null ? builder.scriptManager : new LuaScriptManager();
        this.bullScripts = new BullScripts(this.scriptManager);
        this.serializer = builder.serializer != null ? builder.serializer : new JacksonJobSerializer();
        this.metrics = builder.metrics != null ? builder.metrics : new OxmqMetrics();
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    @Override
    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Starting OxMQ Batch Worker [{}] for queue '{}' with batchSize {} (Virtual Threads: {})",
                    workerId, queueName, batchSize, useVirtualThreads);

            if (useVirtualThreads) {
                this.dispatcherExecutor = Executors.newVirtualThreadPerTaskExecutor();
            } else {
                this.dispatcherExecutor = Executors.newCachedThreadPool(r -> {
                    Thread t = new Thread(r, "oxmq-batch-worker-" + queueName);
                    t.setDaemon(true);
                    return t;
                });
            }

            this.lockExtender = new LockExtender(connectionManager.createDedicatedBinaryConnection(), bullScripts,
                    queueKeys, workerId, lockDurationMs);

            this.pollerThread = new Thread(this::pollLoop, "oxmq-batch-poller-" + queueName);
            this.pollerThread.setDaemon(true);
            this.pollerThread.start();
        }
    }

    private void pollLoop() {
        StatefulRedisConnection<String, byte[]> pollerConn = connectionManager.createDedicatedBinaryConnection();

        while (running.get()) {
            try {
                if (paused.get()) {
                    TimeUnit.MILLISECONDS.sleep(pollIntervalMs);
                    continue;
                }

                long now = System.currentTimeMillis();
                List<Job<T>> jobs = new ArrayList<>();

                for (int i = 0; i < batchSize; i++) {
                    List<Object> rawResult = bullScripts.moveToActive(pollerConn, queueKeys, workerId,
                            (int) lockDurationMs, null, workerId, now);

                    if (rawResult == null || rawResult.size() < 2 || rawResult.get(1) == null) {
                        break;
                    }

                    Object idObj = rawResult.get(1);
                    String jobId = idObj instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : idObj.toString();
                    if (jobId.isEmpty() || "0".equals(jobId)) {
                        break;
                    }

                    @SuppressWarnings("unchecked")
                    List<?> hashFlat = rawResult.get(0) instanceof List<?> list ? list : List.of();
                    Map<String, String> fields = parseHashFields(hashFlat);
                    Job<T> job = buildJobFromFields(jobId, fields);
                    jobs.add(job);
                    lockExtender.registerJob(jobId);
                }

                if (jobs.isEmpty()) {
                    TimeUnit.MILLISECONDS.sleep(pollIntervalMs);
                    continue;
                }

                dispatcherExecutor.submit(() -> executeBatch(jobs));

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (!running.get() || Thread.currentThread().isInterrupted() || e instanceof io.lettuce.core.RedisCommandInterruptedException) {
                    Thread.currentThread().interrupt();
                    break;
                }
                log.warn("Error in OxMQ batch poller loop for queue {}", queueName, e);
                try {
                    TimeUnit.MILLISECONDS.sleep(pollIntervalMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        try {
            pollerConn.close();
        } catch (Exception ignored) {}
    }

    private void executeBatch(List<Job<T>> jobs) {
        long startTime = System.nanoTime();
        try {
            log.debug("Executing batch of {} jobs on queue {}", jobs.size(), queueName);
            Object result = processor.process(jobs);

            long durationNanos = System.nanoTime() - startTime;
            Duration duration = Duration.ofNanos(durationNanos);

            completeBatch(jobs, result, duration);
            for (Job<T> job : jobs) {
                metrics.recordJobCompleted(queueName, duration);
            }
            log.debug("Completed batch of {} jobs in {} ms", jobs.size(), duration.toMillis());

        } catch (Throwable t) {
            long durationNanos = System.nanoTime() - startTime;
            Duration duration = Duration.ofNanos(durationNanos);

            failBatch(jobs, t, duration);
            for (Job<T> job : jobs) {
                metrics.recordJobFailed(queueName, duration, t.getClass().getSimpleName());
            }
            log.warn("Batch of {} jobs failed on queue {}: {}", jobs.size(), queueName, t.getMessage(), t);
        } finally {
            for (Job<T> job : jobs) {
                lockExtender.unregisterJob(job.getId());
            }
        }
    }

    private void completeBatch(List<Job<T>> jobs, Object result, Duration duration) {
        String serializedResult = serializer.serialize(result);
        for (Job<T> job : jobs) {
            try {
                JobOptions opts = job.getOpts();
                bullScripts.moveToFinished(
                    connectionManager.getBinaryConnection(),
                    queueKeys,
                    job.getId(),
                    serializedResult,
                    "returnvalue",
                    "completed",
                    workerId,
                    false,
                    opts != null ? opts.toMap() : Map.of()
                );
            } catch (Exception e) {
                log.warn("Failed to mark job {} completed in batch: {}", job.getId(), e.getMessage());
            }
        }
    }

    private void failBatch(List<Job<T>> jobs, Throwable error, Duration duration) {
        StringWriter sw = new StringWriter();
        error.printStackTrace(new PrintWriter(sw));
        String stackTrace = sw.toString();

        for (Job<T> job : jobs) {
            try {
                JobOptions opts = job.getOpts();
                bullScripts.moveToFinished(
                    connectionManager.getBinaryConnection(),
                    queueKeys,
                    job.getId(),
                    stackTrace,
                    "failedReason",
                    "failed",
                    workerId,
                    false,
                    opts != null ? opts.toMap() : Map.of()
                );
            } catch (Exception e) {
                log.warn("Failed to mark job {} failed in batch: {}", job.getId(), e.getMessage());
            }
        }
    }

    private Job<T> buildJobFromFields(String jobId, Map<String, String> fields) {
        String jobName = fields.getOrDefault("name", "unnamed");
        String dataStr = fields.get("data");
        String optsStr = fields.get("opts");
        String attemptsStr = fields.get("attemptsMade");
        String timestampStr = fields.get("timestamp");

        T data = payloadClass != null ? serializer.deserialize(dataStr, payloadClass) : (T) dataStr;
        JobOptions opts = serializer.deserialize(optsStr, JobOptions.class);

        Job<T> job = new Job<>(jobId, jobName, data, opts);
        job.setQueueName(queueName);
        if (attemptsStr != null) job.setAttemptsMade(Integer.parseInt(attemptsStr));
        if (timestampStr != null) {
            long ts = Long.parseLong(timestampStr);
            job.setTimestamp(ts);
            long waitTimeMs = System.currentTimeMillis() - ts;
            metrics.recordJobWaitTime(queueName, Duration.ofMillis(Math.max(0, waitTimeMs)));
        }

        job.setProgressUpdater((percentage, payload) -> {
            try {
                bullScripts.updateProgress(connectionManager.getBinaryConnection(), queueKeys, jobId, String.valueOf(percentage));
            } catch (Exception e) {
                log.warn("Failed to update progress for job {}", jobId, e);
            }
        });

        job.setLogAppender(msg -> {
            try {
                bullScripts.addLog(connectionManager.getBinaryConnection(), queueKeys, jobId, msg, 0);
            } catch (Exception e) {
                log.warn("Failed to append log for job {}", jobId, e);
            }
        });

        return job;
    }

    private Map<String, String> parseHashFields(List<?> flatList) {
        Map<String, String> map = new HashMap<>();
        if (flatList != null) {
            for (int i = 0; i < flatList.size() - 1; i += 2) {
                String key = toUtf8String(flatList.get(i));
                String val = toUtf8String(flatList.get(i + 1));
                if (key != null) {
                    map.put(key, val);
                }
            }
        }
        return map;
    }

    private String toUtf8String(Object obj) {
        if (obj instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return obj != null ? obj.toString() : null;
    }

    @Override
    public void pause() {
        paused.set(true);
        log.info("Paused batch worker [{}] for queue {}", workerId, queueName);
    }

    @Override
    public void resume() {
        paused.set(false);
        log.info("Resumed batch worker [{}] for queue {}", workerId, queueName);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isPaused() {
        return paused.get();
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            log.info("Closing OxMQ Batch Worker [{}] for queue {}", workerId, queueName);

            if (pollerThread != null) {
                pollerThread.interrupt();
            }

            if (dispatcherExecutor != null) {
                dispatcherExecutor.shutdown();
                try {
                    if (!dispatcherExecutor.awaitTermination(drainTimeoutMs, TimeUnit.MILLISECONDS)) {
                        log.warn("Batch worker [{}] tasks did not finish within drain timeout, forcing shutdown", workerId);
                        dispatcherExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    dispatcherExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }

            if (lockExtender != null) {
                lockExtender.close();
            }

            connectionManager.close();
            log.info("OxMQ Batch Worker [{}] stopped successfully", workerId);
        }
    }

    public static class Builder<T> {
        private String queueName;
        private BatchJobProcessor<T, ?> processor;
        private Class<T> payloadClass;
        private int batchSize = 10;
        private boolean useVirtualThreads = true;
        private long lockDurationMs = 30_000;
        private long drainTimeoutMs = 10_000;
        private long pollIntervalMs = 50;
        private RedisConnectionManager connectionManager;
        private LuaScriptManager scriptManager;
        private JobSerializer serializer;
        private OxmqMetrics metrics;

        public Builder<T> queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Builder<T> processor(BatchJobProcessor<T, ?> processor) {
            this.processor = processor;
            return this;
        }

        public Builder<T> payloadClass(Class<T> payloadClass) {
            this.payloadClass = payloadClass;
            return this;
        }

        public Builder<T> batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder<T> useVirtualThreads(boolean useVirtualThreads) {
            this.useVirtualThreads = useVirtualThreads;
            return this;
        }

        public Builder<T> lockDuration(Duration lockDuration) {
            this.lockDurationMs = lockDuration.toMillis();
            return this;
        }

        public Builder<T> lockDurationMs(long lockDurationMs) {
            this.lockDurationMs = lockDurationMs;
            return this;
        }

        public Builder<T> drainTimeout(Duration drainTimeout) {
            this.drainTimeoutMs = drainTimeout.toMillis();
            return this;
        }

        public Builder<T> drainTimeoutMs(long drainTimeoutMs) {
            this.drainTimeoutMs = drainTimeoutMs;
            return this;
        }

        public Builder<T> pollInterval(Duration pollInterval) {
            this.pollIntervalMs = pollInterval.toMillis();
            return this;
        }

        public Builder<T> pollIntervalMs(long pollIntervalMs) {
            this.pollIntervalMs = pollIntervalMs;
            return this;
        }

        public Builder<T> redisUri(String redisUri) {
            this.connectionManager = new RedisConnectionManager(redisUri);
            return this;
        }

        public Builder<T> redisClient(RedisClient redisClient) {
            this.connectionManager = new RedisConnectionManager(redisClient);
            return this;
        }

        public Builder<T> connectionManager(RedisConnectionManager connectionManager) {
            this.connectionManager = connectionManager;
            return this;
        }

        public Builder<T> scriptManager(LuaScriptManager scriptManager) {
            this.scriptManager = scriptManager;
            return this;
        }

        public Builder<T> serializer(JobSerializer serializer) {
            this.serializer = serializer;
            return this;
        }

        public Builder<T> metrics(OxmqMetrics metrics) {
            this.metrics = metrics;
            return this;
        }

        public OxmqBatchWorker<T> build() {
            return new OxmqBatchWorker<>(this);
        }
    }
}
