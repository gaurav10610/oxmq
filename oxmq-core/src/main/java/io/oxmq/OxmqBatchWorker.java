package io.oxmq;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.lua.LuaScript;
import io.oxmq.lua.LuaScriptManager;
import io.oxmq.metrics.OxmqMetrics;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.serializer.JacksonJobSerializer;
import io.oxmq.serializer.JobSerializer;
import io.oxmq.watchdog.LockExtender;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-throughput batch worker engine for consuming chunks of jobs atomically.
 * Optimized for bulk database ingestion (ClickHouse, Elasticsearch, PostgreSQL batch inserts, S3).
 *
 * @param <T> Payload data type
 */
public class OxmqBatchWorker<T> implements Worker<T> {

    private static final Logger log = LoggerFactory.getLogger(OxmqBatchWorker.class);

    private final String queueName;
    private final String prefix;
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

            this.lockExtender = new LockExtender(connectionManager.createDedicatedConnection(), scriptManager,
                    prefix, workerId, lockDurationMs);

            this.pollerThread = new Thread(this::pollLoop, "oxmq-batch-poller-" + queueName);
            this.pollerThread.setDaemon(true);
            this.pollerThread.start();
        }
    }

    private void pollLoop() {
        StatefulRedisConnection<String, String> pollerConn = connectionManager.createDedicatedConnection();
        String[] keys = new String[]{
                prefix + ":wait",
                prefix + ":active",
                prefix + ":delayed",
                prefix + ":stalled",
                prefix + ":meta"
        };

        while (running.get()) {
            try {
                if (paused.get()) {
                    TimeUnit.MILLISECONDS.sleep(pollIntervalMs);
                    continue;
                }

                long now = System.currentTimeMillis();
                List<List<Object>> rawBatches = scriptManager.eval(pollerConn, LuaScript.MOVE_TO_ACTIVE_BATCH,
                        ScriptOutputType.MULTI, keys,
                        prefix,
                        workerId,
                        String.valueOf(lockDurationMs),
                        String.valueOf(now),
                        String.valueOf(batchSize)
                );

                if (rawBatches == null || rawBatches.isEmpty()) {
                    TimeUnit.MILLISECONDS.sleep(pollIntervalMs);
                    continue;
                }

                List<Job<T>> jobs = new ArrayList<>();
                for (List<Object> rawJob : rawBatches) {
                    if (rawJob != null && rawJob.size() >= 2 && rawJob.get(0) != null) {
                        String jobId = rawJob.get(0).toString();
                        @SuppressWarnings("unchecked")
                        List<String> hashFlat = (List<String>) rawJob.get(1);
                        Map<String, String> fields = parseHashFields(hashFlat);
                        Job<T> job = buildJobFromFields(jobId, fields);
                        jobs.add(job);
                        lockExtender.registerJob(jobId);
                    }
                }

                if (!jobs.isEmpty()) {
                    dispatcherExecutor.submit(() -> executeBatch(jobs, pollerConn));
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
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

    private void executeBatch(List<Job<T>> jobs, StatefulRedisConnection<String, String> conn) {
        long startTime = System.nanoTime();
        try {
            log.debug("Executing batch of {} jobs on queue {}", jobs.size(), queueName);
            Object result = processor.process(jobs);

            long durationNanos = System.nanoTime() - startTime;
            Duration duration = Duration.ofNanos(durationNanos);

            completeBatch(jobs, result, duration, conn);
            for (Job<T> job : jobs) {
                metrics.recordJobCompleted(queueName, duration);
            }
            log.debug("Completed batch of {} jobs in {} ms", jobs.size(), duration.toMillis());

        } catch (Throwable t) {
            long durationNanos = System.nanoTime() - startTime;
            Duration duration = Duration.ofNanos(durationNanos);

            failBatch(jobs, t, duration, conn);
            for (Job<T> job : jobs) {
                metrics.recordJobFailed(queueName, duration, t.getClass().getSimpleName());
            }
            log.warn("Batch of {} jobs failed on queue {}: {}", jobs.size(), queueName, t.getMessage());
        } finally {
            for (Job<T> job : jobs) {
                lockExtender.unregisterJob(job.getId());
            }
        }
    }

    private void completeBatch(List<Job<T>> jobs, Object result, Duration duration, StatefulRedisConnection<String, String> conn) {
        String serializedResult = serializer.serialize(result);
        String[] keys = new String[]{
                prefix + ":active",
                prefix + ":completed",
                prefix + ":failed",
                prefix + ":events"
        };

        List<String> args = new ArrayList<>();
        args.add(prefix);
        args.add("completed");
        args.add(workerId);
        args.add(String.valueOf(System.currentTimeMillis()));
        args.add(serializedResult != null ? serializedResult : "");
        for (Job<T> job : jobs) {
            args.add(job.getId());
        }

        scriptManager.eval(conn, LuaScript.MOVE_TO_FINISHED_BATCH, ScriptOutputType.INTEGER, keys, args.toArray(new String[0]));
    }

    private void failBatch(List<Job<T>> jobs, Throwable error, Duration duration, StatefulRedisConnection<String, String> conn) {
        StringWriter sw = new StringWriter();
        error.printStackTrace(new PrintWriter(sw));
        String stackTrace = sw.toString();

        String[] keys = new String[]{
                prefix + ":active",
                prefix + ":completed",
                prefix + ":failed",
                prefix + ":events"
        };

        List<String> args = new ArrayList<>();
        args.add(prefix);
        args.add("failed");
        args.add(workerId);
        args.add(String.valueOf(System.currentTimeMillis()));
        args.add(stackTrace);
        for (Job<T> job : jobs) {
            args.add(job.getId());
        }

        scriptManager.eval(conn, LuaScript.MOVE_TO_FINISHED_BATCH, ScriptOutputType.INTEGER, keys, args.toArray(new String[0]));
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
        return job;
    }

    private Map<String, String> parseHashFields(List<String> flatList) {
        Map<String, String> map = new HashMap<>();
        if (flatList != null) {
            for (int i = 0; i < flatList.size() - 1; i += 2) {
                map.put(flatList.get(i), flatList.get(i + 1));
            }
        }
        return map;
    }

    @Override
    public void pause() {
        paused.set(true);
    }

    @Override
    public void resume() {
        paused.set(false);
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
            log.info("Stopping OxMQ Batch Worker [{}] for queue {}", workerId, queueName);
            if (pollerThread != null) pollerThread.interrupt();
            if (lockExtender != null) lockExtender.close();
            if (dispatcherExecutor != null) {
                dispatcherExecutor.shutdown();
                try {
                    if (!dispatcherExecutor.awaitTermination(drainTimeoutMs, TimeUnit.MILLISECONDS)) {
                        dispatcherExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    dispatcherExecutor.shutdownNow();
                }
            }
            connectionManager.close();
            log.info("OxMQ Batch Worker [{}] stopped.", workerId);
        }
    }

    public static class Builder<T> {
        private String queueName;
        private RedisConnectionManager connectionManager;
        private RedisClient redisClient;
        private String redisUri;
        private BatchJobProcessor<T, ?> processor;
        private Class<T> payloadClass;
        private int batchSize = 100;
        private boolean useVirtualThreads = true;
        private long lockDurationMs = 30_000;
        private long drainTimeoutMs = 10_000;
        private long pollIntervalMs = 50;
        private LuaScriptManager scriptManager;
        private JobSerializer serializer;
        private OxmqMetrics metrics;

        public Builder<T> queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Builder<T> redisUri(String redisUri) {
            this.redisUri = redisUri;
            return this;
        }

        public Builder<T> redisClient(RedisClient redisClient) {
            this.redisClient = redisClient;
            return this;
        }

        public Builder<T> connectionManager(RedisConnectionManager connectionManager) {
            this.connectionManager = connectionManager;
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

        public Builder<T> drainTimeout(Duration drainTimeout) {
            this.drainTimeoutMs = drainTimeout.toMillis();
            return this;
        }

        public Builder<T> pollIntervalMs(long pollIntervalMs) {
            this.pollIntervalMs = pollIntervalMs;
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
            if (connectionManager == null) {
                if (redisClient != null) {
                    connectionManager = new RedisConnectionManager(redisClient);
                } else if (redisUri != null) {
                    connectionManager = new RedisConnectionManager(redisUri);
                } else {
                    connectionManager = new RedisConnectionManager("redis://localhost:6379");
                }
            }
            return new OxmqBatchWorker<>(this);
        }
    }
}
