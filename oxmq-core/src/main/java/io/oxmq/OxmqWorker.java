package io.oxmq;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.lua.LuaScript;
import io.oxmq.lua.LuaScriptManager;
import io.oxmq.metrics.OxmqMetrics;
import io.oxmq.model.BackoffStrategy;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.rate.RateLimiter;
import io.oxmq.serializer.JacksonJobSerializer;
import io.oxmq.serializer.JobSerializer;
import io.oxmq.watchdog.LockExtender;
import io.oxmq.watchdog.StalledJobSentinel;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * High-throughput, Virtual Thread-native background worker engine.
 *
 * @param <T> Payload data type
 */
public class OxmqWorker<T> implements Worker<T> {

    private static final Logger log = LoggerFactory.getLogger(OxmqWorker.class);

    private final String queueName;
    private final String prefix;
    private final String workerId;
    private final JobProcessor<T, ?> processor;
    private final Class<T> payloadClass;
    private final int concurrency;
    private final boolean useVirtualThreads;
    private final long lockDurationMs;
    private final long drainTimeoutMs;
    private final long pollIntervalMs;
    private final int maxStalledCount;

    private final RedisConnectionManager connectionManager;
    private final LuaScriptManager scriptManager;
    private final JobSerializer serializer;
    private final OxmqMetrics metrics;
    private final RateLimiter rateLimiter;

    private final Semaphore concurrencySemaphore;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean paused = new AtomicBoolean(false);

    private ExecutorService dispatcherExecutor;
    private Thread pollerThread;
    private LockExtender lockExtender;
    private StalledJobSentinel stalledJobSentinel;

    private OxmqWorker(Builder<T> builder) {
        this.queueName = Objects.requireNonNull(builder.queueName, "queueName must not be null");
        this.prefix = "bull:" + queueName;
        this.workerId = "worker:" + UUID.randomUUID();
        this.processor = Objects.requireNonNull(builder.processor, "processor must not be null");
        this.payloadClass = builder.payloadClass;
        this.concurrency = Math.max(1, builder.concurrency);
        this.useVirtualThreads = builder.useVirtualThreads;
        this.lockDurationMs = builder.lockDurationMs;
        this.drainTimeoutMs = builder.drainTimeoutMs;
        this.pollIntervalMs = builder.pollIntervalMs;
        this.maxStalledCount = builder.maxStalledCount;

        this.connectionManager = Objects.requireNonNull(builder.connectionManager, "connectionManager must not be null");
        this.scriptManager = builder.scriptManager != null ? builder.scriptManager : new LuaScriptManager();
        this.serializer = builder.serializer != null ? builder.serializer : new JacksonJobSerializer();
        this.metrics = builder.metrics != null ? builder.metrics : new OxmqMetrics();

        if (builder.rateLimitMax > 0 && builder.rateLimitDuration != null) {
            this.rateLimiter = new RateLimiter(connectionManager.getCommandConnection(), this.scriptManager,
                    this.prefix, builder.rateLimitMax, builder.rateLimitDuration);
        } else {
            this.rateLimiter = null;
        }

        this.concurrencySemaphore = new Semaphore(concurrency);
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    @Override
    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Starting OxMQ Worker [{}] for queue '{}' with concurrency {} (Virtual Threads: {})",
                    workerId, queueName, concurrency, useVirtualThreads);

            if (useVirtualThreads) {
                this.dispatcherExecutor = Executors.newVirtualThreadPerTaskExecutor();
            } else {
                this.dispatcherExecutor = Executors.newCachedThreadPool(r -> {
                    Thread t = new Thread(r, "oxmq-worker-task-" + queueName);
                    t.setDaemon(true);
                    return t;
                });
            }

            this.lockExtender = new LockExtender(connectionManager.createDedicatedConnection(), scriptManager,
                    prefix, workerId, lockDurationMs);

            this.stalledJobSentinel = new StalledJobSentinel(connectionManager.createDedicatedConnection(),
                    prefix, queueName, metrics, lockDurationMs, maxStalledCount);

            this.pollerThread = new Thread(this::pollLoop, "oxmq-poller-" + queueName);
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
                prefix + ":meta",
                prefix + ":limiter"
        };

        while (running.get()) {
            try {
                if (paused.get()) {
                    TimeUnit.MILLISECONDS.sleep(pollIntervalMs);
                    continue;
                }

                if (rateLimiter != null && !rateLimiter.tryAcquire()) {
                    TimeUnit.MILLISECONDS.sleep(pollIntervalMs);
                    continue;
                }

                boolean acquired = concurrencySemaphore.tryAcquire(pollIntervalMs, TimeUnit.MILLISECONDS);
                if (!acquired) {
                    continue;
                }

                long now = System.currentTimeMillis();
                List<Object> rawResult = scriptManager.eval(pollerConn, LuaScript.MOVE_TO_ACTIVE,
                        ScriptOutputType.MULTI, keys,
                        prefix,
                        workerId,
                        String.valueOf(lockDurationMs),
                        String.valueOf(now)
                );

                if (rawResult == null || rawResult.isEmpty() || rawResult.get(0) == null) {
                    concurrencySemaphore.release();
                    TimeUnit.MILLISECONDS.sleep(pollIntervalMs);
                    continue;
                }

                String jobId = rawResult.get(0).toString();
                @SuppressWarnings("unchecked")
                List<String> hashFlat = (List<String>) rawResult.get(1);
                Map<String, String> fields = parseHashFields(hashFlat);

                Job<T> job = buildJobFromFields(jobId, fields);
                lockExtender.registerJob(jobId);

                dispatcherExecutor.submit(() -> executeJob(job, pollerConn));

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.warn("Error in OxMQ worker poll loop for queue {}", queueName, e);
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

    private void executeJob(Job<T> job, StatefulRedisConnection<String, String> conn) {
        long startTime = System.nanoTime();
        try {
            log.debug("Executing job {} [id: {}] on queue {}", job.getName(), job.getId(), queueName);
            Object result = processor.process(job);

            long durationNanos = System.nanoTime() - startTime;
            Duration duration = Duration.ofNanos(durationNanos);

            completeJob(job, result, duration, conn);
            metrics.recordJobCompleted(queueName, duration);
            log.debug("Completed job {} [id: {}] in {} ms", job.getName(), job.getId(), duration.toMillis());

        } catch (Throwable t) {
            long durationNanos = System.nanoTime() - startTime;
            Duration duration = Duration.ofNanos(durationNanos);

            failJob(job, t, duration, conn);
            metrics.recordJobFailed(queueName, duration, t.getClass().getSimpleName());
            log.warn("Job {} [id: {}] failed on queue {}: {}", job.getName(), job.getId(), queueName, t.getMessage());
        } finally {
            lockExtender.unregisterJob(job.getId());
            concurrencySemaphore.release();
        }
    }

    private void completeJob(Job<T> job, Object result, Duration duration, StatefulRedisConnection<String, String> conn) {
        String serializedResult = serializer.serialize(result);
        JobOptions opts = job.getOpts();
        boolean removeOnComplete = opts != null && opts.isRemoveOnComplete();

        String[] keys = new String[]{
                prefix + ":active",
                prefix + ":completed",
                prefix + ":failed",
                prefix + ":delayed",
                prefix + ":events"
        };

        scriptManager.eval(conn, LuaScript.MOVE_TO_FINISHED, ScriptOutputType.INTEGER, keys,
                prefix,
                job.getId(),
                serializedResult != null ? serializedResult : "",
                "completed",
                workerId,
                String.valueOf(System.currentTimeMillis()),
                "1",
                "0",
                removeOnComplete ? "1" : "0",
                "0"
        );
    }

    private void failJob(Job<T> job, Throwable error, Duration duration, StatefulRedisConnection<String, String> conn) {
        StringWriter sw = new StringWriter();
        error.printStackTrace(new PrintWriter(sw));
        String stackTrace = sw.toString();

        JobOptions opts = job.getOpts();
        int maxAttempts = opts != null ? opts.getAttempts() : 1;
        long retryDelayMs = 0;
        if (opts != null && opts.getBackoff() != null) {
            retryDelayMs = opts.getBackoff().calculateDelayMs(job.getAttemptsMade());
        }
        boolean removeOnFail = opts != null && opts.isRemoveOnFail();

        String[] keys = new String[]{
                prefix + ":active",
                prefix + ":completed",
                prefix + ":failed",
                prefix + ":delayed",
                prefix + ":events"
        };

        Long status = scriptManager.eval(conn, LuaScript.MOVE_TO_FINISHED, ScriptOutputType.INTEGER, keys,
                prefix,
                job.getId(),
                stackTrace,
                "failed",
                workerId,
                String.valueOf(System.currentTimeMillis()),
                String.valueOf(maxAttempts),
                String.valueOf(retryDelayMs),
                "0",
                removeOnFail ? "1" : "0"
        );

        if (status != null && status == 0L) {
            metrics.recordJobRetried(queueName);
        }
    }

    private Job<T> buildJobFromFields(String jobId, Map<String, String> fields) {
        String jobName = fields.getOrDefault("name", "unnamed");
        String dataStr = fields.get("data");
        String optsStr = fields.get("opts");
        String attemptsStr = fields.get("attemptsMade");
        String timestampStr = fields.get("timestamp");
        String parentKeyStr = fields.get("parentKey");

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
        if (parentKeyStr != null) job.setParentKey(parentKeyStr);

        job.setProgressUpdater((percentage, payload) -> {
            try {
                RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
                sync.hset(prefix + ":" + jobId, "progress", String.valueOf(percentage));
                sync.publish(prefix + ":events", "{\"event\":\"progress\",\"jobId\":\"" + jobId + "\",\"data\":" + percentage + "}");
            } catch (Exception e) {
                log.warn("Failed to sync progress for job {}", jobId, e);
            }
        });

        job.setLogAppender(msg -> {
            try {
                RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
                sync.rpush(prefix + ":" + jobId + ":logs", msg);
            } catch (Exception e) {
                log.warn("Failed to append log for job {}", jobId, e);
            }
        });

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
        log.info("Paused worker [{}] for queue {}", workerId, queueName);
    }

    @Override
    public void resume() {
        paused.set(false);
        log.info("Resumed worker [{}] for queue {}", workerId, queueName);
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
            log.info("Shutting down OxMQ Worker [{}] for queue {} (drain timeout: {} ms)", workerId, queueName, drainTimeoutMs);

            if (pollerThread != null) {
                pollerThread.interrupt();
            }

            if (lockExtender != null) {
                lockExtender.close();
            }

            if (stalledJobSentinel != null) {
                stalledJobSentinel.close();
            }

            if (dispatcherExecutor != null) {
                dispatcherExecutor.shutdown();
                try {
                    if (!dispatcherExecutor.awaitTermination(drainTimeoutMs, TimeUnit.MILLISECONDS)) {
                        log.warn("Worker [{}] did not drain tasks within {} ms, forcing shutdown", workerId, drainTimeoutMs);
                        dispatcherExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    dispatcherExecutor.shutdownNow();
                }
            }

            connectionManager.close();
            log.info("OxMQ Worker [{}] stopped.", workerId);
        }
    }

    public static class Builder<T> {
        private String queueName;
        private RedisConnectionManager connectionManager;
        private RedisClient redisClient;
        private String redisUri;
        private JobProcessor<T, ?> processor;
        private Class<T> payloadClass;
        private int concurrency = 20;
        private boolean useVirtualThreads = true;
        private long lockDurationMs = 30_000;
        private long drainTimeoutMs = 10_000;
        private long pollIntervalMs = 50;
        private int maxStalledCount = 2;
        private int rateLimitMax = 0;
        private Duration rateLimitDuration = null;
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

        public Builder<T> processor(JobProcessor<T, ?> processor) {
            this.processor = processor;
            return this;
        }

        public Builder<T> payloadClass(Class<T> payloadClass) {
            this.payloadClass = payloadClass;
            return this;
        }

        public Builder<T> concurrency(int concurrency) {
            this.concurrency = concurrency;
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

        public Builder<T> pollIntervalMs(long pollIntervalMs) {
            this.pollIntervalMs = pollIntervalMs;
            return this;
        }

        public Builder<T> maxStalledCount(int maxStalledCount) {
            this.maxStalledCount = maxStalledCount;
            return this;
        }

        public Builder<T> rateLimit(int max, Duration duration) {
            this.rateLimitMax = max;
            this.rateLimitDuration = duration;
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

        public OxmqWorker<T> build() {
            if (connectionManager == null) {
                if (redisClient != null) {
                    connectionManager = new RedisConnectionManager(redisClient);
                } else if (redisUri != null) {
                    connectionManager = new RedisConnectionManager(redisUri);
                } else {
                    connectionManager = new RedisConnectionManager("redis://localhost:6379");
                }
            }
            return new OxmqWorker<>(this);
        }
    }
}
