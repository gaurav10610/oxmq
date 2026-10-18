package io.oxmq;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.oxmq.client.QueueKeys;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.lua.BullScripts;
import io.oxmq.concurrent.DispatcherExecutorFactory;
import io.oxmq.lua.LuaScriptManager;
import io.oxmq.exception.UnrecoverableError;
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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
    private final QueueKeys queueKeys;
    private final String workerId;
    private final JobProcessor<T, ?> processor;
    private final Class<T> payloadClass;
    private final int concurrency;
    private final boolean useVirtualThreads;
    private final long lockDurationMs;
    private final long drainTimeoutMs;
    private final long pollIntervalMs;
    private final int maxStalledCount;
    private final int maxStartedAttempts;

    private final RedisConnectionManager connectionManager;
    private final boolean ownsConnectionManager;
    private final LuaScriptManager scriptManager;
    private final BullScripts bullScripts;
    private final JobSerializer serializer;
    private final OxmqMetrics metrics;
    private final RateLimiter rateLimiter;
    private final List<io.oxmq.interceptor.JobInterceptor> interceptors;

    private final Semaphore concurrencySemaphore;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean paused = new AtomicBoolean(false);

    private final ExecutorService customExecutor;
    private ExecutorService dispatcherExecutor;
    private Thread pollerThread;
    private LockExtender lockExtender;
    private StalledJobSentinel stalledJobSentinel;

    private record ActiveExecution<T>(Job<T> job, Thread thread) {}
    private final ConcurrentMap<String, ActiveExecution<T>> activeExecutions = new ConcurrentHashMap<>();

    private OxmqWorker(Builder<T> builder) {
        this.queueName = Objects.requireNonNull(builder.queueName, "queueName must not be null");
        this.queueKeys = new QueueKeys(builder.prefix, queueName);
        this.prefix = this.queueKeys.getQualifiedName();
        this.workerId = "worker:" + UUID.randomUUID();
        this.processor = Objects.requireNonNull(builder.processor, "processor must not be null");
        this.payloadClass = builder.payloadClass;
        this.concurrency = Math.max(1, builder.concurrency);
        this.useVirtualThreads = builder.useVirtualThreads;
        this.customExecutor = builder.customExecutor;
        this.lockDurationMs = builder.lockDurationMs;
        this.drainTimeoutMs = builder.drainTimeoutMs;
        this.pollIntervalMs = builder.pollIntervalMs;
        this.maxStalledCount = builder.maxStalledCount;
        this.maxStartedAttempts = builder.maxStartedAttempts;
        this.ownsConnectionManager = builder.ownsConnectionManager;

        this.connectionManager = Objects.requireNonNull(builder.connectionManager, "connectionManager must not be null");
        this.scriptManager = builder.scriptManager != null ? builder.scriptManager : new LuaScriptManager();
        this.bullScripts = new BullScripts(this.scriptManager);
        this.serializer = builder.serializer != null ? builder.serializer : new JacksonJobSerializer();
        this.metrics = builder.metrics != null ? builder.metrics : new OxmqMetrics();
        this.interceptors = builder.interceptors != null && !builder.interceptors.isEmpty()
                ? List.copyOf(builder.interceptors)
                : List.of();

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

            if (this.dispatcherExecutor == null) {
                if (this.customExecutor != null) {
                    this.dispatcherExecutor = this.customExecutor;
                } else {
                    this.dispatcherExecutor = DispatcherExecutorFactory.createExecutor(useVirtualThreads, "oxmq-worker-task-" + queueName);
                }
            }

            this.lockExtender = new LockExtender(connectionManager.getBinaryConnection(), bullScripts,
                    queueKeys, workerId, lockDurationMs);

            this.stalledJobSentinel = new StalledJobSentinel(connectionManager.getBinaryConnection(),
                    bullScripts, queueKeys, metrics, lockDurationMs, maxStalledCount);

            this.pollerThread = new Thread(this::pollLoop, "oxmq-poller-" + queueName);
            this.pollerThread.setDaemon(true);
            this.pollerThread.start();
        }
    }

    private void pollLoop() {
        StatefulRedisConnection<String, byte[]> pollerConn = connectionManager.createDedicatedBinaryConnection();
        try {
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
                List<Object> rawResult = bullScripts.moveToActive(pollerConn, queueKeys, workerId,
                        (int) lockDurationMs, null, workerId, now);

                if (rawResult == null || rawResult.size() < 2 || rawResult.get(1) == null) {
                    concurrencySemaphore.release();
                    TimeUnit.MILLISECONDS.sleep(pollIntervalMs);
                    continue;
                }

                Object idObj = rawResult.get(1);
                String jobId = idObj instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : idObj.toString();

                if (jobId == null || jobId.isEmpty() || "0".equals(jobId)) {
                    long expireTime = rawResult.size() > 2 && rawResult.get(2) instanceof Number n ? n.longValue() : 0L;
                    concurrencySemaphore.release();
                    TimeUnit.MILLISECONDS.sleep(expireTime > 0 ? Math.min(expireTime, pollIntervalMs) : pollIntervalMs);
                    continue;
                }

                @SuppressWarnings("unchecked")
                List<?> hashFlat = rawResult.get(0) instanceof List<?> list ? list : List.of();
                Map<String, String> fields = parseHashFields(hashFlat);

                Job<T> job = buildJobFromFields(jobId, fields);
                lockExtender.registerJob(jobId);

                dispatcherExecutor.submit(() -> executeJob(job));

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (!running.get() || Thread.currentThread().isInterrupted() || e instanceof io.lettuce.core.RedisCommandInterruptedException) {
                    Thread.currentThread().interrupt();
                    break;
                }
                log.warn("Error in OxMQ worker poll loop for queue {}", queueName, e);
                try {
                    TimeUnit.MILLISECONDS.sleep(pollIntervalMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    } finally {
        try {
            pollerConn.close();
        } catch (Exception ignored) {}
    }
    }

    private void executeJob(Job<T> job) {
        long startTime = System.nanoTime();
        activeExecutions.put(job.getId(), new ActiveExecution<>(job, Thread.currentThread()));
        try {
            // 1. Interceptor pre-processing
            if (!interceptors.isEmpty()) {
                for (io.oxmq.interceptor.JobInterceptor interceptor : interceptors) {
                    io.oxmq.interceptor.InterceptorDecision decision = interceptor.beforeProcess(job);
                    if (decision != null && decision.isDelay()) {
                        bullScripts.moveToDelayed(connectionManager.getBinaryConnection(), queueKeys,
                                job.getId(), workerId, decision.delayMs(), decision.skipAttempt());
                        log.debug("Job {} [id: {}] intercepted before processing; moved to delayed ({} ms, skipAttempt={})",
                                job.getName(), job.getId(), decision.delayMs(), decision.skipAttempt());
                        return;
                    } else if (decision != null && decision.isReject()) {
                        failJob(job, new io.oxmq.exception.JobRejectedException(decision.reason()), Duration.ZERO);
                        return;
                    }
                }
            }

            log.debug("Executing job {} [id: {}] on queue {}", job.getName(), job.getId(), queueName);
            Object result = processor.process(job);

            long durationNanos = System.nanoTime() - startTime;
            Duration duration = Duration.ofNanos(durationNanos);

            completeJob(job, result, duration);
            metrics.recordJobCompleted(queueName, duration);
            log.debug("Completed job {} [id: {}] in {} ms", job.getName(), job.getId(), duration.toMillis());

            // 2. Interceptor post-processing
            if (!interceptors.isEmpty()) {
                for (io.oxmq.interceptor.JobInterceptor interceptor : interceptors) {
                    try {
                        interceptor.afterProcess(job, result, duration);
                    } catch (Throwable it) {
                        log.warn("Error in interceptor afterProcess for job {}: {}", job.getId(), it.getMessage());
                    }
                }
            }

        } catch (InterruptedException | io.oxmq.exception.JobCancelledException ie) {
            long durationNanos = System.nanoTime() - startTime;
            Duration duration = Duration.ofNanos(durationNanos);
            String reason = job.getCancelReason() != null ? job.getCancelReason() : (ie.getMessage() != null ? ie.getMessage() : "Job cancelled");
            job.setCancelled(true, reason);
            failJob(job, ie, duration);
            metrics.recordJobFailed(queueName, duration, "JobCancelledException");
            log.info("Job {} [id: {}] was cancelled on queue {}: {}", job.getName(), job.getId(), queueName, reason);
        } catch (io.oxmq.exception.RateLimitException rle) {
            bullScripts.moveToDelayed(connectionManager.getBinaryConnection(), queueKeys,
                    job.getId(), workerId, rle.getDelayMs(), true);
            log.info("Job {} [id: {}] rate-limited; moved to delayed (delay: {} ms, skipAttempt: true)",
                    job.getName(), job.getId(), rle.getDelayMs());
        } catch (io.oxmq.exception.DelayedException de) {
            bullScripts.moveToDelayed(connectionManager.getBinaryConnection(), queueKeys,
                    job.getId(), workerId, de.getDelayMs(), true);
            log.debug("Job {} [id: {}] moved to delayed state via DelayedException (delay: {} ms, skipAttempt: true)",
                    job.getName(), job.getId(), de.getDelayMs());
        } catch (io.oxmq.exception.WaitingException we) {
            log.debug("Job {} [id: {}] moved to wait state via WaitingException", job.getName(), job.getId());
        } catch (io.oxmq.exception.WaitingChildrenException wce) {
            log.debug("Job {} [id: {}] moved to waiting-children state via WaitingChildrenException", job.getName(), job.getId());
        } catch (Throwable t) {
            long durationNanos = System.nanoTime() - startTime;
            Duration duration = Duration.ofNanos(durationNanos);

            boolean handledByInterceptor = false;
            if (!interceptors.isEmpty()) {
                for (io.oxmq.interceptor.JobInterceptor interceptor : interceptors) {
                    try {
                        io.oxmq.interceptor.ErrorDecision decision = interceptor.onError(job, t, duration);
                        if (decision != null && (decision.isCooldown() || decision.isDelay())) {
                            bullScripts.moveToDelayed(connectionManager.getBinaryConnection(), queueKeys,
                                    job.getId(), workerId, decision.delayMs(), decision.skipAttempt());
                            log.info("Job {} [id: {}] moved to delayed via interceptor onError (delay: {} ms, skipAttempt: {})",
                                    job.getName(), job.getId(), decision.delayMs(), decision.skipAttempt());
                            handledByInterceptor = true;
                            break;
                        }
                    } catch (Throwable it) {
                        log.warn("Error in interceptor onError for job {}: {}", job.getId(), it.getMessage());
                    }
                }
            }

            if (!handledByInterceptor) {
                failJob(job, t, duration);
                metrics.recordJobFailed(queueName, duration, t.getClass().getSimpleName());
                log.warn("Job {} [id: {}] failed on queue {}: {}", job.getName(), job.getId(), queueName, t.toString(), t);
            }
        } finally {
            activeExecutions.remove(job.getId());
            lockExtender.unregisterJob(job.getId());
            concurrencySemaphore.release();
        }
    }

    private void completeJob(Job<T> job, Object result, Duration duration) {
        String serializedResult = serializer.serialize(result);
        JobOptions opts = job.getOpts();
        bullScripts.moveToFinished(connectionManager.getBinaryConnection(), queueKeys, job.getId(),
                serializedResult, "returnvalue", "completed", workerId, false, opts != null ? opts.toMap() : Map.of());
    }

    private static final int MAX_FAILED_REASON_LENGTH = 8192;

    private static final Map<String, String> CANONICAL_KEYS = Map.ofEntries(
        Map.entry("name", "name"),
        Map.entry("data", "data"),
        Map.entry("opts", "opts"),
        Map.entry("timestamp", "timestamp"),
        Map.entry("delay", "delay"),
        Map.entry("priority", "priority"),
        Map.entry("attemptsMade", "attemptsMade"),
        Map.entry("attemptsStarted", "attemptsStarted"),
        Map.entry("processedOn", "processedOn"),
        Map.entry("finishedOn", "finishedOn"),
        Map.entry("returnvalue", "returnvalue"),
        Map.entry("failedReason", "failedReason"),
        Map.entry("parentKey", "parentKey"),
        Map.entry("parent", "parent"),
        Map.entry("progress", "progress"),
        Map.entry("rjk", "rjk"),
        Map.entry("deid", "deid"),
        Map.entry("pb", "pb"),
        Map.entry("ats", "ats")
    );

    private void failJob(Job<T> job, Throwable error, Duration duration) {
        StringWriter sw = new StringWriter();
        error.printStackTrace(new PrintWriter(sw));
        String stackTrace = sw.toString();
        if (stackTrace.length() > MAX_FAILED_REASON_LENGTH) {
            stackTrace = stackTrace.substring(0, MAX_FAILED_REASON_LENGTH) + "\n[... stack trace truncated for memory optimization ...]";
        }

        boolean isUnrecoverable = (error instanceof UnrecoverableError) ||
                                  (error.getCause() instanceof UnrecoverableError) ||
                                  (error instanceof io.oxmq.exception.JobCancelledException) ||
                                  (error instanceof InterruptedException) ||
                                  job.isCancelled();

        if (maxStartedAttempts > 0 && job.getAttemptsStarted() >= maxStartedAttempts) {
            isUnrecoverable = true;
        }

        JobOptions opts = job.getOpts();
        int maxAttempts = opts != null && opts.getAttempts() > 0 ? opts.getAttempts() : 1;
        int currentAttempts = job.getAttemptsMade() + 1;

        if (!isUnrecoverable && currentAttempts < maxAttempts) {
            BackoffStrategy backoff = opts != null ? opts.getBackoff() : null;
            long delayMs = backoff != null ? backoff.calculateDelayMs(currentAttempts) : 0;
            if (delayMs > 0) {
                bullScripts.moveToDelayed(connectionManager.getBinaryConnection(), queueKeys,
                        job.getId(), workerId, delayMs);
                log.debug("Job {} [id: {}] failed (attempt {}/{}), delayed by {} ms for retry",
                        job.getName(), job.getId(), currentAttempts, maxAttempts, delayMs);
                return;
            } else {
                bullScripts.retryJob(connectionManager.getBinaryConnection(), queueKeys,
                        job.getId(), workerId, opts != null && opts.isLifo());
                log.debug("Job {} [id: {}] failed (attempt {}/{}), moved to wait for retry",
                        job.getName(), job.getId(), currentAttempts, maxAttempts);
                return;
            }
        }

        String failedReasonValue;
        if (job.isCancelled()) {
            failedReasonValue = job.getCancelReason() != null ? "Job cancelled: " + job.getCancelReason() : "Job cancelled";
        } else {
            failedReasonValue = stackTrace;
        }

        bullScripts.moveToFinished(connectionManager.getBinaryConnection(), queueKeys, job.getId(),
                failedReasonValue, "failedReason", "failed", workerId, false, opts != null ? opts.toMap() : Map.of());
    }

    private Job<T> buildJobFromFields(String jobId, Map<String, String> fields) {
        String jobName = fields.getOrDefault("name", "unnamed");
        String dataStr = fields.get("data");
        String optsStr = fields.get("opts");
        String attemptsStr = fields.get("attemptsMade");
        String attemptsStartedStr = fields.get("attemptsStarted");
        String timestampStr = fields.get("timestamp");
        String parentKeyStr = fields.get("parentKey");

        T data = payloadClass != null ? serializer.deserialize(dataStr, payloadClass) : (T) dataStr;
        JobOptions opts = serializer.deserialize(optsStr, JobOptions.class);

        Job<T> job = new Job<>(jobId, jobName, data, opts);
        job.setQueueName(queueName);
        if (attemptsStr != null) job.setAttemptsMade(Integer.parseInt(attemptsStr));
        if (attemptsStartedStr != null) job.setAttemptsStarted(Integer.parseInt(attemptsStartedStr));
        if (timestampStr != null) {
            long ts = Long.parseLong(timestampStr);
            job.setTimestamp(ts);
            long waitTimeMs = System.currentTimeMillis() - ts;
            metrics.recordJobWaitTime(queueName, Duration.ofMillis(Math.max(0, waitTimeMs)));
        }
        if (parentKeyStr != null) job.setParentKey(parentKeyStr);

        // Memory optimization: lazily load children values only when requested by processor
        job.setChildrenValuesSupplier(() -> {
            try {
                RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
                Map<String, String> childValsRaw = sync.hgetall(prefix + ":" + jobId + ":processed");
                if (childValsRaw == null || childValsRaw.isEmpty()) {
                    childValsRaw = sync.hgetall(prefix + ":" + jobId + ":childrenValues");
                }
                if (childValsRaw != null && !childValsRaw.isEmpty()) {
                    Map<String, Object> childVals = new HashMap<>(childValsRaw.size());
                    for (Map.Entry<String, String> entry : childValsRaw.entrySet()) {
                        try {
                            childVals.put(entry.getKey(), serializer.deserialize(entry.getValue(), Object.class));
                        } catch (Exception ex) {
                            childVals.put(entry.getKey(), entry.getValue());
                        }
                    }
                    return childVals;
                }
            } catch (Exception e) {
                log.debug("No childrenValues or failed to fetch childrenValues for job {}", jobId, e);
            }
            return null;
        });

        job.setProgressUpdater((percentage, payload) -> {
            try {
                bullScripts.updateProgress(connectionManager.getBinaryConnection(), queueKeys, jobId, String.valueOf(percentage));
            } catch (Exception e) {
                log.warn("Failed to sync progress for job {}", jobId, e);
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
        if (flatList == null || flatList.isEmpty()) {
            return Map.of();
        }
        int expectedEntries = flatList.size() / 2;
        Map<String, String> map = new HashMap<>(Math.max(8, (int) (expectedEntries / 0.75f) + 1));
        for (int i = 0; i < flatList.size() - 1; i += 2) {
            String key = toUtf8String(flatList.get(i));
            if (key != null) {
                String canonicalKey = CANONICAL_KEYS.getOrDefault(key, key);
                String val = toUtf8String(flatList.get(i + 1));
                map.put(canonicalKey, val);
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
    public boolean cancelJob(String jobId) {
        return cancelJob(jobId, "User requested cancellation");
    }

    @Override
    public boolean cancelJob(String jobId, String reason) {
        if (jobId == null) {
            return false;
        }
        ActiveExecution<T> execution = activeExecutions.get(jobId);
        if (execution != null) {
            execution.job().setCancelled(true, reason);
            execution.job().getCancellationToken().cancel(reason);
            execution.thread().interrupt();
            log.info("Cancelled running job [id: {}] on queue {}: {}", jobId, queueName, reason);
            return true;
        }
        return false;
    }

    @Override
    public int cancelAllJobs() {
        return cancelAllJobs("Worker cancellation requested");
    }

    @Override
    public int cancelAllJobs(String reason) {
        int count = 0;
        for (String jobId : new java.util.ArrayList<>(activeExecutions.keySet())) {
            if (cancelJob(jobId, reason)) {
                count++;
            }
        }
        return count;
    }

    @Override
    public boolean isJobActive(String jobId) {
        return jobId != null && activeExecutions.containsKey(jobId);
    }

    @Override
    public int getActiveJobCount() {
        return activeExecutions.size();
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            log.info("Shutting down OxMQ Worker [{}] for queue {} (drain timeout: {} ms)", workerId, queueName, drainTimeoutMs);

            if (pollerThread != null) {
                pollerThread.interrupt();
            }

            if (stalledJobSentinel != null) {
                stalledJobSentinel.close();
            }

            if (dispatcherExecutor != null && customExecutor == null) {
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

            if (lockExtender != null) {
                lockExtender.close();
            }

            if (ownsConnectionManager && connectionManager != null) {
                connectionManager.close();
            }
            log.info("OxMQ Worker [{}] stopped.", workerId);
        }
    }

    public static class Builder<T> {
        private String queueName;
        private String prefix;
        private RedisConnectionManager connectionManager;
        private RedisClient redisClient;
        private String redisUri;
        private JobProcessor<T, ?> processor;
        private Class<T> payloadClass;
        private int concurrency = 20;
        private boolean useVirtualThreads = true;
        private ExecutorService customExecutor;
        private long lockDurationMs = 30_000;
        private long drainTimeoutMs = 10_000;
        private long pollIntervalMs = 50;
        private int maxStalledCount = 2;
        private int maxStartedAttempts = 0;
        private int rateLimitMax = 0;
        private Duration rateLimitDuration = null;
        private LuaScriptManager scriptManager;
        private JobSerializer serializer;
        private OxmqMetrics metrics;

        public Builder<T> queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Builder<T> prefix(String prefix) {
            this.prefix = prefix;
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

        public Builder<T> executor(ExecutorService customExecutor) {
            this.customExecutor = customExecutor;
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

        public Builder<T> maxStartedAttempts(int maxStartedAttempts) {
            this.maxStartedAttempts = maxStartedAttempts;
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

        private final List<io.oxmq.interceptor.JobInterceptor> interceptors = new java.util.ArrayList<>();

        public Builder<T> interceptor(io.oxmq.interceptor.JobInterceptor interceptor) {
            if (interceptor != null) {
                this.interceptors.add(interceptor);
            }
            return this;
        }

        public Builder<T> interceptors(List<io.oxmq.interceptor.JobInterceptor> interceptors) {
            if (interceptors != null) {
                this.interceptors.addAll(interceptors);
            }
            return this;
        }

        private boolean ownsConnectionManager = false;

        public OxmqWorker<T> build() {
            if (connectionManager == null) {
                ownsConnectionManager = true;
                if (redisClient != null) {
                    connectionManager = new RedisConnectionManager(redisClient);
                } else if (redisUri != null) {
                    connectionManager = new RedisConnectionManager(redisUri);
                } else {
                    connectionManager = new RedisConnectionManager("redis://localhost:6379");
                }
            } else {
                ownsConnectionManager = false;
            }
            return new OxmqWorker<>(this);
        }
    }
}
