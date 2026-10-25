package io.oxmq;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.oxmq.client.QueueKeys;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.concurrent.DispatcherExecutorFactory;
import io.oxmq.lua.BullScripts;
import io.oxmq.lua.ExtensionScripts;
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
import java.util.concurrent.Semaphore;
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
    private final int minBatchSize;
    private final long batchTimeoutMs;
    private final boolean groupAffinity;
    private final int groupConcurrency;
    private final int concurrency;
    private final long maxBatchBytes;
    private final boolean useVirtualThreads;
    private final long lockDurationMs;
    private final long drainTimeoutMs;
    private final long pollIntervalMs;

    private final RedisConnectionManager connectionManager;
    private final boolean ownsConnectionManager;
    private final LuaScriptManager scriptManager;
    private final BullScripts bullScripts;
    private final ExtensionScripts extensionScripts;
    private final JobSerializer serializer;
    private final OxmqMetrics metrics;

    private final Semaphore concurrencySemaphore;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean paused = new AtomicBoolean(false);

    private final ExecutorService customExecutor;
    private ExecutorService dispatcherExecutor;
    private Thread pollerThread;
    private LockExtender lockExtender;

    private record ActiveBatchExecution<T>(List<Job<T>> jobs, Thread thread) {}
    private final java.util.concurrent.ConcurrentMap<String, ActiveBatchExecution<T>> activeJobExecutions = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.Set<ActiveBatchExecution<T>> activeBatches = java.util.concurrent.ConcurrentHashMap.newKeySet();

    private OxmqBatchWorker(Builder<T> builder) {
        this.queueName = Objects.requireNonNull(builder.queueName, "queueName must not be null");
        this.queueKeys = new QueueKeys(builder.prefix, queueName);
        this.prefix = this.queueKeys.getQualifiedName();
        this.workerId = "batch-worker:" + UUID.randomUUID();
        this.processor = Objects.requireNonNull(builder.processor, "processor must not be null");
        this.payloadClass = builder.payloadClass;
        this.batchSize = Math.max(1, builder.batchSize);
        this.minBatchSize = Math.max(1, builder.minBatchSize);
        this.batchTimeoutMs = Math.max(0, builder.batchTimeoutMs);
        this.concurrency = Math.max(1, builder.concurrency);
        this.concurrencySemaphore = new Semaphore(this.concurrency);
        this.maxBatchBytes = builder.maxBatchBytes;
        this.useVirtualThreads = builder.useVirtualThreads;
        this.customExecutor = builder.customExecutor;
        this.lockDurationMs = builder.lockDurationMs;
        this.drainTimeoutMs = builder.drainTimeoutMs;
        this.pollIntervalMs = builder.pollIntervalMs;
        this.ownsConnectionManager = builder.ownsConnectionManager;

        this.connectionManager = Objects.requireNonNull(builder.connectionManager, "connectionManager must not be null");
        this.scriptManager = builder.scriptManager != null ? builder.scriptManager : new LuaScriptManager();
        this.bullScripts = new BullScripts(this.scriptManager);
        this.extensionScripts = new ExtensionScripts(this.scriptManager);
        this.groupAffinity = builder.groupAffinity;
        this.groupConcurrency = Math.max(0, builder.groupConcurrency);
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

            if (this.dispatcherExecutor == null) {
                if (this.customExecutor != null) {
                    this.dispatcherExecutor = this.customExecutor;
                } else {
                    this.dispatcherExecutor = DispatcherExecutorFactory.createExecutor(useVirtualThreads, "oxmq-batch-worker-" + queueName);
                }
            }

            this.lockExtender = new LockExtender(connectionManager.getBinaryConnection(), bullScripts,
                    queueKeys, workerId, lockDurationMs);

            this.pollerThread = new Thread(this::pollLoop, "oxmq-batch-poller-" + queueName);
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

                boolean acquired = concurrencySemaphore.tryAcquire(pollIntervalMs, TimeUnit.MILLISECONDS);
                if (!acquired) {
                    continue;
                }

                List<Job<T>> jobs = new ArrayList<>();
                long currentBatchBytes = 0;
                long pollStartTime = System.currentTimeMillis();

                if (groupAffinity) {
                    long now = System.currentTimeMillis();
                    List<Object> groupBatch = extensionScripts.moveToActiveBatchGroup(pollerConn, queueKeys, workerId,
                            lockDurationMs, batchSize, groupConcurrency, now);
                    if (groupBatch != null && !groupBatch.isEmpty()) {
                        for (Object item : groupBatch) {
                            if (item instanceof List<?> tuple && tuple.size() >= 2) {
                                Object idObj = tuple.get(1);
                                String jobId = idObj instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : idObj.toString();
                                if (jobId != null && !jobId.isEmpty() && !"0".equals(jobId)) {
                                    List<?> hashFlat = tuple.get(0) instanceof List<?> list ? list : List.of();
                                    Map<String, String> fields = parseHashFields(hashFlat);
                                    Job<T> job = buildJobFromFields(jobId, fields);
                                    jobs.add(job);
                                    lockExtender.registerJob(jobId);
                                }
                            }
                        }
                    }
                }

                while (jobs.size() < batchSize) {
                    long now = System.currentTimeMillis();
                    List<Object> rawResult = bullScripts.moveToActive(pollerConn, queueKeys, workerId,
                            (int) lockDurationMs, null, workerId, now);

                    if (rawResult == null || rawResult.size() < 2 || rawResult.get(1) == null
                            || "0".equals(rawResult.get(1).toString()) || "".equals(rawResult.get(1).toString())) {
                        List<Object> groupResult = extensionScripts.moveToActiveGroup(pollerConn, queueKeys, workerId,
                                lockDurationMs, groupConcurrency, now);
                        if (groupResult != null && groupResult.size() >= 2 && groupResult.get(1) != null
                                && !"0".equals(groupResult.get(1).toString()) && !"".equals(groupResult.get(1).toString())) {
                            rawResult = groupResult;
                        }
                    }

                    if (rawResult == null || rawResult.size() < 2 || rawResult.get(1) == null) {
                        if (jobs.size() >= minBatchSize || batchTimeoutMs <= 0 || jobs.isEmpty()) {
                            break;
                        }
                        long elapsed = System.currentTimeMillis() - pollStartTime;
                        if (elapsed >= batchTimeoutMs) {
                            break;
                        }
                        long waitTime = Math.min(pollIntervalMs, batchTimeoutMs - elapsed);
                        if (waitTime > 0) {
                            TimeUnit.MILLISECONDS.sleep(waitTime);
                        }
                        continue;
                    }

                    Object idObj = rawResult.get(1);
                    String jobId = idObj instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : idObj.toString();
                    if (jobId.isEmpty() || "0".equals(jobId)) {
                        if (jobs.size() >= minBatchSize || batchTimeoutMs <= 0 || jobs.isEmpty()) {
                            break;
                        }
                        long elapsed = System.currentTimeMillis() - pollStartTime;
                        if (elapsed >= batchTimeoutMs) {
                            break;
                        }
                        long waitTime = Math.min(pollIntervalMs, batchTimeoutMs - elapsed);
                        if (waitTime > 0) {
                            TimeUnit.MILLISECONDS.sleep(waitTime);
                        }
                        continue;
                    }

                    @SuppressWarnings("unchecked")
                    List<?> hashFlat = rawResult.get(0) instanceof List<?> list ? list : List.of();
                    Map<String, String> fields = parseHashFields(hashFlat);
                    Job<T> job = buildJobFromFields(jobId, fields);
                    jobs.add(job);
                    lockExtender.registerJob(jobId);

                    if (maxBatchBytes > 0) {
                        String dataStr = fields.get("data");
                        if (dataStr != null) {
                            currentBatchBytes += dataStr.getBytes(StandardCharsets.UTF_8).length;
                        }
                        if (currentBatchBytes >= maxBatchBytes && !jobs.isEmpty()) {
                            log.debug("Batch byte budget exceeded ({} >= {} bytes), dispatching {} jobs early",
                                    currentBatchBytes, maxBatchBytes, jobs.size());
                            break;
                        }
                    }
                }

                if (jobs.isEmpty()) {
                    concurrencySemaphore.release();
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
    } finally {
        try {
            pollerConn.close();
        } catch (Exception ignored) {}
    }
    }

    private void executeBatch(List<Job<T>> jobs) {
        long startTime = System.nanoTime();
        ActiveBatchExecution<T> batchExecution = new ActiveBatchExecution<>(jobs, Thread.currentThread());
        activeBatches.add(batchExecution);
        for (Job<T> job : jobs) {
            activeJobExecutions.put(job.getId(), batchExecution);
        }
        try {
            log.debug("Executing batch of {} jobs on queue {}", jobs.size(), queueName);
            Object result = processor.process(jobs);

            long durationNanos = System.nanoTime() - startTime;
            Duration duration = Duration.ofNanos(durationNanos);

            boolean hasPartialFailures = false;
            for (Job<T> job : jobs) {
                if (job.isMarkedFailed()) {
                    hasPartialFailures = true;
                    break;
                }
            }

            if (!hasPartialFailures) {
                completeBatch(jobs, result, duration);
                for (Job<T> job : jobs) {
                    metrics.recordJobCompleted(queueName, duration);
                }
                log.debug("Completed batch of {} jobs in {} ms", jobs.size(), duration.toMillis());
            } else {
                List<Job<T>> successfulJobs = new ArrayList<>();
                for (Job<T> job : jobs) {
                    if (job.isMarkedFailed()) {
                        Throwable err = job.getBatchFailureReason();
                        failSingleJob(job, err, duration);
                        metrics.recordJobFailed(queueName, duration, err != null ? err.getClass().getSimpleName() : "Error");
                        log.info("Batch job {} [id: {}] marked as failed: {}", job.getName(), job.getId(),
                                err != null ? err.getMessage() : "Failed");
                    } else {
                        successfulJobs.add(job);
                    }
                }
                if (!successfulJobs.isEmpty()) {
                    completeBatch(successfulJobs, result, duration);
                    for (Job<T> job : successfulJobs) {
                        metrics.recordJobCompleted(queueName, duration);
                    }
                }
                log.info("Completed batch with partial failures on queue {}: {} succeeded, {} failed",
                        queueName, successfulJobs.size(), jobs.size() - successfulJobs.size());
            }

        } catch (InterruptedException | io.oxmq.exception.JobCancelledException ie) {
            long durationNanos = System.nanoTime() - startTime;
            Duration duration = Duration.ofNanos(durationNanos);
            String reason = ie.getMessage() != null ? ie.getMessage() : "Batch cancelled";
            for (Job<T> job : jobs) {
                if (!job.isCancelled()) {
                    job.setCancelled(true, reason);
                }
            }
            failBatch(jobs, ie, duration);
            for (Job<T> job : jobs) {
                metrics.recordJobFailed(queueName, duration, "JobCancelledException");
            }
            log.info("Batch of {} jobs was cancelled on queue {}: {}", jobs.size(), queueName, reason);
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
                activeJobExecutions.remove(job.getId());
                lockExtender.unregisterJob(job.getId());
            }
            activeBatches.remove(batchExecution);
            concurrencySemaphore.release();
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
                String groupId = job.getGroupKey();
                if (groupId != null && !groupId.isBlank()) {
                    try {
                        extensionScripts.decrementGroupActive(connectionManager.getBinaryConnection(), queueKeys, groupId, groupConcurrency);
                    } catch (Exception e) {
                        log.debug("Failed to decrement group active count for {}: {}", groupId, e.getMessage());
                    }
                }
            } catch (Exception e) {
                log.warn("Failed to mark job {} completed in batch: {}", job.getId(), e.getMessage());
            }
        }
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

    private void failBatch(List<Job<T>> jobs, Throwable error, Duration duration) {
        for (Job<T> job : jobs) {
            failSingleJob(job, error, duration);
        }
    }

    private void failSingleJob(Job<T> job, Throwable error, Duration duration) {
        StringWriter sw = new StringWriter();
        if (error != null) {
            error.printStackTrace(new PrintWriter(sw));
        } else {
            sw.write("Job marked as failed in batch execution");
        }
        String stackTrace = sw.toString();
        if (stackTrace.length() > MAX_FAILED_REASON_LENGTH) {
            stackTrace = stackTrace.substring(0, MAX_FAILED_REASON_LENGTH) + "\n[... stack trace truncated for memory optimization ...]";
        }

        try {
            JobOptions opts = job.getOpts();
            String failedReasonValue;
            if (job.isCancelled()) {
                failedReasonValue = job.getCancelReason() != null ? "Job cancelled: " + job.getCancelReason() : "Job cancelled";
            } else if (error != null && error.getMessage() != null && !error.getMessage().isBlank()) {
                failedReasonValue = error.getMessage();
            } else {
                failedReasonValue = stackTrace;
            }
            bullScripts.moveToFinished(
                connectionManager.getBinaryConnection(),
                queueKeys,
                job.getId(),
                failedReasonValue,
                "failedReason",
                "failed",
                workerId,
                false,
                opts != null ? opts.toMap() : Map.of()
            );
            String groupId = job.getGroupKey();
            if (groupId != null && !groupId.isBlank()) {
                try {
                    extensionScripts.decrementGroupActive(connectionManager.getBinaryConnection(), queueKeys, groupId, groupConcurrency);
                } catch (Exception e) {
                    log.debug("Failed to decrement group active count for {}: {}", groupId, e.getMessage());
                }
            }
        } catch (Exception e) {
            log.warn("Failed to mark job {} failed in batch: {}", job.getId(), e.getMessage());
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
        String groupKeyStr = fields.get("groupKey");
        if (groupKeyStr != null && !groupKeyStr.isBlank()) {
            job.setGroupKey(groupKeyStr);
        } else if (opts != null && opts.getGroup() != null && opts.getGroup().getId() != null) {
            job.setGroupKey(opts.getGroup().getId());
        }
        if (attemptsStr != null) job.setAttemptsMade(Integer.parseInt(attemptsStr));
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
    public boolean cancelJob(String jobId) {
        return cancelJob(jobId, "User requested cancellation");
    }

    @Override
    public boolean cancelJob(String jobId, String reason) {
        if (jobId == null) {
            return false;
        }
        ActiveBatchExecution<T> batchExecution = activeJobExecutions.get(jobId);
        if (batchExecution != null) {
            for (Job<T> job : batchExecution.jobs()) {
                job.setCancelled(true, reason);
                job.getCancellationToken().cancel(reason);
            }
            batchExecution.thread().interrupt();
            log.info("Cancelled running batch containing job [id: {}] on queue {}: {}", jobId, queueName, reason);
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
        for (ActiveBatchExecution<T> batch : new ArrayList<>(activeBatches)) {
            for (Job<T> job : batch.jobs()) {
                job.setCancelled(true, reason);
                job.getCancellationToken().cancel(reason);
                count++;
            }
            batch.thread().interrupt();
        }
        return count;
    }

    @Override
    public boolean isJobActive(String jobId) {
        return jobId != null && activeJobExecutions.containsKey(jobId);
    }

    /**
     * Dynamically throttles a virtual group across the cluster for the specified duration.
     *
     * @param groupId virtual group identifier
     * @param duration duration to throttle
     * @return true if rate limit was applied
     */
    public boolean rateLimitGroup(String groupId, Duration duration) {
        return extensionScripts.setGroupRateLimit(connectionManager.getBinaryConnection(), queueKeys, groupId,
                duration != null ? duration.toMillis() : 0);
    }

    @Override
    public int getActiveJobCount() {
        return activeJobExecutions.size();
    }

    public long getMaxBatchBytes() {
        return maxBatchBytes;
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            log.info("Closing OxMQ Batch Worker [{}] for queue {}", workerId, queueName);

            if (pollerThread != null) {
                pollerThread.interrupt();
            }

            if (dispatcherExecutor != null && customExecutor == null) {
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

            if (ownsConnectionManager && connectionManager != null) {
                connectionManager.close();
            }
            log.info("OxMQ Batch Worker [{}] stopped successfully", workerId);
        }
    }

    public static class Builder<T> {
        private String queueName;
        private String prefix;
        private BatchJobProcessor<T, ?> processor;
        private Class<T> payloadClass;
        private int batchSize = 10;
        private int minBatchSize = 1;
        private long batchTimeoutMs = 0;
        private boolean useVirtualThreads = true;
        private ExecutorService customExecutor;
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

        public Builder<T> prefix(String prefix) {
            this.prefix = prefix;
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

        public Builder<T> minBatchSize(int minBatchSize) {
            this.minBatchSize = Math.max(1, minBatchSize);
            return this;
        }

        public Builder<T> batchTimeout(Duration batchTimeout) {
            this.batchTimeoutMs = batchTimeout != null ? batchTimeout.toMillis() : 0;
            return this;
        }

        public Builder<T> batchTimeoutMs(long batchTimeoutMs) {
            this.batchTimeoutMs = Math.max(0, batchTimeoutMs);
            return this;
        }

        private long maxBatchBytes = 0;

        public Builder<T> maxBatchBytes(long maxBatchBytes) {
            this.maxBatchBytes = maxBatchBytes;
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

        public Builder<T> pollInterval(Duration pollInterval) {
            this.pollIntervalMs = pollInterval.toMillis();
            return this;
        }

        public Builder<T> pollIntervalMs(long pollIntervalMs) {
            this.pollIntervalMs = pollIntervalMs;
            return this;
        }

        private int concurrency = 1;
        private boolean ownsConnectionManager = false;
        private boolean groupAffinity = false;
        private int groupConcurrency = 0;

        public Builder<T> concurrency(int concurrency) {
            this.concurrency = concurrency;
            return this;
        }

        public Builder<T> groupAffinity(boolean groupAffinity) {
            this.groupAffinity = groupAffinity;
            return this;
        }

        public Builder<T> groupConcurrency(int groupConcurrency) {
            this.groupConcurrency = Math.max(0, groupConcurrency);
            return this;
        }

        public Builder<T> redisUri(String redisUri) {
            this.connectionManager = new RedisConnectionManager(redisUri);
            this.ownsConnectionManager = true;
            return this;
        }

        public Builder<T> redisClient(RedisClient redisClient) {
            this.connectionManager = new RedisConnectionManager(redisClient);
            this.ownsConnectionManager = true;
            return this;
        }

        public Builder<T> connectionManager(RedisConnectionManager connectionManager) {
            this.connectionManager = connectionManager;
            this.ownsConnectionManager = false;
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
                this.connectionManager = new RedisConnectionManager("redis://localhost:6379");
                this.ownsConnectionManager = true;
            }
            return new OxmqBatchWorker<>(this);
        }
    }
}
