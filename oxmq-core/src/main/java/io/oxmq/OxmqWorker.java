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
import io.oxmq.serializer.BullMsgPack;
import io.oxmq.serializer.JacksonJobSerializer;
import io.oxmq.serializer.JobSerializer;
import io.oxmq.watchdog.LockExtender;
import io.oxmq.watchdog.StalledJobSentinel;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
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
    private final long defaultJobTimeoutMs;
    private final int groupConcurrency;

    private final RedisConnectionManager connectionManager;
    private final boolean ownsConnectionManager;
    private final LuaScriptManager scriptManager;
    private final BullScripts bullScripts;
    private final io.oxmq.lua.ExtensionScripts extensionScripts;
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
    private java.util.concurrent.ScheduledExecutorService timeoutWatchdog;
    private final Map<String, Long> jobTimeoutsMs;
    private io.lettuce.core.pubsub.StatefulRedisPubSubConnection<String, String> controlPubSubConn;
    private io.lettuce.core.pubsub.RedisPubSubListener<String, String> controlListener;
    private final byte[] defaultWorkerOptsBytes;
    private final io.oxmq.model.JobContext workerJobContext;

    @FunctionalInterface
    public interface CustomBackoffStrategy<T> {
        Duration calculateDelay(int attemptsMade, Throwable error, Job<T> job);
    }

    private final Map<String, CustomBackoffStrategy<T>> customBackoffStrategies;
    private final List<Consumer<Job<T>>> activeListeners = new CopyOnWriteArrayList<>();
    private final List<BiConsumer<Job<T>, Object>> completedListeners = new CopyOnWriteArrayList<>();
    private final List<BiConsumer<Job<T>, Throwable>> failedListeners = new CopyOnWriteArrayList<>();
    private final List<BiConsumer<Job<T>, Object>> progressListeners = new CopyOnWriteArrayList<>();
    private final List<Runnable> drainedListeners = new CopyOnWriteArrayList<>();
    private final List<Runnable> pausedListeners = new CopyOnWriteArrayList<>();
    private final List<Runnable> resumedListeners = new CopyOnWriteArrayList<>();
    private final List<Consumer<Throwable>> errorListeners = new CopyOnWriteArrayList<>();

    private record ActiveExecution<T>(Job<T> job, Thread thread) {}
    private final ConcurrentMap<String, ActiveExecution<T>> activeExecutions = new ConcurrentHashMap<>();

    private OxmqWorker(Builder<T> builder) {
        this.queueName = Objects.requireNonNull(builder.queueName, "queueName must not be null");
        this.queueKeys = new QueueKeys(builder.prefix, queueName);
        this.prefix = this.queueKeys.getQualifiedName();
        this.workerId = "worker:" + UUID.randomUUID();
        if (builder.processor != null) {
            this.processor = builder.processor;
        } else if (builder.reactiveProcessor != null) {
            this.processor = job -> awaitReactiveCompletion(job, builder.reactiveProcessor.process(job));
        } else {
            throw new NullPointerException("Either processor or reactiveProcessor must be provided");
        }
        this.payloadClass = builder.payloadClass;
        this.concurrency = Math.max(1, builder.concurrency);
        this.useVirtualThreads = builder.useVirtualThreads;
        this.customExecutor = builder.customExecutor;
        this.lockDurationMs = builder.lockDurationMs;
        this.drainTimeoutMs = builder.drainTimeoutMs;
        this.pollIntervalMs = builder.pollIntervalMs;
        this.maxStalledCount = builder.maxStalledCount;
        this.maxStartedAttempts = builder.maxStartedAttempts;
        this.defaultJobTimeoutMs = builder.defaultJobTimeoutMs;
        this.jobTimeoutsMs = Map.copyOf(builder.jobTimeoutsMs);
        this.groupConcurrency = Math.max(0, builder.groupConcurrency);
        this.ownsConnectionManager = builder.ownsConnectionManager;

        this.connectionManager = Objects.requireNonNull(builder.connectionManager, "connectionManager must not be null");
        this.scriptManager = builder.scriptManager != null ? builder.scriptManager : new LuaScriptManager();
        this.bullScripts = new BullScripts(this.scriptManager);
        this.extensionScripts = new io.oxmq.lua.ExtensionScripts(this.scriptManager);
        this.serializer = builder.serializer != null ? builder.serializer : new JacksonJobSerializer();
        this.metrics = builder.metrics != null ? builder.metrics : new OxmqMetrics();
        this.interceptors = builder.interceptors != null && !builder.interceptors.isEmpty()
                ? List.copyOf(builder.interceptors)
                : List.of();

        this.customBackoffStrategies = Map.copyOf(builder.customBackoffStrategies);
        this.activeListeners.addAll(builder.activeListeners);
        this.completedListeners.addAll(builder.completedListeners);
        this.failedListeners.addAll(builder.failedListeners);
        this.progressListeners.addAll(builder.progressListeners);
        this.drainedListeners.addAll(builder.drainedListeners);
        this.pausedListeners.addAll(builder.pausedListeners);
        this.resumedListeners.addAll(builder.resumedListeners);
        this.errorListeners.addAll(builder.errorListeners);

        if (builder.rateLimitMax > 0 && builder.rateLimitDuration != null) {
            this.rateLimiter = new RateLimiter(connectionManager.getCommandConnection(), this.scriptManager,
                    this.prefix, builder.rateLimitMax, builder.rateLimitDuration);
        } else {
            this.rateLimiter = null;
        }

        this.concurrencySemaphore = new Semaphore(concurrency);

        Map<String, Object> wOpts = new HashMap<>(4);
        wOpts.put("token", workerId);
        wOpts.put("lockDuration", (int) lockDurationMs);
        wOpts.put("name", workerId);
        this.defaultWorkerOptsBytes = BullMsgPack.pack(wOpts);

        this.workerJobContext = new io.oxmq.model.JobContext() {
            @Override
            public void updateData(String jobId, Object newData) {
                try {
                    String serialized = serializer.serialize(newData);
                    bullScripts.updateData(connectionManager.getBinaryConnection(), queueKeys, jobId, serialized);
                } catch (Exception e) {
                    log.warn("Failed to update data for job {} in queue {}: {}", jobId, queueName, e.getMessage());
                }
            }

            @Override
            public void updateProgress(String jobId, int percentage, Object payload) {
                try {
                    bullScripts.updateProgress(connectionManager.getBinaryConnection(), queueKeys, jobId, String.valueOf(percentage));
                    ActiveExecution<T> active = activeExecutions.get(jobId);
                    if (active != null) {
                        emitProgress(active.job, payload != null ? payload : percentage);
                    }
                } catch (Exception e) {
                    log.warn("Failed to sync progress for job {}", jobId, e);
                }
            }

            @Override
            public void addLog(String jobId, String msg) {
                try {
                    bullScripts.addLog(connectionManager.getBinaryConnection(), queueKeys, jobId, msg, 0);
                } catch (Exception e) {
                    log.warn("Failed to append log for job {}", jobId, e);
                }
            }

            @Override
            public boolean clearLogs(String jobId) {
                try {
                    RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
                    Long del = sync.del(queueKeys.toJobLogsKey(jobId));
                    return del != null && del > 0;
                } catch (Exception e) {
                    log.warn("Failed to clear logs for job {} in queue {}: {}", jobId, queueName, e.getMessage());
                    return false;
                }
            }

            @Override
            public void saveCheckpoint(String jobId, String step, Object state) {
                try {
                    String json = serializer.serialize(state);
                    ActiveExecution<T> active = activeExecutions.get(jobId);
                    if (active != null) {
                        active.job.setCheckpointJson(json);
                        active.job.setCheckpointStep(step);
                    }
                    RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
                    String jobKey = queueKeys.toJobKey(jobId);
                    if (step != null && !step.isBlank()) {
                        sync.hset(jobKey, Map.of("checkpoint", json, "checkpoint_step", step));
                    } else {
                        sync.hset(jobKey, "checkpoint", json);
                    }
                    log.debug("Saved checkpoint for job {} [step: {}] in queue {}", jobId, step, queueName);
                } catch (Exception e) {
                    log.warn("Failed to persist checkpoint for job {} in queue {}: {}", jobId, queueName, e.getMessage());
                }
            }

            @Override
            public void spawnChildren(String jobId, java.util.List<io.oxmq.model.FlowJob> children) {
                if (children == null || children.isEmpty()) return;
                try {
                    StatefulRedisConnection<String, byte[]> binConn = connectionManager.getBinaryConnection();
                    String parentKey = queueKeys.toJobKey(jobId);
                    Map<String, Object> parentObj = Map.of("id", jobId, "queueKey", queueKeys.getQualifiedName());

                    for (io.oxmq.model.FlowJob child : children) {
                        JobOptions childOpts = child.getOpts() != null ? child.getOpts() : new JobOptions();
                        Map<String, Object> childOptsMap = new HashMap<>(childOpts.toMap());
                        childOptsMap.put("parentKey", parentKey);
                        childOptsMap.put("parent", parentObj);

                        String childQueueName = child.getQueueName() != null ? child.getQueueName() : queueName;
                        io.oxmq.client.QueueKeys childKeys = childQueueName.equals(queueName)
                                ? queueKeys
                                : new io.oxmq.client.QueueKeys(queueKeys.getPrefix(), childQueueName);
                        String childData = serializer.serialize(child.getData());
                        long nowTs = System.currentTimeMillis();

                        bullScripts.addStandardJob(binConn, childKeys, "", child.getName(), childData, childOptsMap, nowTs);
                    }

                    int moveRes = bullScripts.moveToWaitingChildren(binConn, queueKeys, jobId, workerId, "", System.currentTimeMillis());
                    log.debug("Moved parent job {} to waiting-children: result code {}", jobId, moveRes);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to spawn child jobs for job " + jobId, e);
                }
                throw new io.oxmq.exception.WaitingChildrenException();
            }

            @Override
            public io.oxmq.model.JobDependencies getDependencies(String jobId) {
                try {
                    RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
                    String jobKey = queueKeys.toJobKey(jobId);
                    Set<String> unprocessed = sync.smembers(jobKey + ":dependencies");
                    Map<String, String> processed = sync.hgetall(jobKey + ":processed");
                    Map<String, String> failed = sync.hgetall(jobKey + ":failed");
                    return new io.oxmq.model.JobDependencies(
                            jobId,
                            unprocessed != null ? new ArrayList<>(unprocessed) : Collections.emptyList(),
                            processed != null ? processed : Collections.emptyMap(),
                            failed != null ? failed : Collections.emptyMap()
                    );
                } catch (Exception e) {
                    return new io.oxmq.model.JobDependencies(jobId, Collections.emptyList(), Collections.emptyMap(), Collections.emptyMap());
                }
            }

            @Override
            public Map<String, Object> getChildrenValues(String jobId) {
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
            }

            @Override
            public Map<String, String> getIgnoredChildrenFailures(String jobId) {
                try {
                    RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
                    Map<String, String> failures = sync.hgetall(prefix + ":" + jobId + ":failed");
                    return failures != null ? failures : Map.of();
                } catch (Exception e) {
                    return Map.of();
                }
            }
        };
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

            this.timeoutWatchdog = io.oxmq.watchdog.WatchdogScheduler.sharedScheduler();

            this.stalledJobSentinel = new StalledJobSentinel(connectionManager.getBinaryConnection(),
                    bullScripts, queueKeys, metrics, lockDurationMs, maxStalledCount);

            this.pollerThread = new Thread(this::pollLoop, "oxmq-poller-" + queueName);
            this.pollerThread.setDaemon(true);
            this.pollerThread.start();

            startControlListener();
        }
    }

    private void startControlListener() {
        try {
            this.controlPubSubConn = connectionManager.getPubSubConnection();
            this.controlListener = new io.lettuce.core.pubsub.RedisPubSubAdapter<>() {
                @Override
                public void message(String channel, String message) {
                    if (channel != null && channel.equals(queueKeys.toKey("control"))
                            && message != null && message.startsWith("CANCEL:")) {
                        String[] parts = message.split(":", 3);
                        if (parts.length >= 2) {
                            String targetJobId = parts[1];
                            String cancelReason = parts.length >= 3 ? parts[2] : "Cluster cancellation requested";
                            if (isJobActive(targetJobId)) {
                                log.info("Received cluster-wide CANCEL for active job [id: {}] on queue {}: {}",
                                        targetJobId, queueName, cancelReason);
                                cancelJob(targetJobId, cancelReason);
                            }
                        }
                    }
                }
            };
            this.controlPubSubConn.addListener(this.controlListener);
            this.controlPubSubConn.sync().subscribe(queueKeys.toKey("control"));
            log.debug("OxMQ Worker [{}] subscribed to control channel {}", workerId, queueKeys.toKey("control"));
        } catch (Exception e) {
            log.warn("Failed to subscribe to control channel for queue {}: {}", queueName, e.getMessage());
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
                List<Object> rawResult;
                if (groupConcurrency > 0) {
                    rawResult = extensionScripts.moveToActiveGroup(pollerConn, queueKeys, workerId,
                            lockDurationMs, groupConcurrency, now);
                    if (rawResult == null || rawResult.size() < 2 || rawResult.get(1) == null
                            || "0".equals(rawResult.get(1).toString()) || "".equals(rawResult.get(1).toString())) {
                        rawResult = bullScripts.moveToActive(pollerConn, queueKeys, defaultWorkerOptsBytes, now);
                    }
                } else {
                    rawResult = bullScripts.moveToActive(pollerConn, queueKeys, defaultWorkerOptsBytes, now);
                    if (rawResult == null || rawResult.size() < 2 || rawResult.get(1) == null
                            || "0".equals(rawResult.get(1).toString()) || "".equals(rawResult.get(1).toString())) {
                        List<Object> groupResult = extensionScripts.moveToActiveGroup(pollerConn, queueKeys, workerId,
                                lockDurationMs, groupConcurrency, now);
                        if (groupResult != null && groupResult.size() >= 2 && groupResult.get(1) != null
                                && !"0".equals(groupResult.get(1).toString()) && !"".equals(groupResult.get(1).toString())) {
                            rawResult = groupResult;
                        }
                    }
                }

                if (rawResult == null || rawResult.size() < 2 || rawResult.get(1) == null) {
                    concurrencySemaphore.release();
                    emitDrained();
                    waitForMarkerOrSleep(pollerConn);
                    continue;
                }

                Object idObj = rawResult.get(1);
                String jobId = idObj instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : idObj.toString();

                if (jobId == null || jobId.isEmpty() || "0".equals(jobId)) {
                    long expireTime = rawResult.size() > 2 && rawResult.get(2) instanceof Number n ? n.longValue() : 0L;
                    concurrencySemaphore.release();
                    emitDrained();
                    if (expireTime > 0) {
                        TimeUnit.MILLISECONDS.sleep(Math.min(expireTime, pollIntervalMs));
                    } else {
                        waitForMarkerOrSleep(pollerConn);
                    }
                    continue;
                }

                @SuppressWarnings("unchecked")
                List<?> hashFlat = rawResult.get(0) instanceof List<?> list ? list : List.of();
                Map<String, String> fields = parseHashFields(hashFlat);

                Job<T> job = buildJobFromFields(jobId, fields);
                lockExtender.registerJob(jobId);

                if (job.getRepeatJobKey() != null && !job.getRepeatJobKey().isBlank()) {
                    rescheduleJobScheduler(job.getRepeatJobKey(), job);
                }

                dispatcherExecutor.submit(() -> executeJob(job));

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (!running.get() || Thread.currentThread().isInterrupted() || e instanceof io.lettuce.core.RedisCommandInterruptedException) {
                    Thread.currentThread().interrupt();
                    break;
                }
                emitError(e);
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
        emitActive(job);

        long effectiveTimeoutMs = job.getOpts() != null && job.getOpts().getJobTimeoutMs() > 0
                ? job.getOpts().getJobTimeoutMs()
                : (jobTimeoutsMs.containsKey(job.getName()) ? jobTimeoutsMs.get(job.getName()) : defaultJobTimeoutMs);

        java.util.concurrent.ScheduledFuture<?> timeoutFuture = null;
        if (effectiveTimeoutMs > 0 && timeoutWatchdog != null && !timeoutWatchdog.isShutdown()) {
            final Thread jobThread = Thread.currentThread();
            timeoutFuture = timeoutWatchdog.schedule(() -> {
                if (activeExecutions.containsKey(job.getId())) {
                    String timeoutReason = "Job execution timed out after " + effectiveTimeoutMs + " ms";
                    log.warn("Job {} [id: {}] timed out after {} ms on queue {}",
                            job.getName(), job.getId(), effectiveTimeoutMs, queueName);
                    job.setCancelled(true, timeoutReason);
                    job.getCancellationToken().cancel(timeoutReason);
                    jobThread.interrupt();
                }
            }, effectiveTimeoutMs, TimeUnit.MILLISECONDS);
        }

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
            Object rawResult = processor.process(job);
            Object result = rawResult;
            if (rawResult instanceof java.util.concurrent.Flow.Publisher<?> publisher) {
                result = awaitReactiveCompletion(job, publisher);
            } else if (rawResult instanceof org.reactivestreams.Publisher<?> rsPublisher) {
                result = awaitReactiveCompletion(job, org.reactivestreams.FlowAdapters.toFlowPublisher(rsPublisher));
            } else if (rawResult instanceof java.util.concurrent.Future<?> future) {
                result = awaitFutureCompletion(job, future);
            }

            long durationNanos = System.nanoTime() - startTime;
            Duration duration = Duration.ofNanos(durationNanos);

            completeJob(job, result, duration);
            metrics.recordJobCompleted(queueName, job.getGroupKey(), duration);
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
            Thread.interrupted(); // Clear interrupted flag so Redis cleanup commands can complete
            long durationNanos = System.nanoTime() - startTime;
            Duration duration = Duration.ofNanos(durationNanos);
            String reason = job.getCancelReason() != null ? job.getCancelReason() : (ie.getMessage() != null ? ie.getMessage() : "Job cancelled");
            job.setCancelled(true, reason);
            Throwable finalError = ie;
            if (reason.toLowerCase().contains("timed out") || ie instanceof io.oxmq.exception.JobTimeoutException) {
                finalError = new io.oxmq.exception.JobTimeoutException(reason, Duration.ofMillis(effectiveTimeoutMs));
            }
            failJob(job, finalError, duration);
            metrics.recordJobFailed(queueName, job.getGroupKey(), duration, finalError.getClass().getSimpleName());
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
            Thread.interrupted(); // Clear interrupted flag if set
            long durationNanos = System.nanoTime() - startTime;
            Duration duration = Duration.ofNanos(durationNanos);

            Throwable finalError = t;
            if (job.isCancelled() && job.getCancelReason() != null && job.getCancelReason().toLowerCase().contains("timed out")) {
                finalError = new io.oxmq.exception.JobTimeoutException(job.getCancelReason(), Duration.ofMillis(effectiveTimeoutMs));
            }

            boolean handledByInterceptor = false;
            if (!interceptors.isEmpty()) {
                for (io.oxmq.interceptor.JobInterceptor interceptor : interceptors) {
                    try {
                        io.oxmq.interceptor.ErrorDecision decision = interceptor.onError(job, finalError, duration);
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
                failJob(job, finalError, duration);
                metrics.recordJobFailed(queueName, job.getGroupKey(), duration, finalError.getClass().getSimpleName());
                log.warn("Job {} [id: {}] failed on queue {}: {}", job.getName(), job.getId(), queueName, finalError.toString(), finalError);
            }
        } finally {
            if (timeoutFuture != null) {
                timeoutFuture.cancel(false);
            }
            activeExecutions.remove(job.getId());
            lockExtender.unregisterJob(job.getId());
            concurrencySemaphore.release();
        }
    }

    private Object awaitFutureCompletion(Job<T> job, java.util.concurrent.Future<?> future) throws Exception {
        if (future == null) {
            return null;
        }
        job.getCancellationToken().onCancel(reason -> future.cancel(true));
        try {
            return future.get();
        } catch (java.util.concurrent.ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof Exception ex) {
                throw ex;
            } else if (cause instanceof Error err) {
                throw err;
            } else {
                throw new RuntimeException(cause);
            }
        }
    }

    private Object awaitReactiveCompletion(Job<T> job, java.util.concurrent.Flow.Publisher<?> publisher) throws Exception {
        if (publisher == null) {
            return null;
        }

        java.util.concurrent.CompletableFuture<Object> completionFuture = new java.util.concurrent.CompletableFuture<>();
        java.util.concurrent.atomic.AtomicReference<java.util.concurrent.Flow.Subscription> subscriptionRef = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<Object> lastItemRef = new java.util.concurrent.atomic.AtomicReference<>();

        publisher.subscribe(new java.util.concurrent.Flow.Subscriber<Object>() {
            @Override
            public void onSubscribe(java.util.concurrent.Flow.Subscription subscription) {
                subscriptionRef.set(subscription);
                job.getCancellationToken().onCancel(reason -> {
                    subscription.cancel();
                    completionFuture.completeExceptionally(
                            new io.oxmq.exception.JobCancelledException(reason != null ? reason : "Cancelled"));
                });
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Object item) {
                lastItemRef.set(item);
                if (item instanceof Number num) {
                    try {
                        job.updateProgress(num.intValue());
                    } catch (Exception e) {
                        log.debug("Failed to update progress from reactive emission for job {}: {}", job.getId(), e.getMessage());
                    }
                } else if (item != null) {
                    try {
                        if (item instanceof CharSequence seq) {
                            try {
                                int p = Integer.parseInt(seq.toString().trim());
                                job.updateProgress(p);
                                return;
                            } catch (NumberFormatException ignored) {}
                        }
                        job.updateProgress(job.getProgress(), item);
                    } catch (Exception e) {
                        log.debug("Failed to checkpoint reactive payload for job {}: {}", job.getId(), e.getMessage());
                    }
                }
            }

            @Override
            public void onError(Throwable throwable) {
                completionFuture.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                completionFuture.complete(lastItemRef.get());
            }
        });

        try {
            return completionFuture.get();
        } catch (InterruptedException ie) {
            java.util.concurrent.Flow.Subscription s = subscriptionRef.get();
            if (s != null) {
                s.cancel();
            }
            Thread.currentThread().interrupt();
            throw ie;
        } catch (java.util.concurrent.ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof Exception ex) {
                throw ex;
            } else if (cause instanceof Error err) {
                throw err;
            } else {
                throw new RuntimeException(cause);
            }
        }
    }

    private void completeJob(Job<T> job, Object result, Duration duration) {
        String serializedResult = serializer.serialize(result);
        JobOptions opts = job.getOpts();
        bullScripts.moveToFinished(connectionManager.getBinaryConnection(), queueKeys, job.getId(),
                serializedResult, "returnvalue", "completed", workerId, false, opts != null ? opts.toMap() : Map.of());

        String groupId = job.getGroupKey();
        if (groupId != null && !groupId.isBlank()) {
            extensionScripts.decrementGroupActiveAsync(connectionManager.getBinaryConnection(), queueKeys, groupId, groupConcurrency);
        }

        emitCompleted(job, result);
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

        boolean isTimeout = (error instanceof io.oxmq.exception.JobTimeoutException) ||
                            (job.getCancelReason() != null && job.getCancelReason().toLowerCase().contains("timed out"));

        boolean isUnrecoverable = (error instanceof UnrecoverableError) ||
                                  (error.getCause() instanceof UnrecoverableError) ||
                                  (!isTimeout && (error instanceof io.oxmq.exception.JobCancelledException)) ||
                                  (!isTimeout && (error instanceof InterruptedException)) ||
                                  (!isTimeout && job.isCancelled()) ||
                                  job.isDiscarded();

        if (maxStartedAttempts > 0 && job.getAttemptsStarted() >= maxStartedAttempts) {
            isUnrecoverable = true;
        }

        JobOptions opts = job.getOpts();
        int maxAttempts = opts != null && opts.getAttempts() > 0 ? opts.getAttempts() : 1;
        int currentAttempts = job.getAttemptsMade() + 1;

        String failedReasonValue;
        if (isTimeout) {
            failedReasonValue = job.getCancelReason() != null ? job.getCancelReason() : error.getMessage();
        } else if (job.isCancelled()) {
            failedReasonValue = job.getCancelReason() != null ? "Job cancelled: " + job.getCancelReason() : "Job cancelled";
        } else {
            failedReasonValue = stackTrace;
        }

        Map<String, Object> fieldsToUpdate = new HashMap<>();
        fieldsToUpdate.put("failedReason", failedReasonValue);
        fieldsToUpdate.put("stacktrace", serializer.serialize(List.of(stackTrace)));

        String groupId = job.getGroupKey();
        if (groupId != null && !groupId.isBlank()) {
            extensionScripts.decrementGroupActiveAsync(connectionManager.getBinaryConnection(), queueKeys, groupId, groupConcurrency);
        }

        if (!isUnrecoverable && currentAttempts < maxAttempts) {
            BackoffStrategy backoff = opts != null ? opts.getBackoff() : null;
            long delayMs = 0;
            if (backoff instanceof BackoffStrategy.Custom custom) {
                CustomBackoffStrategy<T> customFn = customBackoffStrategies.get(custom.name());
                if (customFn != null) {
                    Duration d = customFn.calculateDelay(currentAttempts, error, job);
                    delayMs = d != null ? Math.max(0, d.toMillis()) : 0;
                }
            } else if (backoff != null) {
                delayMs = backoff.calculateDelayMs(currentAttempts);
            }

            if (delayMs > 0) {
                bullScripts.moveToDelayed(connectionManager.getBinaryConnection(), queueKeys,
                        job.getId(), workerId, delayMs, false, fieldsToUpdate);
                emitFailed(job, error);
                log.debug("Job {} [id: {}] failed (attempt {}/{}), delayed by {} ms for retry",
                        job.getName(), job.getId(), currentAttempts, maxAttempts, delayMs);
                return;
            } else {
                bullScripts.retryJob(connectionManager.getBinaryConnection(), queueKeys,
                        job.getId(), workerId, opts != null && opts.isLifo(), fieldsToUpdate);
                emitFailed(job, error);
                log.debug("Job {} [id: {}] failed (attempt {}/{}), moved to wait for retry",
                        job.getName(), job.getId(), currentAttempts, maxAttempts);
                return;
            }
        }

        bullScripts.moveToFinished(connectionManager.getBinaryConnection(), queueKeys, job.getId(),
                failedReasonValue, "failedReason", "failed", workerId, false, opts != null ? opts.toMap() : Map.of());
        emitFailed(job, error);
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
        JobOptions opts = (optsStr == null || optsStr.isBlank() || "{}".equals(optsStr))
                ? JobOptions.defaults()
                : serializer.deserialize(optsStr, JobOptions.class);

        Job<T> job = new Job<>(jobId, jobName, data, opts);
        job.setQueueName(queueName);
        String groupKeyStr = fields.get("groupKey");
        if (groupKeyStr != null && !groupKeyStr.isBlank()) {
            job.setGroupKey(groupKeyStr);
        }
        if (attemptsStr != null) job.setAttemptsMade(Integer.parseInt(attemptsStr));
        if (attemptsStartedStr != null) job.setAttemptsStarted(Integer.parseInt(attemptsStartedStr));
        if (timestampStr != null) {
            long ts = Long.parseLong(timestampStr);
            job.setTimestamp(ts);
            long waitTimeMs = System.currentTimeMillis() - ts;
            metrics.recordJobWaitTime(queueName, Duration.ofMillis(Math.max(0, waitTimeMs)));
        }
        if (parentKeyStr != null) job.setParentKey(parentKeyStr);
        String checkpointStr = fields.get("checkpoint");
        String checkpointStepStr = fields.get("checkpoint_step");
        if (checkpointStr != null) job.setCheckpointJson(checkpointStr);
        if (checkpointStepStr != null) job.setCheckpointStep(checkpointStepStr);
        String rjkStr = fields.get("rjk");
        if (rjkStr != null && !rjkStr.isBlank()) {
            job.setRepeatJobKey(rjkStr);
        }

        job.setContext(workerJobContext);
        return job;
    }

    private Map<String, String> parseHashFields(List<?> flatList) {
        if (flatList == null || flatList.isEmpty()) {
            return Map.of();
        }
        int expectedEntries = flatList.size() / 2;
        Map<String, String> map = new HashMap<>(Math.max(8, (int) (expectedEntries / 0.75f) + 1));
        for (int i = 0; i < flatList.size() - 1; i += 2) {
            String canonicalKey = matchCanonicalKey(flatList.get(i));
            if (canonicalKey != null) {
                String val = toUtf8String(flatList.get(i + 1));
                map.put(canonicalKey, val);
            }
        }
        return map;
    }

    static String matchCanonicalKey(Object obj) {
        if (obj instanceof byte[] b) {
            return matchCanonicalBytes(b);
        }
        return obj != null ? CANONICAL_KEYS.getOrDefault(obj.toString(), obj.toString()) : null;
    }

    static String matchCanonicalBytes(byte[] b) {
        int len = b.length;
        switch (len) {
            case 2:
                if (b[0] == 'p' && b[1] == 'b') return "pb";
                break;
            case 3:
                if (b[0] == 'r' && b[1] == 'j' && b[2] == 'k') return "rjk";
                if (b[0] == 'a' && b[1] == 't' && b[2] == 's') return "ats";
                break;
            case 4:
                if (b[0] == 'n' && b[1] == 'a' && b[2] == 'm' && b[3] == 'e') return "name";
                if (b[0] == 'd' && b[1] == 'a' && b[2] == 't' && b[3] == 'a') return "data";
                if (b[0] == 'o' && b[1] == 'p' && b[2] == 't' && b[3] == 's') return "opts";
                if (b[0] == 'd' && b[1] == 'e' && b[2] == 'i' && b[3] == 'd') return "deid";
                break;
            case 5:
                if (b[0] == 'd' && b[1] == 'e' && b[2] == 'l' && b[3] == 'a' && b[4] == 'y') return "delay";
                break;
            case 6:
                if (b[0] == 'p' && b[1] == 'a' && b[2] == 'r' && b[3] == 'e' && b[4] == 'n' && b[5] == 't') return "parent";
                break;
            case 8:
                if (b[0] == 'p' && b[1] == 'r') {
                    if (b[2] == 'i' && b[3] == 'o' && b[4] == 'r' && b[5] == 'i' && b[6] == 't' && b[7] == 'y') return "priority";
                    if (b[2] == 'o' && b[3] == 'g' && b[4] == 'r' && b[5] == 'e' && b[6] == 's' && b[7] == 's') return "progress";
                }
                if (b[0] == 'g' && b[1] == 'r' && b[2] == 'o' && b[3] == 'u' && b[4] == 'p' && b[5] == 'K' && b[6] == 'e' && b[7] == 'y') return "groupKey";
                break;
            case 9:
                if (b[0] == 't' && b[1] == 'i' && b[2] == 'm' && b[3] == 'e' && b[4] == 's' && b[5] == 't' && b[6] == 'a' && b[7] == 'm' && b[8] == 'p') return "timestamp";
                if (b[0] == 'p' && b[1] == 'a' && b[2] == 'r' && b[3] == 'e' && b[4] == 'n' && b[5] == 't' && b[6] == 'K' && b[7] == 'e' && b[8] == 'y') return "parentKey";
                break;
            case 10:
                if (b[0] == 'f' && b[1] == 'i' && b[2] == 'n' && b[3] == 'i' && b[4] == 's' && b[5] == 'h' && b[6] == 'e' && b[7] == 'd' && b[8] == 'O' && b[9] == 'n') return "finishedOn";
                if (b[0] == 'c' && b[1] == 'h' && b[2] == 'e' && b[3] == 'c' && b[4] == 'k' && b[5] == 'p' && b[6] == 'o' && b[7] == 'i' && b[8] == 'n' && b[9] == 't') return "checkpoint";
                break;
            case 11:
                if (b[0] == 'p' && b[1] == 'r' && b[2] == 'o' && b[3] == 'c' && b[4] == 'e' && b[5] == 's' && b[6] == 's' && b[7] == 'e' && b[8] == 'd' && b[9] == 'O' && b[10] == 'n') return "processedOn";
                if (b[0] == 'r' && b[1] == 'e' && b[2] == 't' && b[3] == 'u' && b[4] == 'r' && b[5] == 'n' && b[6] == 'v' && b[7] == 'a' && b[8] == 'l' && b[9] == 'u' && b[10] == 'e') return "returnvalue";
                break;
            case 12:
                if (b[0] == 'a' && b[1] == 't' && b[2] == 't' && b[3] == 'e' && b[4] == 'm' && b[5] == 'p' && b[6] == 't' && b[7] == 's' && b[8] == 'M' && b[9] == 'a' && b[10] == 'd' && b[11] == 'e') return "attemptsMade";
                if (b[0] == 'f' && b[1] == 'a' && b[2] == 'i' && b[3] == 'l' && b[4] == 'e' && b[5] == 'd' && b[6] == 'R' && b[7] == 'e' && b[8] == 'a' && b[9] == 's' && b[10] == 'o' && b[11] == 'n') return "failedReason";
                break;
            case 15:
                if (b[0] == 'a' && b[1] == 't' && b[2] == 't' && b[3] == 'e' && b[4] == 'm' && b[5] == 'p' && b[6] == 't' && b[7] == 's' && b[8] == 'S' && b[9] == 't' && b[10] == 'a' && b[11] == 'r' && b[12] == 't' && b[13] == 'e' && b[14] == 'd') return "attemptsStarted";
                if (b[0] == 'c' && b[1] == 'h' && b[2] == 'e' && b[3] == 'c' && b[4] == 'k' && b[5] == 'p' && b[6] == 'o' && b[7] == 'i' && b[8] == 'n' && b[9] == 't' && b[10] == '_' && b[11] == 's' && b[12] == 't' && b[13] == 'e' && b[14] == 'p') return "checkpoint_step";
                break;
        }
        String fallback = new String(b, StandardCharsets.UTF_8);
        return CANONICAL_KEYS.getOrDefault(fallback, fallback);
    }

    private String toUtf8String(Object obj) {
        if (obj instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return obj != null ? obj.toString() : null;
    }

    private void waitForMarkerOrSleep(StatefulRedisConnection<String, byte[]> pollerConn) {
        double blockTimeout = Math.max(0.05, pollIntervalMs / 1000.0);
        try {
            pollerConn.sync().bzpopmin(blockTimeout, queueKeys.getKey("marker"));
        } catch (Exception e) {
            try {
                TimeUnit.MILLISECONDS.sleep(Math.min(50, pollIntervalMs));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void rescheduleJobScheduler(String schedulerId, Job<T> job) {
        dispatcherExecutor.submit(() -> {
            try {
                RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
                String schedulerKey = queueKeys.toKey("repeat:" + schedulerId);
                Map<String, String> schedData = sync.hgetall(schedulerKey);
                if (schedData == null || schedData.isEmpty()) {
                    return;
                }

                long now = System.currentTimeMillis();
                String endDateStr = schedData.get("endDate");
                if (endDateStr != null && !endDateStr.isBlank()) {
                    try {
                        long endDate = Long.parseLong(endDateStr);
                        if (now > endDate) {
                            log.debug("JobScheduler {} reached endDate {}, skipping reschedule", schedulerId, endDate);
                            return;
                        }
                    } catch (NumberFormatException ignored) {}
                }

                String limitStr = schedData.get("limit");
                String icStr = schedData.get("ic");
                if (limitStr != null && !limitStr.isBlank() && icStr != null && !icStr.isBlank()) {
                    try {
                        int limit = Integer.parseInt(limitStr);
                        int ic = Integer.parseInt(icStr);
                        if (ic >= limit) {
                            log.debug("JobScheduler {} reached iteration limit ({}/{}), skipping reschedule", schedulerId, ic, limit);
                            return;
                        }
                    } catch (NumberFormatException ignored) {}
                }

                String everyStr = schedData.get("every");
                String pattern = schedData.get("pattern");
                String tz = schedData.get("tz");
                String name = schedData.getOrDefault("name", job.getName());
                String dataJson = schedData.getOrDefault("data", "{}");
                String optsJson = schedData.get("opts");
                @SuppressWarnings("unchecked")
                Map<String, Object> optsMap = (optsJson != null && !optsJson.isBlank())
                        ? serializer.deserialize(optsJson, Map.class)
                        : Map.of();

                Long every = null;
                long nextMillis;

                if (everyStr != null && !everyStr.isBlank()) {
                    every = Long.parseLong(everyStr);
                    nextMillis = now + every;
                } else if (pattern != null && !pattern.isBlank()) {
                    nextMillis = io.oxmq.util.CronEvaluator.parse(pattern).nextExecution(now, tz);
                } else {
                    return;
                }

                bullScripts.addJobScheduler(
                        connectionManager.getBinaryConnection(),
                        queueKeys,
                        schedulerId,
                        nextMillis,
                        name,
                        dataJson,
                        optsMap,
                        every,
                        pattern,
                        tz
                );
                log.debug("Rescheduled next occurrence for JobScheduler {} at {} (in {} ms)",
                        schedulerId, nextMillis, Math.max(0, nextMillis - now));
            } catch (Exception e) {
                log.warn("Failed to reschedule JobScheduler {} for job {}", schedulerId, job.getId(), e);
            }
        });
    }

    @Override
    public void pause() {
        if (paused.compareAndSet(false, true)) {
            emitPaused();
            log.info("Paused worker [{}] for queue {}", workerId, queueName);
        }
    }

    @Override
    public void resume() {
        if (paused.compareAndSet(true, false)) {
            emitResumed();
            log.info("Resumed worker [{}] for queue {}", workerId, queueName);
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isPaused() {
        return paused.get();
    }

    /**
     * Dynamically throttles a virtual group across the cluster for the specified duration.
     *
     * @param groupId virtual group identifier
     * @param duration duration to throttle
     * @return true if rate limit was applied
     */
    public boolean rateLimitGroup(String groupId, Duration duration) {
        return rateLimitGroup(groupId, 1, duration);
    }

    /**
     * Dynamically throttles a virtual group across the cluster allowing up to maxJobs within the window.
     * Mirrors BullMQ Pro's setGroupRateLimit(groupId, max, duration).
     *
     * @param groupId  virtual group identifier
     * @param maxJobs  maximum number of jobs allowed in the duration window
     * @param duration duration of the window
     * @return true if rate limit was applied
     */
    public boolean rateLimitGroup(String groupId, int maxJobs, Duration duration) {
        return rateLimitGroup(groupId, maxJobs, duration != null ? duration.toMillis() : 0);
    }

    /**
     * Dynamically throttles a virtual group across the cluster allowing up to maxJobs within the window in milliseconds.
     *
     * @param groupId    virtual group identifier
     * @param maxJobs    maximum number of jobs allowed in the duration window
     * @param durationMs duration in milliseconds
     * @return true if rate limit was applied
     */
    public boolean rateLimitGroup(String groupId, int maxJobs, long durationMs) {
        return extensionScripts.setGroupRateLimit(connectionManager.getBinaryConnection(), queueKeys, groupId,
                maxJobs, durationMs);
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
    public OxmqWorker<T> onActive(Consumer<Job<T>> listener) {
        if (listener != null) activeListeners.add(listener);
        return this;
    }

    @Override
    public OxmqWorker<T> onCompleted(BiConsumer<Job<T>, Object> listener) {
        if (listener != null) completedListeners.add(listener);
        return this;
    }

    @Override
    public OxmqWorker<T> onFailed(BiConsumer<Job<T>, Throwable> listener) {
        if (listener != null) failedListeners.add(listener);
        return this;
    }

    @Override
    public OxmqWorker<T> onProgress(BiConsumer<Job<T>, Object> listener) {
        if (listener != null) progressListeners.add(listener);
        return this;
    }

    @Override
    public OxmqWorker<T> onDrained(Runnable listener) {
        if (listener != null) drainedListeners.add(listener);
        return this;
    }

    @Override
    public OxmqWorker<T> onPaused(Runnable listener) {
        if (listener != null) pausedListeners.add(listener);
        return this;
    }

    @Override
    public OxmqWorker<T> onResumed(Runnable listener) {
        if (listener != null) resumedListeners.add(listener);
        return this;
    }

    @Override
    public OxmqWorker<T> onError(Consumer<Throwable> listener) {
        if (listener != null) errorListeners.add(listener);
        return this;
    }

    private void emitActive(Job<T> job) {
        for (Consumer<Job<T>> l : activeListeners) {
            try { l.accept(job); } catch (Throwable e) { log.warn("Error in worker onActive listener", e); }
        }
    }

    private void emitCompleted(Job<T> job, Object result) {
        for (BiConsumer<Job<T>, Object> l : completedListeners) {
            try { l.accept(job, result); } catch (Throwable e) { log.warn("Error in worker onCompleted listener", e); }
        }
    }

    private void emitFailed(Job<T> job, Throwable error) {
        for (BiConsumer<Job<T>, Throwable> l : failedListeners) {
            try { l.accept(job, error); } catch (Throwable e) { log.warn("Error in worker onFailed listener", e); }
        }
    }

    private void emitProgress(Job<T> job, Object progress) {
        for (BiConsumer<Job<T>, Object> l : progressListeners) {
            try { l.accept(job, progress); } catch (Throwable e) { log.warn("Error in worker onProgress listener", e); }
        }
    }

    private void emitDrained() {
        for (Runnable l : drainedListeners) {
            try { l.run(); } catch (Throwable e) { log.warn("Error in worker onDrained listener", e); }
        }
    }

    private void emitPaused() {
        for (Runnable l : pausedListeners) {
            try { l.run(); } catch (Throwable e) { log.warn("Error in worker onPaused listener", e); }
        }
    }

    private void emitResumed() {
        for (Runnable l : resumedListeners) {
            try { l.run(); } catch (Throwable e) { log.warn("Error in worker onResumed listener", e); }
        }
    }

    private void emitError(Throwable error) {
        for (Consumer<Throwable> l : errorListeners) {
            try { l.accept(error); } catch (Throwable e) { log.warn("Error in worker onError listener", e); }
        }
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

            if (controlPubSubConn != null) {
                try {
                    controlPubSubConn.sync().unsubscribe(queueKeys.toKey("control"));
                    if (controlListener != null) {
                        controlPubSubConn.removeListener(controlListener);
                    }
                } catch (Exception ignored) {}
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
        private io.oxmq.reactive.ReactiveJobProcessor<T, ?> reactiveProcessor;
        private Class<T> payloadClass;
        private int concurrency = 20;
        private int groupConcurrency = 0;
        private boolean useVirtualThreads = true;
        private ExecutorService customExecutor;
        private long lockDurationMs = 30_000;
        private long drainTimeoutMs = 10_000;
        private long pollIntervalMs = 50;
        private long defaultJobTimeoutMs = 0;
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

        public Builder<T> reactiveProcessor(io.oxmq.reactive.ReactiveJobProcessor<T, ?> reactiveProcessor) {
            this.reactiveProcessor = reactiveProcessor;
            return this;
        }

        public Builder<T> reactiveStreamsProcessor(io.oxmq.reactive.ReactiveStreamProcessor<T, ?> reactiveStreamsProcessor) {
            this.reactiveProcessor = io.oxmq.reactive.ReactiveJobProcessor.from(reactiveStreamsProcessor);
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

        public Builder<T> groupConcurrency(int groupConcurrency) {
            this.groupConcurrency = Math.max(0, groupConcurrency);
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

        public Builder<T> defaultJobTimeout(Duration defaultJobTimeout) {
            this.defaultJobTimeoutMs = defaultJobTimeout != null ? defaultJobTimeout.toMillis() : 0;
            return this;
        }

        public Builder<T> defaultJobTimeoutMs(long defaultJobTimeoutMs) {
            this.defaultJobTimeoutMs = defaultJobTimeoutMs;
            return this;
        }

        private final Map<String, Long> jobTimeoutsMs = new HashMap<>();

        public Builder<T> jobTimeout(String jobName, Duration timeout) {
            if (jobName != null && timeout != null) {
                this.jobTimeoutsMs.put(jobName, timeout.toMillis());
            }
            return this;
        }

        public Builder<T> jobTimeouts(Map<String, Duration> timeouts) {
            if (timeouts != null) {
                timeouts.forEach((k, v) -> {
                    if (k != null && v != null) {
                        this.jobTimeoutsMs.put(k, v.toMillis());
                    }
                });
            }
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

        private final Map<String, CustomBackoffStrategy<T>> customBackoffStrategies = new HashMap<>();
        private final List<Consumer<Job<T>>> activeListeners = new ArrayList<>();
        private final List<BiConsumer<Job<T>, Object>> completedListeners = new ArrayList<>();
        private final List<BiConsumer<Job<T>, Throwable>> failedListeners = new ArrayList<>();
        private final List<BiConsumer<Job<T>, Object>> progressListeners = new ArrayList<>();
        private final List<Runnable> drainedListeners = new ArrayList<>();
        private final List<Runnable> pausedListeners = new ArrayList<>();
        private final List<Runnable> resumedListeners = new ArrayList<>();
        private final List<Consumer<Throwable>> errorListeners = new ArrayList<>();

        public Builder<T> backoffStrategy(String name, CustomBackoffStrategy<T> strategy) {
            this.customBackoffStrategies.put(name, strategy);
            return this;
        }

        public Builder<T> backoffStrategy(String name, BiFunction<Integer, Throwable, Duration> strategy) {
            this.customBackoffStrategies.put(name, (attempts, err, job) -> strategy.apply(attempts, err));
            return this;
        }

        public Builder<T> onActive(Consumer<Job<T>> listener) {
            if (listener != null) this.activeListeners.add(listener);
            return this;
        }

        public Builder<T> onCompleted(BiConsumer<Job<T>, Object> listener) {
            if (listener != null) this.completedListeners.add(listener);
            return this;
        }

        public Builder<T> onFailed(BiConsumer<Job<T>, Throwable> listener) {
            if (listener != null) this.failedListeners.add(listener);
            return this;
        }

        public Builder<T> onProgress(BiConsumer<Job<T>, Object> listener) {
            if (listener != null) this.progressListeners.add(listener);
            return this;
        }

        public Builder<T> onDrained(Runnable listener) {
            if (listener != null) this.drainedListeners.add(listener);
            return this;
        }

        public Builder<T> onPaused(Runnable listener) {
            if (listener != null) this.pausedListeners.add(listener);
            return this;
        }

        public Builder<T> onResumed(Runnable listener) {
            if (listener != null) this.resumedListeners.add(listener);
            return this;
        }

        public Builder<T> onError(Consumer<Throwable> listener) {
            if (listener != null) this.errorListeners.add(listener);
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
