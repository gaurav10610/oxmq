package io.oxmq;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XReadArgs.StreamOffset;
import io.lettuce.core.api.StatefulRedisConnection;
import io.oxmq.client.QueueKeys;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.serializer.JacksonJobSerializer;
import io.oxmq.serializer.JobSerializer;
import java.io.Closeable;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Event listener for real-time OxMQ queue lifecycle events backed by Redis Streams (with Pub/Sub compatibility).
 * Provides 100% parity with BullMQ's QueueEvents.
 */
public class QueueEvents implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(QueueEvents.class);

    private final String prefix;
    private final String queueName;
    private final String channelName;
    private final RedisConnectionManager connectionManager;
    private final boolean ownsConnectionManager;
    private final JobSerializer serializer;
    private final Map<String, List<BiConsumer<String, Map<String, Object>>>> eventListeners = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, List<CompletableFuture<Object>>> pendingJobFutures = new ConcurrentHashMap<>();
    private Thread streamThread;
    private volatile boolean running = false;

    public QueueEvents(String prefix, String queueName, RedisConnectionManager connectionManager, JobSerializer serializer) {
        this(prefix, queueName, connectionManager, serializer, false);
    }

    public QueueEvents(String prefix, String queueName, RedisConnectionManager connectionManager, JobSerializer serializer, boolean ownsConnectionManager) {
        this.queueName = Objects.requireNonNull(queueName, "queueName must not be null");
        this.prefix = QueueKeys.resolvePrefix(prefix);
        this.channelName = this.prefix + ":" + queueName + ":events";
        this.connectionManager = Objects.requireNonNull(connectionManager, "connectionManager must not be null");
        this.serializer = serializer != null ? serializer : new JacksonJobSerializer();
        this.ownsConnectionManager = ownsConnectionManager;
    }

    public QueueEvents(String prefix, String queueName, String redisUri) {
        this(prefix, queueName, new RedisConnectionManager(redisUri), new JacksonJobSerializer(), true);
    }

    public QueueEvents(String prefix, String queueName, RedisClient redisClient) {
        this(prefix, queueName, new RedisConnectionManager(redisClient), new JacksonJobSerializer(), true);
    }

    public QueueEvents(String queueName, String redisUri) {
        this(null, queueName, redisUri);
    }

    public QueueEvents(String queueName, RedisClient redisClient) {
        this(null, queueName, redisClient);
    }

    public QueueEvents(String queueName, RedisConnectionManager connectionManager, JobSerializer serializer) {
        this(null, queueName, connectionManager, serializer, false);
    }

    public String getPrefix() {
        return prefix;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getChannelName() {
        return channelName;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Starts listening to Redis Streams queue events.
     */
    public synchronized QueueEvents start() {
        if (!running) {
            this.running = true;
            this.streamThread = new Thread(this::streamPollingLoop, "oxmq-queue-events-" + channelName);
            this.streamThread.setDaemon(true);
            this.streamThread.start();
            log.info("QueueEvents started listening on Redis stream: {}", channelName);
        }
        return this;
    }

    private void streamPollingLoop() {
        String lastId = "$";
        StatefulRedisConnection<String, String> conn = null;
        try {
            conn = connectionManager.createDedicatedConnection();
            var sync = conn.sync();
            XReadArgs xreadArgs = XReadArgs.Builder.block(Duration.ofSeconds(2)).count(100);
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    List<StreamMessage<String, String>> messages = sync.xread(
                            xreadArgs,
                            StreamOffset.from(channelName, lastId)
                    );
                    if (messages != null && !messages.isEmpty()) {
                        for (StreamMessage<String, String> message : messages) {
                            lastId = message.getId();
                            handleStreamPayload(message.getBody());
                        }
                    }
                } catch (Exception e) {
                    if (!running) {
                        break;
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } catch (Exception e) {
            if (running) {
                log.warn("QueueEvents stream loop error on {}", channelName, e);
            }
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception ignored) {}
            }
        }
    }

    private void handleStreamPayload(Map<String, String> body) {
        if (body == null || body.isEmpty()) {
            return;
        }
        String event = body.get("event");
        String jobId = body.get("jobId");
        if (event != null) {
            // Direct O(1) resolution for waiting futures
            if (jobId != null && ("completed".equals(event) || "failed".equals(event))) {
                List<CompletableFuture<Object>> futures = pendingJobFutures.remove(jobId);
                if (futures != null && !futures.isEmpty()) {
                    if ("completed".equals(event)) {
                        Object retVal = body.get("returnvalue");
                        for (CompletableFuture<Object> f : futures) {
                            f.complete(retVal);
                        }
                    } else {
                        String reason = body.get("failedReason");
                        RuntimeException ex = new RuntimeException(reason != null ? reason : "Job " + jobId + " failed");
                        for (CompletableFuture<Object> f : futures) {
                            f.completeExceptionally(ex);
                        }
                    }
                }
            }

            Map<String, Object> payload = new HashMap<>(body);
            List<BiConsumer<String, Map<String, Object>>> listeners = eventListeners.get(event);
            if (listeners != null) {
                for (var listener : listeners) {
                    try {
                        listener.accept(jobId, payload);
                    } catch (Exception e) {
                        log.error("Error executing QueueEvents stream listener for event '{}'", event, e);
                    }
                }
            }
        }
    }

    /**
     * Registers a generic event listener for the specified event name.
     */
    public QueueEvents on(String eventName, BiConsumer<String, Map<String, Object>> listener) {
        eventListeners.computeIfAbsent(eventName, k -> new CopyOnWriteArrayList<>()).add(listener);
        return this;
    }

    /**
     * Unregisters an event listener.
     */
    public QueueEvents off(String eventName, BiConsumer<String, Map<String, Object>> listener) {
        List<BiConsumer<String, Map<String, Object>>> listeners = eventListeners.get(eventName);
        if (listeners != null) {
            listeners.remove(listener);
        }
        return this;
    }

    /**
     * Unregisters an event listener (alias for off).
     */
    public QueueEvents removeListener(String eventName, BiConsumer<String, Map<String, Object>> listener) {
        return off(eventName, listener);
    }

    /**
     * Waits until the specified job completes or fails, returning its result or throwing on failure.
     */
    public Object waitUntilFinished(String jobId, Duration timeout) throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<Object> future = waitUntilFinishedAsync(jobId);
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            future.cancel(true);
            throw e;
        }
    }

    /**
     * Asynchronously waits until the specified job completes or fails with a timeout.
     */
    public CompletableFuture<Object> waitUntilFinishedAsync(String jobId, Duration timeout) {
        CompletableFuture<Object> future = waitUntilFinishedAsync(jobId);
        if (timeout != null && !timeout.isNegative() && !timeout.isZero()) {
            future.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }
        return future;
    }

    /**
     * Asynchronously waits until the specified job completes or fails.
     */
    public CompletableFuture<Object> waitUntilFinishedAsync(String jobId) {
        if (!running) {
            start();
        }
        CompletableFuture<Object> future = new CompletableFuture<>();
        pendingJobFutures.computeIfAbsent(jobId, k -> new CopyOnWriteArrayList<>()).add(future);

        future.whenComplete((res, err) -> {
            List<CompletableFuture<Object>> list = pendingJobFutures.get(jobId);
            if (list != null) {
                list.remove(future);
                if (list.isEmpty()) {
                    pendingJobFutures.remove(jobId);
                }
            }
        });

        return future;
    }

    /**
     * Triggers when a job transitions to WAITING state.
     */
    public QueueEvents onWaiting(Consumer<String> jobIdConsumer) {
        return on("waiting", (jobId, payload) -> jobIdConsumer.accept(jobId));
    }

    /**
     * Triggers when a job successfully completes.
     */
    public QueueEvents onCompleted(BiConsumer<String, Object> consumer) {
        return on("completed", (jobId, payload) -> consumer.accept(jobId, payload.get("returnvalue")));
    }

    /**
     * Triggers when a job fails permanently.
     */
    public QueueEvents onFailed(BiConsumer<String, String> consumer) {
        return on("failed", (jobId, payload) -> consumer.accept(jobId, (String) payload.get("failedReason")));
    }

    /**
     * Triggers when a job emits a real-time progress update.
     */
    public QueueEvents onProgress(BiConsumer<String, Object> consumer) {
        return on("progress", (jobId, payload) -> consumer.accept(jobId, payload.get("data")));
    }

    /**
     * Triggers when a failed job is scheduled for retry.
     */
    public QueueEvents onRetried(Consumer<String> jobIdConsumer) {
        return on("retried", (jobId, payload) -> jobIdConsumer.accept(jobId));
    }

    /**
     * Triggers when the queue is paused.
     */
    public QueueEvents onPaused(Runnable action) {
        return on("paused", (jobId, payload) -> action.run());
    }

    /**
     * Triggers when the queue is resumed.
     */
    public QueueEvents onResumed(Runnable action) {
        return on("resumed", (jobId, payload) -> action.run());
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public synchronized void close() {
        if (running) {
            running = false;
            if (streamThread != null) {
                streamThread.interrupt();
            }
            if (ownsConnectionManager && connectionManager != null) {
                connectionManager.close();
            }
            eventListeners.clear();
            log.info("QueueEvents stopped for queue: {}", queueName);
        }
    }

    public static class Builder {
        private String prefix;
        private String queueName;
        private RedisConnectionManager connectionManager;
        private RedisClient redisClient;
        private String redisUri;
        private JobSerializer serializer;

        public Builder prefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Builder redisUri(String redisUri) {
            this.redisUri = redisUri;
            return this;
        }

        public Builder redisClient(RedisClient redisClient) {
            this.redisClient = redisClient;
            return this;
        }

        public Builder connectionManager(RedisConnectionManager connectionManager) {
            this.connectionManager = connectionManager;
            return this;
        }

        public Builder serializer(JobSerializer serializer) {
            this.serializer = serializer;
            return this;
        }

        public QueueEvents build() {
            boolean owns = false;
            if (connectionManager == null) {
                owns = true;
                if (redisClient != null) {
                    connectionManager = new RedisConnectionManager(redisClient);
                } else if (redisUri != null) {
                    connectionManager = new RedisConnectionManager(redisUri);
                } else {
                    connectionManager = new RedisConnectionManager("redis://localhost:6379");
                }
            }
            return new QueueEvents(prefix, queueName, connectionManager, serializer, owns);
        }
    }
}
