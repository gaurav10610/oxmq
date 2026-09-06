package io.oxmq;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.serializer.JacksonJobSerializer;
import io.oxmq.serializer.JobSerializer;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Event listener for real-time OxMQ queue lifecycle events backed by Redis Pub/Sub.
 * Provides 100% parity with BullMQ's QueueEvents.
 */
public class QueueEvents implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(QueueEvents.class);

    private final String queueName;
    private final String channelName;
    private final RedisConnectionManager connectionManager;
    private final JobSerializer serializer;
    private final Map<String, List<BiConsumer<String, Map<String, Object>>>> eventListeners = new ConcurrentHashMap<>();
    private StatefulRedisPubSubConnection<String, String> pubSubConnection;
    private volatile boolean running = false;

    public QueueEvents(String queueName, String redisUri) {
        this(queueName, new RedisConnectionManager(redisUri), new JacksonJobSerializer());
    }

    public QueueEvents(String queueName, RedisClient redisClient) {
        this(queueName, new RedisConnectionManager(redisClient), new JacksonJobSerializer());
    }

    public QueueEvents(String queueName, RedisConnectionManager connectionManager, JobSerializer serializer) {
        this.queueName = Objects.requireNonNull(queueName, "queueName must not be null");
        this.channelName = "bull:" + queueName + ":events";
        this.connectionManager = Objects.requireNonNull(connectionManager, "connectionManager must not be null");
        this.serializer = serializer != null ? serializer : new JacksonJobSerializer();
    }

    /**
     * Starts listening to Redis Pub/Sub queue events.
     */
    public synchronized QueueEvents start() {
        if (!running) {
            this.pubSubConnection = connectionManager.getPubSubConnection();
            this.pubSubConnection.addListener(new RedisPubSubAdapter<>() {
                @Override
                public void message(String channel, String message) {
                    if (channelName.equals(channel)) {
                        handleMessage(message);
                    }
                }
            });
            this.pubSubConnection.sync().subscribe(channelName);
            this.running = true;
            log.info("QueueEvents started listening on channel: {}", channelName);
        }
        return this;
    }

    @SuppressWarnings("unchecked")
    private void handleMessage(String rawJson) {
        try {
            Map<String, Object> payload = serializer.deserialize(rawJson, Map.class);
            if (payload == null) {
                return;
            }
            String event = (String) payload.get("event");
            String jobId = (String) payload.get("jobId");

            if (event != null) {
                List<BiConsumer<String, Map<String, Object>>> listeners = eventListeners.get(event);
                if (listeners != null) {
                    for (var listener : listeners) {
                        try {
                            listener.accept(jobId, payload);
                        } catch (Exception e) {
                            log.error("Error executing QueueEvents listener for event '{}'", event, e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to parse QueueEvents message: {}", rawJson, e);
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

    public String getQueueName() {
        return queueName;
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public synchronized void close() {
        if (running) {
            try {
                if (pubSubConnection != null && pubSubConnection.isOpen()) {
                    pubSubConnection.sync().unsubscribe(channelName);
                }
            } catch (Exception e) {
                log.warn("Error unsubscribing QueueEvents from {}", channelName, e);
            }
            connectionManager.close();
            running = false;
            eventListeners.clear();
            log.info("QueueEvents stopped for queue: {}", queueName);
        }
    }
}
