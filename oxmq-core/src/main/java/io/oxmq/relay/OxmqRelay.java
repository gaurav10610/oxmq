package io.oxmq.relay;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.serializer.JacksonJobSerializer;
import io.oxmq.serializer.JobSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Distributed pub/sub control channel, inter-node event router, and messaging engine.
 * Supports hierarchical topic wildcards (* and >), message retention, and heartbeat node leases.
 */
public class OxmqRelay implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OxmqRelay.class);

    private final String nodeId;
    private final String prefix;
    private final RedisConnectionManager connectionManager;
    private final boolean ownsConnectionManager;
    private final JobSerializer serializer;
    private final String eventsChannel;
    private final String nodesKeyPrefix;
    private final String retainedKeyPrefix;
    private final String retainedTopicsSetKey;

    private final StatefulRedisPubSubConnection<String, String> pubSubConn;
    private final RedisPubSubListener<String, String> pubSubListener;
    private final List<RegisteredSubscription> subscriptions = new CopyOnWriteArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ScheduledFuture<?> heartbeatTask;

    private static class RegisteredSubscription implements RelaySubscription {
        private final String pattern;
        private final String[] patternSegments;
        private final boolean isCatchAll;
        private final boolean isExact;
        private final Consumer<RelayMessage> handler;
        private final AtomicBoolean active = new AtomicBoolean(true);
        private final Set<String> seenMessageIds = Collections.synchronizedSet(
            Collections.newSetFromMap(new LinkedHashMap<String, Boolean>(128, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > 2000;
                }
            })
        );

        RegisteredSubscription(String pattern, Consumer<RelayMessage> handler) {
            this.pattern = pattern;
            this.handler = handler;
            this.isCatchAll = "*".equals(pattern) || ">".equals(pattern);
            this.isExact = !isCatchAll && !pattern.contains("*") && !pattern.contains(">");
            this.patternSegments = (isExact || isCatchAll) ? null : pattern.split("\\.");
        }

        @Override
        public String getPattern() {
            return pattern;
        }

        @Override
        public void unsubscribe() {
            active.set(false);
        }

        @Override
        public boolean isActive() {
            return active.get();
        }

        public boolean matches(String topic) {
            if (topic == null) return false;
            if (isCatchAll || pattern.equals(topic)) return true;
            if (isExact) return false;
            String[] tSegments = topic.split("\\.");
            return matchesSegments(patternSegments, tSegments);
        }

        public boolean tryDeliver(RelayMessage msg) {
            if (!active.get()) return false;
            String mid = msg.getMessageId();
            if (mid != null && !seenMessageIds.add(mid)) {
                return false; // Already delivered this message
            }
            try {
                handler.accept(msg);
                return true;
            } catch (Throwable t) {
                log.warn("Error in relay subscription handler for topic {}", msg.getTopic(), t);
                return false;
            }
        }
    }

    private OxmqRelay(Builder builder) {
        this.nodeId = builder.nodeId != null ? builder.nodeId : "node:" + UUID.randomUUID();
        this.prefix = builder.prefix != null ? builder.prefix : "bull:relay";
        this.serializer = builder.serializer != null ? builder.serializer : new JacksonJobSerializer();
        this.ownsConnectionManager = builder.ownsConnectionManager;
        this.connectionManager = Objects.requireNonNull(builder.connectionManager, "connectionManager must not be null");

        this.eventsChannel = prefix + ":events";
        this.nodesKeyPrefix = prefix + ":nodes:";
        this.retainedKeyPrefix = prefix + ":retained:";
        this.retainedTopicsSetKey = prefix + ":retained_topics";

        this.pubSubConn = connectionManager.getPubSubConnection();
        this.pubSubListener = new RedisPubSubAdapter<>() {
            @Override
            public void message(String channel, String message) {
                if (eventsChannel.equals(channel) && message != null) {
                    dispatchIncomingMessage(message);
                }
            }
        };
        this.pubSubConn.addListener(this.pubSubListener);
        this.pubSubConn.sync().subscribe(eventsChannel);

        // Synchronously register initial lease so node is immediately visible
        renewLease();

        this.heartbeatTask = io.oxmq.watchdog.WatchdogScheduler.sharedScheduler()
                .scheduleWithFixedDelay(this::renewLease, 5, 5, TimeUnit.SECONDS);

        log.info("Started OxMQ Relay [nodeId: {}] on channel {}", nodeId, eventsChannel);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Publishes a message to a topic.
     *
     * @param topic topic name (e.g. "orders.42.status")
     * @param payload payload object
     */
    public void publish(String topic, Object payload) {
        publish(topic, payload, null);
    }

    /**
     * Publishes a message to a topic with a retention duration.
     * Retained messages are cached in Redis and immediately delivered to new subscribers.
     *
     * @param topic topic name
     * @param payload payload object
     * @param retain retention duration, or null for non-retained
     */
    public void publish(String topic, Object payload, Duration retain) {
        Objects.requireNonNull(topic, "topic must not be null");
        if (!running.get()) {
            throw new IllegalStateException("OxmqRelay is closed");
        }

        long now = System.currentTimeMillis();
        String payloadJson = serializer.serialize(payload != null ? payload : Map.of());
        boolean isRetained = retain != null && !retain.isZero() && !retain.isNegative();

        RelayMessage message = new RelayMessage(topic, payloadJson, now, nodeId, isRetained);
        String messageJson = serializer.serialize(message);

        RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
        if (isRetained) {
            String retainKey = retainedKeyPrefix + topic;
            sync.set(retainKey, messageJson);
            sync.pexpire(retainKey, retain.toMillis());
            sync.sadd(retainedTopicsSetKey, topic);
        }

        sync.publish(eventsChannel, messageJson);
        log.debug("Published relay message to topic {} (retained: {})", topic, isRetained);
    }

    /**
     * Subscribes to a topic pattern.
     * Supports exact topics ("orders.status"), single-segment wildcard ("orders.*.status"),
     * and multi-segment trailing wildcard ("orders.>").
     *
     * @param topicPattern topic pattern
     * @param handler message consumer
     * @return subscription handle
     */
    public RelaySubscription subscribe(String topicPattern, Consumer<RelayMessage> handler) {
        Objects.requireNonNull(topicPattern, "topicPattern must not be null");
        Objects.requireNonNull(handler, "handler must not be null");
        if (!running.get()) {
            throw new IllegalStateException("OxmqRelay is closed");
        }

        RegisteredSubscription sub = new RegisteredSubscription(topicPattern, handler);
        subscriptions.add(sub);

        // Immediately dispatch matching retained messages
        dispatchRetainedMessages(sub);

        return sub;
    }

    private void dispatchRetainedMessages(RegisteredSubscription sub) {
        try {
            RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
            Set<String> retainedTopics = sync.smembers(retainedTopicsSetKey);
            if (retainedTopics == null || retainedTopics.isEmpty()) {
                return;
            }

            for (String topic : retainedTopics) {
                if (!sub.isActive()) break;
                if (sub.matches(topic)) {
                    String key = retainedKeyPrefix + topic;
                    String json = sync.get(key);
                    if (json != null && !json.isBlank()) {
                        try {
                            RelayMessage msg = serializer.deserialize(json, RelayMessage.class);
                            sub.tryDeliver(msg);
                        } catch (Exception ex) {
                            log.warn("Failed to dispatch retained relay message for topic {}", topic, ex);
                        }
                    } else {
                        sync.srem(retainedTopicsSetKey, topic);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to fetch retained messages for pattern {}", sub.getPattern(), e);
        }
    }

    private void dispatchIncomingMessage(String rawJson) {
        try {
            RelayMessage msg = serializer.deserialize(rawJson, RelayMessage.class);
            if (msg == null || msg.getTopic() == null) {
                return;
            }

            for (RegisteredSubscription sub : subscriptions) {
                if (sub.isActive() && sub.matches(msg.getTopic())) {
                    sub.tryDeliver(msg);
                }
            }
        } catch (Exception e) {
            log.debug("Failed to deserialize relay message: {}", e.getMessage());
        }
    }

    /**
     * Evaluates whether a topic matches a wildcard topic pattern.
     * <ul>
     *   <li>{@code *} matches exactly one dot-separated segment.</li>
     *   <li>{@code >} matches one or more trailing dot-separated segments.</li>
     *   <li>Exact matching without wildcards.</li>
     * </ul>
     */
    public static boolean matchesPattern(String pattern, String topic) {
        if (pattern == null || topic == null) return false;
        if (pattern.equals(topic) || "*".equals(pattern) || ">".equals(pattern)) return true;

        String[] pSegments = pattern.split("\\.");
        String[] tSegments = topic.split("\\.");
        return matchesSegments(pSegments, tSegments);
    }

    private static boolean matchesSegments(String[] pSegments, String[] tSegments) {
        int pi = 0, ti = 0;
        while (pi < pSegments.length && ti < tSegments.length) {
            String p = pSegments[pi];
            if (">".equals(p)) {
                return true;
            }
            if (!"*".equals(p) && !p.equals(tSegments[ti])) {
                return false;
            }
            pi++;
            ti++;
        }

        if (pi < pSegments.length && ">".equals(pSegments[pi])) {
            return true;
        }

        return pi == pSegments.length && ti == tSegments.length;
    }

    private void renewLease() {
        if (!running.get()) return;
        try {
            RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
            String key = nodesKeyPrefix + nodeId;
            sync.set(key, String.valueOf(System.currentTimeMillis()));
            sync.pexpire(key, 15000);
        } catch (Exception e) {
            log.debug("Failed to renew relay node lease for {}: {}", nodeId, e.getMessage());
        }
    }

    /**
     * Returns the active cluster node IDs currently registered with heartbeats.
     */
    public List<String> getActiveNodes() {
        try {
            RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
            List<String> keys = sync.keys(nodesKeyPrefix + "*");
            if (keys == null || keys.isEmpty()) return List.of();
            List<String> nodes = new ArrayList<>(keys.size());
            for (String key : keys) {
                nodes.add(key.substring(nodesKeyPrefix.length()));
            }
            return nodes;
        } catch (Exception e) {
            return List.of();
        }
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            log.info("Shutting down OxMQ Relay [nodeId: {}]", nodeId);
            if (heartbeatTask != null) {
                heartbeatTask.cancel(true);
            }

            for (RegisteredSubscription sub : subscriptions) {
                sub.unsubscribe();
            }
            subscriptions.clear();

            try {
                RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();
                sync.del(nodesKeyPrefix + nodeId);
            } catch (Exception ignored) {}

            if (pubSubConn != null) {
                try {
                    pubSubConn.sync().unsubscribe(eventsChannel);
                    if (pubSubListener != null) {
                        pubSubConn.removeListener(pubSubListener);
                    }
                } catch (Exception ignored) {}
            }

            if (ownsConnectionManager && connectionManager != null) {
                connectionManager.close();
            }
        }
    }

    public static class Builder {
        private String nodeId;
        private String prefix;
        private RedisConnectionManager connectionManager;
        private RedisClient redisClient;
        private String redisUri;
        private boolean ownsConnectionManager = false;
        private JobSerializer serializer;

        public Builder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder prefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder connectionManager(RedisConnectionManager connectionManager) {
            this.connectionManager = connectionManager;
            return this;
        }

        public Builder redisClient(RedisClient redisClient) {
            this.redisClient = redisClient;
            return this;
        }

        public Builder redisUri(String redisUri) {
            this.redisUri = redisUri;
            return this;
        }

        public Builder serializer(JobSerializer serializer) {
            this.serializer = serializer;
            return this;
        }

        public OxmqRelay build() {
            if (connectionManager == null) {
                ownsConnectionManager = true;
                if (redisClient != null) {
                    connectionManager = new RedisConnectionManager(redisClient);
                } else if (redisUri != null) {
                    connectionManager = new RedisConnectionManager(redisUri);
                } else {
                    connectionManager = new RedisConnectionManager("redis://localhost:6379");
                }
            }
            return new OxmqRelay(this);
        }
    }
}
