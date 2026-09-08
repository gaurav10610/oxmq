package io.oxmq.client;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.io.Closeable;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages Lettuce Redis client instances, connections, and lifecycle.
 */
public class RedisConnectionManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RedisConnectionManager.class);

    private final RedisClient redisClient;
    private final boolean ownsClient;
    private volatile StatefulRedisConnection<String, String> commandConnection;
    private volatile StatefulRedisPubSubConnection<String, String> pubSubConnection;

    public RedisConnectionManager(String redisUri) {
        this(RedisClient.create(RedisURI.create(Objects.requireNonNull(redisUri, "redisUri must not be null"))), true);
    }

    public RedisConnectionManager(RedisURI redisUri) {
        this(RedisClient.create(Objects.requireNonNull(redisUri, "redisUri must not be null")), true);
    }

    public RedisConnectionManager(RedisClient redisClient) {
        this(redisClient, false);
    }

    public RedisConnectionManager(RedisClient redisClient, boolean ownsClient) {
        this.redisClient = Objects.requireNonNull(redisClient, "redisClient must not be null");
        this.ownsClient = ownsClient;
    }

    /**
     * Gets or creates a reusable stateful command connection.
     */
    public StatefulRedisConnection<String, String> getCommandConnection() {
        if (commandConnection == null || !commandConnection.isOpen()) {
            synchronized (this) {
                if (commandConnection == null || !commandConnection.isOpen()) {
                    commandConnection = redisClient.connect();
                }
            }
        }
        return commandConnection;
    }

    /**
     * Creates a new dedicated stateful command connection (useful for blocking operations or independent threads).
     */
    public StatefulRedisConnection<String, String> createDedicatedConnection() {
        return redisClient.connect();
    }

    /**
     * Convenience method to get synchronous Redis commands.
     */
    public io.lettuce.core.api.sync.RedisCommands<String, String> sync() {
        return getCommandConnection().sync();
    }

    /**
     * Convenience method to get asynchronous Redis commands.
     */
    public io.lettuce.core.api.async.RedisAsyncCommands<String, String> async() {
        return getCommandConnection().async();
    }

    /**
     * Gets or creates a reusable Pub/Sub connection.
     */
    public StatefulRedisPubSubConnection<String, String> getPubSubConnection() {
        if (pubSubConnection == null || !pubSubConnection.isOpen()) {
            synchronized (this) {
                if (pubSubConnection == null || !pubSubConnection.isOpen()) {
                    pubSubConnection = redisClient.connectPubSub();
                }
            }
        }
        return pubSubConnection;
    }

    public RedisClient getRedisClient() {
        return redisClient;
    }

    @Override
    public void close() {
        try {
            if (commandConnection != null && commandConnection.isOpen()) {
                commandConnection.close();
            }
        } catch (Exception e) {
            log.warn("Error closing command connection", e);
        }
        try {
            if (pubSubConnection != null && pubSubConnection.isOpen()) {
                pubSubConnection.close();
            }
        } catch (Exception e) {
            log.warn("Error closing PubSub connection", e);
        }
        if (ownsClient) {
            try {
                redisClient.shutdown();
            } catch (Exception e) {
                log.warn("Error shutting down RedisClient", e);
            }
        }
    }
}
