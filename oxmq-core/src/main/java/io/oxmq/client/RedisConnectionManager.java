package io.oxmq.client;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages Lettuce Redis client instances, connections, and lifecycle.
 */
public class RedisConnectionManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RedisConnectionManager.class);

    private final RedisClient redisClient;
    private final boolean ownsClient;
    private final ReentrantLock connectionLock = new ReentrantLock();
    private volatile StatefulRedisConnection<String, String> commandConnection;
    private volatile StatefulRedisConnection<String, byte[]> binaryConnection;
    private volatile StatefulRedisPubSubConnection<String, String> pubSubConnection;

    private static final io.lettuce.core.codec.RedisCodec<String, byte[]> BINARY_CODEC =
            io.lettuce.core.codec.RedisCodec.of(io.lettuce.core.codec.StringCodec.UTF8, io.lettuce.core.codec.ByteArrayCodec.INSTANCE);

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
            connectionLock.lock();
            try {
                if (commandConnection == null || !commandConnection.isOpen()) {
                    commandConnection = redisClient.connect();
                }
            } finally {
                connectionLock.unlock();
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
     * Gets or creates a reusable binary command connection (String keys, byte[] values) for BullMQ Lua scripts.
     */
    public StatefulRedisConnection<String, byte[]> getBinaryConnection() {
        if (binaryConnection == null || !binaryConnection.isOpen()) {
            connectionLock.lock();
            try {
                if (binaryConnection == null || !binaryConnection.isOpen()) {
                    binaryConnection = redisClient.connect(BINARY_CODEC);
                }
            } finally {
                connectionLock.unlock();
            }
        }
        return binaryConnection;
    }

    /**
     * Creates a new dedicated stateful binary command connection.
     */
    public StatefulRedisConnection<String, byte[]> createDedicatedBinaryConnection() {
        return redisClient.connect(BINARY_CODEC);
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
            connectionLock.lock();
            try {
                if (pubSubConnection == null || !pubSubConnection.isOpen()) {
                    pubSubConnection = redisClient.connectPubSub();
                }
            } finally {
                connectionLock.unlock();
            }
        }
        return pubSubConnection;
    }

    public RedisClient getRedisClient() {
        return redisClient;
    }

    public RedisClient getClient() {
        return redisClient;
    }

    public StatefulRedisPubSubConnection<String, String> createDedicatedPubSubConnection() {
        return redisClient.connectPubSub();
    }

    @Override
    public void close() {
        connectionLock.lock();
        try {
            if (commandConnection != null && commandConnection.isOpen()) {
                commandConnection.close();
            }
        } catch (Exception e) {
            log.warn("Error closing command connection", e);
        } finally {
            try {
                if (binaryConnection != null && binaryConnection.isOpen()) {
                    binaryConnection.close();
                }
            } catch (Exception e) {
                log.warn("Error closing binary connection", e);
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
            connectionLock.unlock();
        }
    }
}
