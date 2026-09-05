package io.oxmq;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.lua.LuaScript;
import io.oxmq.lua.LuaScriptManager;
import io.oxmq.model.FlowJob;
import io.oxmq.model.JobOptions;
import io.oxmq.serializer.JacksonJobSerializer;
import io.oxmq.serializer.JobSerializer;
import java.io.Closeable;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Workflow producer for parent-child DAG task trees.
 */
public class FlowProducer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(FlowProducer.class);

    private final RedisConnectionManager connectionManager;
    private final LuaScriptManager scriptManager;
    private final JobSerializer serializer;

    public FlowProducer(String redisUri) {
        this(new RedisConnectionManager(redisUri), new LuaScriptManager(), new JacksonJobSerializer());
    }

    public FlowProducer(RedisClient redisClient) {
        this(new RedisConnectionManager(redisClient), new LuaScriptManager(), new JacksonJobSerializer());
    }

    public FlowProducer(RedisConnectionManager connectionManager, LuaScriptManager scriptManager, JobSerializer serializer) {
        this.connectionManager = Objects.requireNonNull(connectionManager, "connectionManager must not be null");
        this.scriptManager = scriptManager != null ? scriptManager : new LuaScriptManager();
        this.serializer = serializer != null ? serializer : new JacksonJobSerializer();
    }

    /**
     * Atomically adds a hierarchical flow tree (parent and all children) into Redis.
     *
     * @param root FlowJob tree root
     * @return generated parent Job ID
     */
    public String add(FlowJob<?> root) {
        Objects.requireNonNull(root, "Root flow job must not be null");
        StatefulRedisConnection<String, String> conn = connectionManager.getCommandConnection();
        return addNode(root, null, conn);
    }

    private String addNode(FlowJob<?> node, String parentKey, StatefulRedisConnection<String, String> conn) {
        String queueName = node.getQueueName();
        String prefix = "bull:" + queueName;
        RedisCommands<String, String> sync = conn.sync();

        String jobId = String.valueOf(sync.incr(prefix + ":id"));
        String nodeKey = prefix + ":" + jobId;
        long now = System.currentTimeMillis();

        String serializedData = serializer.serialize(node.getData());
        String serializedOpts = serializer.serialize(node.getOpts());

        if (node.getChildren().isEmpty()) {
            // Leaf node: schedule directly into wait or delayed
            String[] keys = new String[]{
                    prefix + ":wait",
                    prefix + ":delayed",
                    prefix + ":id",
                    prefix + ":events",
                    prefix + ":meta"
            };

            scriptManager.eval(conn, LuaScript.ADD_JOB, ScriptOutputType.VALUE, keys,
                    prefix,
                    jobId,
                    node.getName(),
                    serializedData != null ? serializedData : "{}",
                    serializedOpts != null ? serializedOpts : "{}",
                    String.valueOf(now),
                    String.valueOf(node.getOpts() != null ? node.getOpts().getDelayMs() : 0),
                    parentKey != null ? parentKey : ""
            );
            log.debug("Enqueued flow leaf job {} [id: {}] with parent: {}", node.getName(), jobId, parentKey);

        } else {
            // Parent node: create job record in WAITING_CHILDREN state
            int childCount = node.getChildren().size();

            sync.hmset(nodeKey, java.util.Map.of(
                    "id", jobId,
                    "name", node.getName(),
                    "data", serializedData != null ? serializedData : "{}",
                    "opts", serializedOpts != null ? serializedOpts : "{}",
                    "timestamp", String.valueOf(now),
                    "unresolvedChildrenCount", String.valueOf(childCount),
                    "queueWaitKey", prefix + ":wait",
                    "state", "WAITING_CHILDREN"
            ));

            if (parentKey != null) {
                sync.hset(nodeKey, "parentKey", parentKey);
            }

            log.debug("Created flow parent job {} [id: {}] waiting for {} children", node.getName(), jobId, childCount);

            // Recursively add all children
            for (FlowJob<?> child : node.getChildren()) {
                addNode(child, nodeKey, conn);
            }
        }

        return jobId;
    }

    @Override
    public void close() {
        connectionManager.close();
    }
}
