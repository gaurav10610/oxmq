package io.oxmq;

import io.lettuce.core.RedisClient;
import io.oxmq.client.QueueKeys;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.lua.BullScripts;
import io.oxmq.lua.LuaScriptManager;
import io.oxmq.model.FlowJob;
import io.oxmq.serializer.JacksonJobSerializer;
import io.oxmq.serializer.JobSerializer;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Workflow producer for parent-child DAG task trees using BullMQ Lua scripts.
 */
public class FlowProducer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(FlowProducer.class);

    private final RedisConnectionManager connectionManager;
    private final LuaScriptManager scriptManager;
    private final BullScripts bullScripts;
    private final JobSerializer serializer;

    public FlowProducer(String redisUri) {
        this(new RedisConnectionManager(redisUri), new LuaScriptManager(), new JacksonJobSerializer());
    }

    public FlowProducer(RedisClient redisClient) {
        this(new RedisConnectionManager(redisClient), new LuaScriptManager(), new JacksonJobSerializer());
    }

    public FlowProducer(RedisConnectionManager connectionManager) {
        this(connectionManager, new LuaScriptManager(), new JacksonJobSerializer());
    }

    public FlowProducer(RedisConnectionManager connectionManager, LuaScriptManager scriptManager, JobSerializer serializer) {
        this.connectionManager = Objects.requireNonNull(connectionManager, "connectionManager must not be null");
        this.scriptManager = scriptManager != null ? scriptManager : new LuaScriptManager();
        this.bullScripts = new BullScripts(this.scriptManager);
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
        return addNode(root, null, null);
    }

    private String addNode(FlowJob<?> node, String parentKey, Map<String, Object> parentObj) {
        String queueName = node.getQueueName();
        QueueKeys queueKeys = new QueueKeys("bull", queueName);
        long now = System.currentTimeMillis();

        String customJobId = node.getOpts() != null ? node.getOpts().getJobId() : null;
        String serializedData = serializer.serialize(node.getData());

        Map<String, Object> optsMap = node.getOpts() != null ? node.getOpts().toMap() : new HashMap<>();
        if (parentKey != null) {
            optsMap.put("parentKey", parentKey);
        }
        if (parentObj != null) {
            optsMap.put("parent", parentObj);
        }

        if (node.getChildren().isEmpty()) {
            // Leaf node: schedule directly into wait queue
            String jobId = bullScripts.addStandardJob(
                connectionManager.getBinaryConnection(),
                queueKeys,
                customJobId,
                node.getName(),
                serializedData,
                optsMap,
                now
            );
            log.debug("Enqueued flow leaf job {} [id: {}] with parent: {}", node.getName(), jobId, parentKey);
            return jobId;

        } else {
            // Parent node: schedule into waiting-children state
            String parentId = bullScripts.addParentJob(
                connectionManager.getBinaryConnection(),
                queueKeys,
                customJobId,
                node.getName(),
                serializedData,
                optsMap,
                now
            );

            log.debug("Created flow parent job {} [id: {}] waiting for {} children",
                    node.getName(), parentId, node.getChildren().size());

            String nodeParentKey = queueKeys.toJobKey(parentId);
            Map<String, Object> childParentObj = Map.of(
                "id", parentId,
                "queueKey", queueKeys.getQualifiedName()
            );

            // Recursively add all children
            for (FlowJob<?> child : node.getChildren()) {
                addNode(child, nodeParentKey, childParentObj);
            }

            return parentId;
        }
    }

    @Override
    public void close() {
        connectionManager.close();
    }
}
