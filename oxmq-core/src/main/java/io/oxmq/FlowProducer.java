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
    private final String defaultPrefix;
    private final String clusterTag;

    public static FlowProducer forCluster(String clusterTag, String redisUri) {
        return new FlowProducer(new RedisConnectionManager(redisUri), null, clusterTag);
    }

    public static FlowProducer forCluster(String clusterTag, RedisClient redisClient) {
        return new FlowProducer(new RedisConnectionManager(redisClient), null, clusterTag);
    }

    public static FlowProducer forCluster(String clusterTag, RedisConnectionManager connectionManager) {
        return new FlowProducer(connectionManager, null, clusterTag);
    }

    public FlowProducer(String redisUri) {
        this(new RedisConnectionManager(redisUri), new LuaScriptManager(), new JacksonJobSerializer(), null, null);
    }

    public FlowProducer(String redisUri, String defaultPrefix) {
        this(new RedisConnectionManager(redisUri), new LuaScriptManager(), new JacksonJobSerializer(), defaultPrefix, null);
    }

    public FlowProducer(RedisClient redisClient) {
        this(new RedisConnectionManager(redisClient), new LuaScriptManager(), new JacksonJobSerializer(), null, null);
    }

    public FlowProducer(RedisClient redisClient, String defaultPrefix) {
        this(new RedisConnectionManager(redisClient), new LuaScriptManager(), new JacksonJobSerializer(), defaultPrefix, null);
    }

    public FlowProducer(RedisConnectionManager connectionManager) {
        this(connectionManager, new LuaScriptManager(), new JacksonJobSerializer(), null, null);
    }

    public FlowProducer(RedisConnectionManager connectionManager, String defaultPrefix) {
        this(connectionManager, new LuaScriptManager(), new JacksonJobSerializer(), defaultPrefix, null);
    }

    public FlowProducer(RedisConnectionManager connectionManager, String defaultPrefix, String clusterTag) {
        this(connectionManager, new LuaScriptManager(), new JacksonJobSerializer(), defaultPrefix, clusterTag);
    }

    public FlowProducer(RedisConnectionManager connectionManager, LuaScriptManager scriptManager, JobSerializer serializer) {
        this(connectionManager, scriptManager, serializer, null, null);
    }

    public FlowProducer(RedisConnectionManager connectionManager, LuaScriptManager scriptManager, JobSerializer serializer, String defaultPrefix) {
        this(connectionManager, scriptManager, serializer, defaultPrefix, null);
    }

    public FlowProducer(RedisConnectionManager connectionManager, LuaScriptManager scriptManager, JobSerializer serializer, String defaultPrefix, String clusterTag) {
        this.connectionManager = Objects.requireNonNull(connectionManager, "connectionManager must not be null");
        this.scriptManager = scriptManager != null ? scriptManager : new LuaScriptManager();
        this.bullScripts = new BullScripts(this.scriptManager);
        this.serializer = serializer != null ? serializer : new JacksonJobSerializer();
        this.defaultPrefix = defaultPrefix;
        this.clusterTag = clusterTag;
    }

    public String getDefaultPrefix() {
        return defaultPrefix;
    }

    public String getClusterTag() {
        return clusterTag;
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

    /**
     * Atomically/sequentially adds multiple workflow DAG trees into Redis.
     *
     * @param roots List of root FlowJob trees
     * @return List of generated root Job IDs
     */
    public java.util.List<String> addBulk(java.util.List<FlowJob<?>> roots) {
        if (roots == null || roots.isEmpty()) {
            return java.util.List.of();
        }
        java.util.List<String> parentIds = new java.util.ArrayList<>(roots.size());
        for (FlowJob<?> root : roots) {
            parentIds.add(add(root));
        }
        return parentIds;
    }

    private String addNode(FlowJob<?> node, String parentKey, Map<String, Object> parentObj) {
        String queueName = node.getQueueName();
        String nodePrefix = node.getPrefix() != null && !node.getPrefix().isBlank() ? node.getPrefix() : defaultPrefix;
        QueueKeys queueKeys = (clusterTag != null && !clusterTag.isBlank())
                ? QueueKeys.forClusterFlow(clusterTag, nodePrefix, queueName)
                : new QueueKeys(nodePrefix, queueName);
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
            if (node.getOpts() != null && node.getOpts().getDeduplicationId() != null && !node.getOpts().getDeduplicationId().isBlank()) {
                throw new IllegalArgumentException(
                    "Deduplication options cannot be set on parent DAG nodes (node '" + node.getName() + "'); set on leaf nodes only.");
            }
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
