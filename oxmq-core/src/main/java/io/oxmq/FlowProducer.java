package io.oxmq;

import io.lettuce.core.RedisClient;
import io.oxmq.client.QueueKeys;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.lua.BullScripts;
import io.oxmq.lua.LuaScriptManager;
import io.oxmq.model.FlowJob;
import io.oxmq.model.JobOptions;
import io.oxmq.serializer.JacksonJobSerializer;
import io.oxmq.serializer.JobSerializer;
import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
    private final boolean ownsConnectionManager;

    public static FlowProducer forCluster(String clusterTag, String redisUri) {
        return new FlowProducer(new RedisConnectionManager(redisUri), new LuaScriptManager(), new JacksonJobSerializer(), null, clusterTag, true);
    }

    public static FlowProducer forCluster(String clusterTag, RedisClient redisClient) {
        return new FlowProducer(new RedisConnectionManager(redisClient), new LuaScriptManager(), new JacksonJobSerializer(), null, clusterTag, true);
    }

    public static FlowProducer forCluster(String clusterTag, RedisConnectionManager connectionManager) {
        return new FlowProducer(connectionManager, new LuaScriptManager(), new JacksonJobSerializer(), null, clusterTag, false);
    }

    public FlowProducer(String redisUri) {
        this(new RedisConnectionManager(redisUri), new LuaScriptManager(), new JacksonJobSerializer(), null, null, true);
    }

    public FlowProducer(String redisUri, String defaultPrefix) {
        this(new RedisConnectionManager(redisUri), new LuaScriptManager(), new JacksonJobSerializer(), defaultPrefix, null, true);
    }

    public FlowProducer(RedisClient redisClient) {
        this(new RedisConnectionManager(redisClient), new LuaScriptManager(), new JacksonJobSerializer(), null, null, true);
    }

    public FlowProducer(RedisClient redisClient, String defaultPrefix) {
        this(new RedisConnectionManager(redisClient), new LuaScriptManager(), new JacksonJobSerializer(), defaultPrefix, null, true);
    }

    public FlowProducer(RedisConnectionManager connectionManager) {
        this(connectionManager, new LuaScriptManager(), new JacksonJobSerializer(), null, null, false);
    }

    public FlowProducer(RedisConnectionManager connectionManager, String defaultPrefix) {
        this(connectionManager, new LuaScriptManager(), new JacksonJobSerializer(), defaultPrefix, null, false);
    }

    public FlowProducer(RedisConnectionManager connectionManager, String defaultPrefix, String clusterTag) {
        this(connectionManager, new LuaScriptManager(), new JacksonJobSerializer(), defaultPrefix, clusterTag, false);
    }

    public FlowProducer(RedisConnectionManager connectionManager, LuaScriptManager scriptManager, JobSerializer serializer) {
        this(connectionManager, scriptManager, serializer, null, null, false);
    }

    public FlowProducer(RedisConnectionManager connectionManager, LuaScriptManager scriptManager, JobSerializer serializer, String defaultPrefix) {
        this(connectionManager, scriptManager, serializer, defaultPrefix, null, false);
    }

    public FlowProducer(RedisConnectionManager connectionManager, LuaScriptManager scriptManager, JobSerializer serializer, String defaultPrefix, String clusterTag) {
        this(connectionManager, scriptManager, serializer, defaultPrefix, clusterTag, false);
    }

    public FlowProducer(RedisConnectionManager connectionManager, LuaScriptManager scriptManager, JobSerializer serializer, String defaultPrefix, String clusterTag, boolean ownsConnectionManager) {
        this.connectionManager = Objects.requireNonNull(connectionManager, "connectionManager must not be null");
        this.scriptManager = scriptManager != null ? scriptManager : new LuaScriptManager();
        this.bullScripts = new BullScripts(this.scriptManager);
        this.serializer = serializer != null ? serializer : new JacksonJobSerializer();
        this.defaultPrefix = defaultPrefix;
        this.clusterTag = clusterTag;
        this.ownsConnectionManager = ownsConnectionManager;
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
            Map<String, Object> effectiveParentObj = new HashMap<>(parentObj);
            if (node.getOpts() != null) {
                if (node.getOpts().isFailParentOnFailure()) effectiveParentObj.put("fpof", true);
                if (node.getOpts().isContinueParentOnFailure()) effectiveParentObj.put("cpof", true);
                if (node.getOpts().isIgnoreDependencyOnFailure()) effectiveParentObj.put("idof", true);
                if (node.getOpts().isRemoveDependencyOnFailure()) effectiveParentObj.put("rdof", true);
            }
            optsMap.put("parent", effectiveParentObj);
        }

        if (node.getChildren().isEmpty()) {
            // Leaf node: schedule directly into wait queue
            String jobId;
            JobOptions opts = node.getOpts();
            if (opts != null && opts.getDelayMs() > 0) {
                jobId = bullScripts.addDelayedJob(
                    connectionManager.getBinaryConnection(),
                    queueKeys,
                    customJobId,
                    node.getName(),
                    serializedData,
                    optsMap,
                    now,
                    opts.getDelayMs()
                );
            } else if (opts != null && opts.getPriority() > 0) {
                jobId = bullScripts.addPrioritizedJob(
                    connectionManager.getBinaryConnection(),
                    queueKeys,
                    customJobId,
                    node.getName(),
                    serializedData,
                    optsMap,
                    now,
                    opts.getPriority()
                );
            } else {
                jobId = bullScripts.addStandardJob(
                    connectionManager.getBinaryConnection(),
                    queueKeys,
                    customJobId,
                    node.getName(),
                    serializedData,
                    optsMap,
                    now
                );
            }
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
            Map<String, Object> childParentObj = new HashMap<>();
            childParentObj.put("id", parentId);
            childParentObj.put("queueKey", queueKeys.getQualifiedName());
            if (node.getOpts() != null) {
                if (node.getOpts().isFailParentOnFailure()) childParentObj.put("fpof", true);
                if (node.getOpts().isContinueParentOnFailure()) childParentObj.put("cpof", true);
                if (node.getOpts().isIgnoreDependencyOnFailure()) childParentObj.put("idof", true);
                if (node.getOpts().isRemoveDependencyOnFailure()) childParentObj.put("rdof", true);
            }

            // Recursively add all children (pipelining leaf tasks for high-speed DAG ingestion)
            java.util.List<FlowJob<?>> leafChildren = new java.util.ArrayList<>();
            for (FlowJob<?> child : node.getChildren()) {
                if (child.getChildren().isEmpty()) {
                    leafChildren.add(child);
                } else {
                    addNode(child, nodeParentKey, childParentObj);
                }
            }

            if (!leafChildren.isEmpty()) {
                addLeafChildrenPipelined(leafChildren, nodeParentKey, childParentObj);
            }

            return parentId;
        }
    }

    private void addLeafChildrenPipelined(java.util.List<FlowJob<?>> leaves, String parentKey, Map<String, Object> parentObj) {
        if (leaves.size() == 1) {
            addNode(leaves.get(0), parentKey, parentObj);
            return;
        }

        long now = System.currentTimeMillis();
        java.util.List<java.util.concurrent.CompletableFuture<String>> futures = new java.util.ArrayList<>(leaves.size());
        io.lettuce.core.api.StatefulRedisConnection<String, byte[]> conn = connectionManager.getBinaryConnection();
        Map<String, QueueKeys> localKeysCache = new HashMap<>();

        for (FlowJob<?> leaf : leaves) {
            String queueName = leaf.getQueueName();
            String nodePrefix = leaf.getPrefix() != null && !leaf.getPrefix().isBlank() ? leaf.getPrefix() : defaultPrefix;
            String cacheKey = (nodePrefix != null ? nodePrefix : "") + ":" + queueName;
            QueueKeys queueKeys = localKeysCache.computeIfAbsent(cacheKey, k ->
                    (clusterTag != null && !clusterTag.isBlank())
                            ? QueueKeys.forClusterFlow(clusterTag, nodePrefix, queueName)
                            : new QueueKeys(nodePrefix, queueName)
            );

            String customJobId = leaf.getOpts() != null ? leaf.getOpts().getJobId() : null;
            String serializedData = serializer.serialize(leaf.getData());

            Map<String, Object> optsMap = leaf.getOpts() != null ? leaf.getOpts().toMap() : new HashMap<>();
            if (parentKey != null) {
                optsMap.put("parentKey", parentKey);
            }
            if (parentObj != null) {
                Map<String, Object> effectiveParentObj = new HashMap<>(parentObj);
                if (leaf.getOpts() != null) {
                    if (leaf.getOpts().isFailParentOnFailure()) effectiveParentObj.put("fpof", true);
                    if (leaf.getOpts().isContinueParentOnFailure()) effectiveParentObj.put("cpof", true);
                    if (leaf.getOpts().isIgnoreDependencyOnFailure()) effectiveParentObj.put("idof", true);
                    if (leaf.getOpts().isRemoveDependencyOnFailure()) effectiveParentObj.put("rdof", true);
                }
                optsMap.put("parent", effectiveParentObj);
            }

            JobOptions opts = leaf.getOpts();
            if (opts != null && opts.getDelayMs() > 0) {
                futures.add(bullScripts.addDelayedJobAsync(conn, queueKeys, customJobId, leaf.getName(), serializedData, optsMap, now, opts.getDelayMs()));
            } else if (opts != null && opts.getPriority() > 0) {
                futures.add(bullScripts.addPrioritizedJobAsync(conn, queueKeys, customJobId, leaf.getName(), serializedData, optsMap, now, opts.getPriority()));
            } else {
                futures.add(bullScripts.addStandardJobAsync(conn, queueKeys, customJobId, leaf.getName(), serializedData, optsMap, now));
            }
        }

        try {
            java.util.concurrent.CompletableFuture.allOf(futures.toArray(new java.util.concurrent.CompletableFuture[0])).get(10, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Error awaiting pipelined leaf flow jobs: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to enqueue pipelined leaf flow jobs: " + e.getMessage(), e);
        }
    }

    /**
     * Retrieves the hierarchical execution tree of a workflow flow DAG starting from a root job ID.
     * Mirrors BullMQ's FlowProducer.getFlow(opts).
     *
     * @param queueName queue where the root parent job resides
     * @param jobId     ID of the parent/root job
     * @return populated JobNode tree with statuses and children dependencies, or null if root job not found
     */
    public io.oxmq.model.JobNode getFlow(String queueName, String jobId) {
        return getFlow(queueName, jobId, 10);
    }

    /**
     * Retrieves the hierarchical execution tree of a workflow flow DAG with configurable maximum depth.
     *
     * @param queueName queue where the root parent job resides
     * @param jobId     ID of the parent/root job
     * @param maxDepth  maximum recursion depth to traverse
     * @return populated JobNode tree with statuses and children dependencies, or null if root job not found
     */
    public io.oxmq.model.JobNode getFlow(String queueName, String jobId, int maxDepth) {
        if (queueName == null || jobId == null || maxDepth < 0) {
            return null;
        }

        QueueKeys queueKeys = (clusterTag != null && !clusterTag.isBlank())
                ? QueueKeys.forClusterFlow(clusterTag, defaultPrefix, queueName)
                : new QueueKeys(defaultPrefix, queueName);

        String jobKey = queueKeys.toJobKey(jobId);
        var sync = connectionManager.getCommandConnection().sync();
        Map<String, String> fields = sync.hgetall(jobKey);
        if (fields == null || fields.isEmpty()) {
            return null;
        }

        String name = fields.getOrDefault("name", "unnamed");
        String dataStr = fields.get("data");
        String optsStr = fields.get("opts");
        io.oxmq.model.JobOptions opts = (optsStr != null && !optsStr.isBlank())
                ? serializer.deserialize(optsStr, io.oxmq.model.JobOptions.class)
                : new io.oxmq.model.JobOptions();

        io.oxmq.model.Job<Object> job = new io.oxmq.model.Job<>(jobId, name, dataStr, opts);
        job.setQueueName(queueName);
        String attemptsStr = fields.get("attemptsMade");
        if (attemptsStr == null) {
            attemptsStr = fields.get("atm");
        }
        if (attemptsStr != null) {
            try { job.setAttemptsMade(Integer.parseInt(attemptsStr)); } catch (NumberFormatException ignored) {}
        }
        String attemptsStartedStr = fields.get("attemptsStarted");
        if (attemptsStartedStr == null) {
            attemptsStartedStr = fields.get("ats");
        }
        if (attemptsStartedStr != null) {
            try { job.setAttemptsStarted(Integer.parseInt(attemptsStartedStr)); } catch (NumberFormatException ignored) {}
        }
        String progressStr = fields.get("progress");
        if (progressStr != null) {
            try {
                job.setProgress(Integer.parseInt(progressStr));
            } catch (NumberFormatException ignored) {}
        }
        String parentKey = fields.get("parentKey");
        if (parentKey != null) job.setParentKey(parentKey);

        io.oxmq.model.JobNode jobNode = new io.oxmq.model.JobNode(job);
        if (maxDepth == 0) {
            return jobNode;
        }

        // Collect child job keys from pending, processed, failed, and unsuccessful sets
        Set<String> childKeys = new java.util.LinkedHashSet<>();
        Set<String> pending = sync.smembers(jobKey + ":dependencies");
        if (pending != null) childKeys.addAll(pending);
        Map<String, String> processed = sync.hgetall(jobKey + ":processed");
        if (processed != null) childKeys.addAll(processed.keySet());
        Map<String, String> failed = sync.hgetall(jobKey + ":failed");
        if (failed != null) childKeys.addAll(failed.keySet());
        List<String> unsuccessful = sync.zrange(jobKey + ":unsuccessful", 0, -1);
        if (unsuccessful != null) childKeys.addAll(unsuccessful);

        for (String childKey : childKeys) {
            String[] tokens = childKey.split(":");
            if (tokens.length >= 2) {
                String childJobId = tokens[tokens.length - 1];
                String childQueue = tokens[tokens.length - 2].replace("{", "").replace("}", "");
                io.oxmq.model.JobNode childNode = getFlow(childQueue, childJobId, maxDepth - 1);
                if (childNode != null) {
                    jobNode.addChild(childNode);
                }
            }
        }

        return jobNode;
    }

    @Override
    public void close() {
        if (ownsConnectionManager) {
            connectionManager.close();
        }
    }
}
