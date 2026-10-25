package io.oxmq.lua;

/**
 * Enumeration of all custom OxMQ extension Lua scripts.
 * <p>
 * These scripts are isolated in {@code lua/extensions/} to preserve 100% clean upstream
 * synchronization with open-source BullMQ.
 */
public enum ExtensionLuaScript {
    ADD_GROUP_JOB("lua/extensions/addGroupJob-1.lua", 8),
    MOVE_TO_ACTIVE_GROUP("lua/extensions/moveToActiveGroup-1.lua", 7),
    MOVE_TO_ACTIVE_BATCH_GROUP("lua/extensions/moveToActiveBatchGroup-1.lua", 7),
    DECREMENT_GROUP_ACTIVE("lua/extensions/decrementGroupActive-1.lua", 4),
    PAUSE_GROUP("lua/extensions/pauseGroup-1.lua", 3),
    RESUME_GROUP("lua/extensions/resumeGroup-1.lua", 4),
    DELETE_GROUP("lua/extensions/deleteGroup-1.lua", 6),
    SET_GROUP_RATE_LIMIT("lua/extensions/setGroupRateLimit-1.lua", 3),
    GET_GROUPS_BY_STATUS("lua/extensions/getGroupsByStatus-1.lua", 1),
    GET_GROUP_JOBS("lua/extensions/getGroupJobs-1.lua", 2),
    REPAIR_GROUP("lua/extensions/repairGroup-1.lua", 6);

    private final String resourcePath;
    private final int numberOfKeys;

    ExtensionLuaScript(String resourcePath, int numberOfKeys) {
        this.resourcePath = resourcePath;
        this.numberOfKeys = numberOfKeys;
    }

    public String getResourcePath() {
        return resourcePath;
    }

    public int getNumberOfKeys() {
        return numberOfKeys;
    }
}
