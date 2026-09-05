package io.oxmq.lua;

/**
 * Enumeration of BullMQ-compatible Lua scripts used by OxMQ.
 */
public enum LuaScript {
    ADD_JOB("lua/addJob.lua"),
    MOVE_TO_ACTIVE("lua/moveToActive.lua"),
    MOVE_TO_FINISHED("lua/moveToFinished.lua"),
    RETRY_JOB("lua/retryJob.lua"),
    EXTEND_LOCK("lua/extendLock.lua"),
    CLEAN_QUEUE("lua/cleanQueue.lua"),
    PAUSE_QUEUE("lua/pauseQueue.lua"),
    RATE_LIMIT("lua/rateLimit.lua"),
    OBLITERATE("lua/obliterate.lua");

    private final String resourcePath;

    LuaScript(String resourcePath) {
        this.resourcePath = resourcePath;
    }

    public String getResourcePath() {
        return resourcePath;
    }
}
