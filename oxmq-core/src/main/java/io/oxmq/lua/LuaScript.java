package io.oxmq.lua;

/**
 * Enumeration of all 49 standalone BullMQ Lua scripts used by OxMQ.
 */
public enum LuaScript {
    ADD_STANDARD_JOB("lua/addStandardJob-9.lua", 9),
    ADD_DELAYED_JOB("lua/addDelayedJob-6.lua", 6),
    ADD_PRIORITIZED_JOB("lua/addPrioritizedJob-9.lua", 9),
    ADD_PARENT_JOB("lua/addParentJob-6.lua", 6),
    MOVE_TO_ACTIVE("lua/moveToActive-11.lua", 11),
    MOVE_TO_FINISHED("lua/moveToFinished-14.lua", 14),
    MOVE_TO_DELAYED("lua/moveToDelayed-12.lua", 12),
    MOVE_TO_WAITING_CHILDREN("lua/moveToWaitingChildren-7.lua", 7),
    RETRY_JOB("lua/retryJob-11.lua", 11),
    EXTEND_LOCK("lua/extendLock-2.lua", 2),
    EXTEND_LOCKS("lua/extendLocks-1.lua", 1),
    RELEASE_LOCK("lua/releaseLock-1.lua", 1),
    MOVE_STALLED_JOBS_TO_WAIT("lua/moveStalledJobsToWait-9.lua", 9),
    MOVE_JOB_FROM_ACTIVE_TO_WAIT("lua/moveJobFromActiveToWait-9.lua", 9),
    MOVE_JOBS_TO_WAIT("lua/moveJobsToWait-8.lua", 8),
    UPDATE_PROGRESS("lua/updateProgress-3.lua", 3),
    ADD_LOG("lua/addLog-2.lua", 2),
    PAUSE("lua/pause-7.lua", 7),
    PROMOTE("lua/promote-9.lua", 9),
    CLEAN_JOBS_IN_SET("lua/cleanJobsInSet-3.lua", 3),
    DRAIN("lua/drain-5.lua", 5),
    OBLITERATE("lua/obliterate-2.lua", 2),
    REMOVE_JOB("lua/removeJob-2.lua", 2),
    REMOVE_CHILD_DEPENDENCY("lua/removeChildDependency-1.lua", 1),
    REMOVE_DEDUPLICATION_KEY("lua/removeDeduplicationKey-1.lua", 1),
    REMOVE_JOB_SCHEDULER("lua/removeJobScheduler-3.lua", 3),
    REMOVE_ORPHANED_JOBS("lua/removeOrphanedJobs-1.lua", 1),
    REMOVE_UNPROCESSED_CHILDREN("lua/removeUnprocessedChildren-2.lua", 2),
    REPROCESS_JOB("lua/reprocessJob-7.lua", 7),
    SAVE_STACKTRACE("lua/saveStacktrace-1.lua", 1),
    UPDATE_DATA("lua/updateData-1.lua", 1),
    UPDATE_JOB_SCHEDULER("lua/updateJobScheduler-12.lua", 12),
    ADD_JOB_SCHEDULER("lua/addJobScheduler-11.lua", 11),
    GET_JOB_SCHEDULER("lua/getJobScheduler-1.lua", 1),
    GET_COUNTS("lua/getCounts-1.lua", 1),
    GET_COUNTS_PER_PRIORITY("lua/getCountsPerPriority-2.lua", 2),
    GET_DEPENDENCY_COUNTS("lua/getDependencyCounts-4.lua", 4),
    GET_JOBS("lua/getJobs-1.lua", 1),
    GET_METRICS("lua/getMetrics-2.lua", 2),
    GET_RANGES("lua/getRanges-1.lua", 1),
    GET_RATE_LIMIT_TTL("lua/getRateLimitTtl-2.lua", 2),
    GET_STATE("lua/getState-8.lua", 8),
    GET_STATE_V2("lua/getStateV2-8.lua", 8),
    IS_FINISHED("lua/isFinished-3.lua", 3),
    IS_JOB_IN_LIST("lua/isJobInList-1.lua", 1),
    IS_MAXED("lua/isMaxed-2.lua", 2),
    CHANGE_DELAY("lua/changeDelay-4.lua", 4),
    CHANGE_PRIORITY("lua/changePriority-7.lua", 7),
    PAGINATE("lua/paginate-1.lua", 1);

    private final String resourcePath;
    private final int numberOfKeys;

    LuaScript(String resourcePath, int numberOfKeys) {
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
