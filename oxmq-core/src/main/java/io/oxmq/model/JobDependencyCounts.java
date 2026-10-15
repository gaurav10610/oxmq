package io.oxmq.model;

/**
 * Breakdown of child dependency states for a parent job in a DAG.
 * Powered by BullMQ's {@code getDependencyCounts-4.lua}.
 */
public record JobDependencyCounts(long processed, long unprocessed, long failed, long ignored) {

    public static JobDependencyCounts empty() {
        return new JobDependencyCounts(0, 0, 0, 0);
    }

    public long total() {
        return processed + unprocessed + failed + ignored;
    }

    public boolean isAllProcessed() {
        return unprocessed == 0 && failed == 0;
    }
}
