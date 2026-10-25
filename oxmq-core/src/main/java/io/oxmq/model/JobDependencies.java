package io.oxmq.model;

import java.util.List;
import java.util.Map;

/**
 * Detailed dependency breakdown for a parent job in a workflow DAG.
 * Mirrors BullMQ's job.getDependencies() output.
 *
 * @param parentJobId ID of the parent job
 * @param unprocessedChildKeys list of child job keys still pending execution
 * @param processedChildren map of completed child job keys to their return values
 * @param failedChildren map of failed child job keys to their failure reasons
 */
public record JobDependencies(
    String parentJobId,
    List<String> unprocessedChildKeys,
    Map<String, String> processedChildren,
    Map<String, String> failedChildren
) {
    public int totalDependencies() {
        return (unprocessedChildKeys != null ? unprocessedChildKeys.size() : 0)
                + (processedChildren != null ? processedChildren.size() : 0)
                + (failedChildren != null ? failedChildren.size() : 0);
    }

    public boolean isComplete() {
        return (unprocessedChildKeys == null || unprocessedChildKeys.isEmpty())
                && (failedChildren == null || failedChildren.isEmpty());
    }

    public boolean hasFailures() {
        return failedChildren != null && !failedChildren.isEmpty();
    }
}
