package io.oxmq.model;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Execution context and operations provider for a connected {@link Job}.
 * Decouples active domain job operations from direct Redis connection management,
 * eliminating the need to allocate 12+ lambda closures per dequeued job.
 */
public interface JobContext {

    default void updateData(String jobId, Object newData) {}

    default void promote(String jobId) {}

    default void changeDelay(String jobId, Duration delay) {}

    default void changePriority(String jobId, int priority) {}

    default void retry(String jobId) {}

    default boolean remove(String jobId) {
        return false;
    }

    default JobState getState(String jobId) {
        return null;
    }

    default JobDependencies getDependencies(String jobId) {
        return null;
    }

    default JobNode getFlow(String jobId, int maxDepth) {
        return null;
    }

    default boolean clearLogs(String jobId) {
        return false;
    }

    default void addLog(String jobId, String log) {}

    default void updateProgress(String jobId, int progress, Object payload) {}

    default void saveCheckpoint(String jobId, String step, Object state) {}

    default void spawnChildren(String jobId, List<? extends FlowJob<?>> children) {}

    default Map<String, Object> getChildrenValues(String jobId) {
        return null;
    }

    default Map<String, String> getIgnoredChildrenFailures(String jobId) {
        return Map.of();
    }

    default Object waitUntilFinished(String jobId, Duration timeout) throws Exception {
        return null;
    }

    default CompletableFuture<Object> waitUntilFinishedAsync(String jobId) {
        return null;
    }
}
