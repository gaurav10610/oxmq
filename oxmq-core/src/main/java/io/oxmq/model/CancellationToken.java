package io.oxmq.model;

import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Cooperative cancellation token for long-running jobs, mirroring JavaScript's AbortSignal.
 */
public class CancellationToken {

    private volatile boolean cancelled = false;
    private volatile String reason;
    private final CopyOnWriteArrayList<Consumer<String>> callbacks = new CopyOnWriteArrayList<>();

    /**
     * Checks if cancellation has been requested for this token or current thread is interrupted.
     */
    public boolean isCancelled() {
        return cancelled || Thread.currentThread().isInterrupted();
    }

    /**
     * Throws {@link io.oxmq.exception.JobCancelledException} if cancellation has been requested.
     *
     * @throws io.oxmq.exception.JobCancelledException if cancelled or thread interrupted
     */
    public void throwIfCancelled() {
        if (isCancelled()) {
            throw new io.oxmq.exception.JobCancelledException(
                    reason != null ? reason : "Job cancelled or thread interrupted");
        }
    }

    /**
     * Returns the cancellation reason, if any.
     */
    public String getReason() {
        return reason;
    }

    /**
     * Triggers cancellation on this token and notifies registered callbacks.
     *
     * @param reason cancellation reason
     */
    public void cancel(String reason) {
        this.reason = reason;
        this.cancelled = true;
        for (Consumer<String> callback : callbacks) {
            try {
                callback.accept(reason);
            } catch (Exception ignored) {}
        }
    }

    /**
     * Registers a callback to be invoked when cancellation is triggered.
     * If already cancelled, the callback is executed immediately.
     *
     * @param callback consumer receiving the cancellation reason
     */
    public void onCancel(Consumer<String> callback) {
        Objects.requireNonNull(callback, "callback must not be null");
        callbacks.add(callback);
        if (cancelled) {
            try {
                callback.accept(reason);
            } catch (Exception ignored) {}
        }
    }
}
