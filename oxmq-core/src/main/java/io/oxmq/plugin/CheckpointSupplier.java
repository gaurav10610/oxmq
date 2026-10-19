package io.oxmq.plugin;

/**
 * Functional interface implemented by job results or payloads that supply a checkpoint cursor.
 * <p>
 * When a worker returns an object implementing {@link CheckpointSupplier},
 * {@link CheckpointStatePlugin} will automatically commit the new cursor to persistent storage.
 */
@FunctionalInterface
public interface CheckpointSupplier {

    /**
     * Returns the checkpoint cursor (e.g. delta token, stream offset, sequence number, or timestamp).
     *
     * @return cursor string, or {@code null} if no checkpoint update is required
     */
    String getCheckpointCursor();
}
