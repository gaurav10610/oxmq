package io.oxmq.spring.outbox;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Pluggable SPI for storing and retrieving Outbox records.
 */
public interface OxmqOutboxRepository {

    /**
     * Persists a newly created outbox record within the current database transaction.
     */
    void save(OxmqOutboxRecord record);

    /**
     * Retrieves up to {@code limit} records ready for publishing (e.g. status = PENDING).
     */
    List<OxmqOutboxRecord> findPending(int limit);

    /**
     * Marks a record as successfully published to Redis.
     */
    void markPublished(String id);

    /**
     * Marks multiple records as successfully published to Redis in a single batch operation.
     */
    default void markAllPublished(List<String> ids) {
        if (ids != null) {
            for (String id : ids) {
                markPublished(id);
            }
        }
    }

    /**
     * Records a publishing error and increments retry attempts.
     */
    void markFailed(String id, String error);

    /**
     * Finds a record by its identifier.
     */
    Optional<OxmqOutboxRecord> findById(String id);

    /**
     * Purges records that were published longer ago than {@code maxAge}.
     */
    long purgePublishedOlderThan(Duration maxAge);
}
