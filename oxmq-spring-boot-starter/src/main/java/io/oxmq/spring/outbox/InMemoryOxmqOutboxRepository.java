package io.oxmq.spring.outbox;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Thread-safe, in-memory implementation of {@link OxmqOutboxRepository} for tests and embedded applications.
 */
public class InMemoryOxmqOutboxRepository implements OxmqOutboxRepository {

    private final ConcurrentMap<String, OxmqOutboxRecord> storage = new ConcurrentHashMap<>();

    @Override
    public void save(OxmqOutboxRecord record) {
        if (record != null && record.getId() != null) {
            storage.put(record.getId(), record);
        }
    }

    @Override
    public List<OxmqOutboxRecord> findPending(int limit) {
        return storage.values().stream()
                .filter(r -> r.getStatus() == OxmqOutboxRecord.Status.PENDING)
                .sorted(Comparator.comparing(OxmqOutboxRecord::getCreatedAt))
                .limit(limit > 0 ? limit : 100)
                .toList();
    }

    @Override
    public void markPublished(String id) {
        OxmqOutboxRecord record = storage.get(id);
        if (record != null) {
            record.setStatus(OxmqOutboxRecord.Status.PUBLISHED);
            record.setPublishedAt(Instant.now());
        }
    }

    @Override
    public void markAllPublished(List<String> ids) {
        if (ids == null || ids.isEmpty()) return;
        Instant now = Instant.now();
        for (String id : ids) {
            OxmqOutboxRecord record = storage.get(id);
            if (record != null) {
                record.setStatus(OxmqOutboxRecord.Status.PUBLISHED);
                record.setPublishedAt(now);
            }
        }
    }

    @Override
    public void markFailed(String id, String error) {
        OxmqOutboxRecord record = storage.get(id);
        if (record != null) {
            record.setAttempts(record.getAttempts() + 1);
            record.setLastError(error);
            if (record.getAttempts() >= 10) {
                record.setStatus(OxmqOutboxRecord.Status.FAILED);
            }
        }
    }

    @Override
    public Optional<OxmqOutboxRecord> findById(String id) {
        return Optional.ofNullable(storage.get(id));
    }

    @Override
    public long purgePublishedOlderThan(Duration maxAge) {
        Instant cutoff = Instant.now().minus(maxAge);
        long removed = 0;
        for (var entry : storage.entrySet()) {
            OxmqOutboxRecord r = entry.getValue();
            if (r.getStatus() == OxmqOutboxRecord.Status.PUBLISHED
                    && r.getPublishedAt() != null
                    && r.getPublishedAt().isBefore(cutoff)) {
                if (storage.remove(entry.getKey(), r)) {
                    removed++;
                }
            }
        }
        return removed;
    }

    public void clear() {
        storage.clear();
    }

    public int size() {
        return storage.size();
    }
}
