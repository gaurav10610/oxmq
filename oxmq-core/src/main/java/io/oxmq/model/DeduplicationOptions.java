package io.oxmq.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Deduplication options conforming to BullMQ v6 semantics.
 * <p>
 * Supports standard TTL deduplication as well as {@code keepLastIfActive} which
 * guarantees at most 2 jobs per deduplication ID (1 active, 1 waiting),
 * automatically requeuing the latest payload once the active job finishes.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeduplicationOptions {

    private final String id;
    private final long ttlMs;
    private final boolean keepLastIfActive;

    public DeduplicationOptions(String id, long ttlMs, boolean keepLastIfActive) {
        this.id = Objects.requireNonNull(id, "deduplication id must not be null");
        this.ttlMs = ttlMs;
        this.keepLastIfActive = keepLastIfActive;
    }

    public static DeduplicationOptions of(String id) {
        return new DeduplicationOptions(id, 0, false);
    }

    public static DeduplicationOptions of(String id, Duration ttl) {
        return new DeduplicationOptions(id, ttl != null ? ttl.toMillis() : 0, false);
    }

    public static DeduplicationOptions ofKeepLastIfActive(String id) {
        return new DeduplicationOptions(id, 0, true);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getId() {
        return id;
    }

    public long getTtlMs() {
        return ttlMs;
    }

    public boolean isKeepLastIfActive() {
        return keepLastIfActive;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("id", id);
        if (ttlMs > 0 && !keepLastIfActive) {
            map.put("ttl", ttlMs);
        }
        if (keepLastIfActive) {
            map.put("keepLastIfActive", true);
        }
        return map;
    }

    public static class Builder {
        private String id;
        private long ttlMs = 0;
        private boolean keepLastIfActive = false;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder ttl(Duration ttl) {
            this.ttlMs = ttl != null ? ttl.toMillis() : 0;
            return this;
        }

        public Builder ttlMs(long ttlMs) {
            this.ttlMs = ttlMs;
            return this;
        }

        public Builder keepLastIfActive(boolean keepLastIfActive) {
            this.keepLastIfActive = keepLastIfActive;
            return this;
        }

        public DeduplicationOptions build() {
            return new DeduplicationOptions(id, ttlMs, keepLastIfActive);
        }
    }
}
