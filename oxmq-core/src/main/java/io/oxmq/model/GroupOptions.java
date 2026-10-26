package io.oxmq.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Objects;

/**
 * Configuration options for virtual queue groups (multi-tenant sub-queues).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GroupOptions {

    private String id;
    private long maxSize = 0;
    private int priority = 0;

    public GroupOptions() {}

    public GroupOptions(String id) {
        this.id = Objects.requireNonNull(id, "Group ID must not be null");
    }

    public GroupOptions(String id, long maxSize) {
        this.id = Objects.requireNonNull(id, "Group ID must not be null");
        this.maxSize = Math.max(0, maxSize);
    }

    public GroupOptions(String id, long maxSize, int priority) {
        this.id = Objects.requireNonNull(id, "Group ID must not be null");
        this.maxSize = Math.max(0, maxSize);
        this.priority = priority;
    }

    public static GroupOptions of(String id) {
        return new GroupOptions(id);
    }

    public static GroupOptions of(String id, long maxSize) {
        return new GroupOptions(id, maxSize);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public static class Builder {
        private String id;
        private long maxSize = 0;
        private int priority = 0;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder maxSize(long maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        public GroupOptions build() {
            return new GroupOptions(Objects.requireNonNull(id, "Group id must not be null"), maxSize, priority);
        }
    }
}
