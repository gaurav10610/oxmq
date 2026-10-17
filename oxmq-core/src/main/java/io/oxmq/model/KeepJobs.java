package io.oxmq.model;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Retention policy for completed and failed jobs in Redis.
 * Matches BullMQ's keepJobs { count, age, limit } specification.
 */
public class KeepJobs {

    private final int count;
    private final long ageSeconds;
    private final int limit;

    public KeepJobs() {
        this(-1, 0, 1000);
    }

    @com.fasterxml.jackson.annotation.JsonCreator
    public KeepJobs(
            @com.fasterxml.jackson.annotation.JsonProperty("count") Integer count,
            @com.fasterxml.jackson.annotation.JsonProperty("age") Long age,
            @com.fasterxml.jackson.annotation.JsonProperty("limit") Integer limit) {
        this.count = count != null ? count : -1;
        this.ageSeconds = age != null ? age : 0;
        this.limit = (limit != null && limit > 0) ? limit : 1000;
    }

    public KeepJobs(int count, long ageSeconds) {
        this(count, ageSeconds, 1000);
    }

    public static KeepJobs count(int count) {
        return new KeepJobs(count, 0);
    }

    public static KeepJobs age(Duration age) {
        return new KeepJobs(0, age != null ? age.toSeconds() : 0);
    }

    public static KeepJobs of(int count, Duration age) {
        return new KeepJobs(count, age != null ? age.toSeconds() : 0);
    }

    public static KeepJobs of(int count, Duration age, int limit) {
        return new KeepJobs(count, age != null ? age.toSeconds() : 0, limit);
    }

    public static KeepJobs removeImmediately() {
        return new KeepJobs(0, 0);
    }

    public int getCount() {
        return count;
    }

    public long getAgeSeconds() {
        return ageSeconds;
    }

    public int getLimit() {
        return limit;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        if (count >= 0) {
            map.put("count", count);
        }
        if (ageSeconds > 0) {
            map.put("age", ageSeconds);
        }
        if (limit > 0) {
            map.put("limit", limit);
        }
        return map;
    }
}
