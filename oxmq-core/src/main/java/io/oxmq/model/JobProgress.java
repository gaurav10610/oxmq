package io.oxmq.model;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Progress payload of a running job.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record JobProgress(int percentage, Object payload) {

    public static JobProgress of(int percentage) {
        return new JobProgress(Math.min(100, Math.max(0, percentage)), null);
    }

    public static JobProgress of(int percentage, Object payload) {
        return new JobProgress(Math.min(100, Math.max(0, percentage)), payload);
    }
}
