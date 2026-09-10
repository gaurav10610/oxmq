package io.oxmq.model;

/**
 * Encapsulates a job creation request for single and bulk enqueuing.
 *
 * @param <T> Payload data type
 */
public record JobRequest<T>(String name, T data, JobOptions options) {

    public JobRequest(String name, T data) {
        this(name, data, JobOptions.defaults());
    }

    public static <T> JobRequest<T> of(String name, T data) {
        return new JobRequest<>(name, data, JobOptions.defaults());
    }

    public static <T> JobRequest<T> of(String name, T data, JobOptions options) {
        return new JobRequest<>(name, data, options);
    }
}
