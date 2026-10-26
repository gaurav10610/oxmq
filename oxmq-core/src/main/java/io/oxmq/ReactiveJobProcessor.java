package io.oxmq;

/**
 * Functional interface for processing OxMQ background jobs reactively.
 *
 * @param <T> Payload data type
 * @param <R> Emitted return value type
 * @deprecated Use {@link io.oxmq.reactive.ReactiveJobProcessor} instead.
 */
@Deprecated(since = "1.8.0", forRemoval = true)
@FunctionalInterface
public interface ReactiveJobProcessor<T, R> extends io.oxmq.reactive.ReactiveJobProcessor<T, R> {
}
