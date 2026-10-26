package io.oxmq;

/**
 * Functional interface for processing OxMQ background jobs returning a Reactive Streams {@link org.reactivestreams.Publisher}.
 *
 * @param <T> Payload data type
 * @param <R> Emitted return value type
 * @deprecated Use {@link io.oxmq.reactive.ReactiveStreamProcessor} instead.
 */
@Deprecated(since = "1.8.0", forRemoval = true)
@FunctionalInterface
public interface ReactiveStreamProcessor<T, R> extends io.oxmq.reactive.ReactiveStreamProcessor<T, R> {
}
