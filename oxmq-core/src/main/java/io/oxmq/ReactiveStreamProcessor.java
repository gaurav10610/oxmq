package io.oxmq;

/**
 * Functional interface for processing OxMQ background jobs returning a Reactive Streams {@link org.reactivestreams.Publisher}.
 *
 * @param <T> Payload data type
 * @param <R> Emitted return value type
 */
@FunctionalInterface
public interface ReactiveStreamProcessor<T, R> extends io.oxmq.reactive.ReactiveStreamProcessor<T, R> {
}
