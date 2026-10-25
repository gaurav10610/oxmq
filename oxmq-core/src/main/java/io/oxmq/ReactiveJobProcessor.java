package io.oxmq;

/**
 * Functional interface for processing OxMQ background jobs reactively.
 *
 * @param <T> Payload data type
 * @param <R> Emitted return value type
 */
@FunctionalInterface
public interface ReactiveJobProcessor<T, R> extends io.oxmq.reactive.ReactiveJobProcessor<T, R> {
}
