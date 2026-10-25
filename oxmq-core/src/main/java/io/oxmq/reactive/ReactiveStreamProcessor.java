package io.oxmq.reactive;

import io.oxmq.model.Job;
import org.reactivestreams.Publisher;

/**
 * Functional interface for processing OxMQ background jobs returning a Reactive Streams {@link Publisher}
 * (such as Project Reactor Flux/Mono or RxJava 3 Flowable).
 *
 * @param <T> Payload data type
 * @param <R> Emitted return value type
 */
@FunctionalInterface
public interface ReactiveStreamProcessor<T, R> {

    /**
     * Executes the background job logic returning a Reactive Streams {@link Publisher}.
     *
     * @param job Job instance containing metadata, payload, and progress/log handles
     * @return {@link Publisher} emitting intermediate progress or final result
     * @throws Exception Any exception triggering failure
     */
    Publisher<? extends R> process(Job<T> job) throws Exception;
}
