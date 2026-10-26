package io.oxmq.reactive;

import io.oxmq.model.Job;
import java.util.concurrent.Flow;

/**
 * Functional interface for processing OxMQ background jobs reactively.
 * <p>
 * Returning a {@link Flow.Publisher} enables streaming job execution, intermediate
 * progress checkpoints on each emitted item, and cooperative cancellation.
 * Compatible with Project Reactor (Flux, Mono), RxJava 3, and standard Java 9+ Flow.
 *
 * @param <T> Payload data type
 * @param <R> Emitted return value type
 */
@FunctionalInterface
public interface ReactiveJobProcessor<T, R> {

    /**
     * Executes the background job logic reactively.
     *
     * @param job Job instance containing metadata, payload, and progress/log handles
     * @return {@link Flow.Publisher} emitting intermediate progress or final result
     * @throws Exception Any exception triggering retries or failure transitions
     */
    Flow.Publisher<? extends R> process(Job<T> job) throws Exception;

    /**
     * Adapts a Reactive Streams {@link org.reactivestreams.Publisher} (e.g., Project Reactor Flux/Mono)
     * to a {@link ReactiveJobProcessor}.
     */
    static <T, R> ReactiveJobProcessor<T, R> from(ReactiveStreamProcessor<T, R> streamProcessor) {
        return job -> org.reactivestreams.FlowAdapters.toFlowPublisher((org.reactivestreams.Publisher<R>) streamProcessor.process(job));
    }
}
