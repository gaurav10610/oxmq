package io.oxmq.relay;

/**
 * Handle representing an active topic subscription in {@link OxmqRelay}.
 */
public interface RelaySubscription extends AutoCloseable {

    /**
     * The topic pattern this subscription is listening to.
     */
    String getPattern();

    /**
     * Unsubscribes this handler from receiving further messages.
     */
    void unsubscribe();

    /**
     * Returns true if this subscription is currently active.
     */
    boolean isActive();

    @Override
    default void close() {
        unsubscribe();
    }
}
