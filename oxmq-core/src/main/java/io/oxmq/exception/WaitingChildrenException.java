package io.oxmq.exception;

/**
 * Exception thrown when a job is dynamically moved to waiting-children state from active state.
 * Conforms to BullMQ {@code WaitingChildrenError}.
 */
public class WaitingChildrenException extends RuntimeException {

    public static final String DEFAULT_MESSAGE = "oxmq:movedToWaitingChildren";

    public WaitingChildrenException() {
        super(DEFAULT_MESSAGE);
    }

    public WaitingChildrenException(String message) {
        super(message);
    }
}
