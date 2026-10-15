package io.oxmq.exception;

/**
 * Exception thrown when a job is dynamically moved to waiting state from active state.
 * Conforms to BullMQ {@code WaitingError}.
 */
public class WaitingException extends RuntimeException {

    public static final String DEFAULT_MESSAGE = "oxmq:movedToWait";

    public WaitingException() {
        super(DEFAULT_MESSAGE);
    }

    public WaitingException(String message) {
        super(message);
    }
}
