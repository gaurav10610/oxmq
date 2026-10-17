package io.oxmq.exception;

/**
 * Thrown or recorded when a running job is cancelled cooperatively via {@code worker.cancelJob(jobId)}.
 * Indicates that the failure was intentional and the job must not be retried.
 */
public class JobCancelledException extends RuntimeException {

    public JobCancelledException(String message) {
        super(message);
    }

    public JobCancelledException(String message, Throwable cause) {
        super(message, cause);
    }
}
