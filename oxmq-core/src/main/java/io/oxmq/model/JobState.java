package io.oxmq.model;

/**
 * Lifecycle states of an OxMQ job.
 */
public enum JobState {
    WAITING,
    ACTIVE,
    DELAYED,
    COMPLETED,
    FAILED,
    PAUSED,
    STALLED,
    WAITING_CHILDREN
}
