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
    WAITING_CHILDREN,
    PRIORITIZED;

    public static JobState fromBullMqState(String stateStr) {
        if (stateStr == null || stateStr.isBlank()) {
            return null;
        }
        return switch (stateStr.toLowerCase().trim()) {
            case "wait", "waiting" -> WAITING;
            case "active" -> ACTIVE;
            case "delayed" -> DELAYED;
            case "completed" -> COMPLETED;
            case "failed" -> FAILED;
            case "paused" -> PAUSED;
            case "waiting-children" -> WAITING_CHILDREN;
            case "stalled" -> STALLED;
            case "prioritized" -> PRIORITIZED;
            default -> null;
        };
    }

    public String toBullMqState() {
        return switch (this) {
            case WAITING -> "wait";
            case ACTIVE -> "active";
            case DELAYED -> "delayed";
            case COMPLETED -> "completed";
            case FAILED -> "failed";
            case PAUSED -> "paused";
            case WAITING_CHILDREN -> "waiting-children";
            case STALLED -> "stalled";
            case PRIORITIZED -> "prioritized";
        };
    }

    public String getBullMqName() {
        return toBullMqState();
    }
}
