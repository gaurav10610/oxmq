package io.oxmq.exception;

/**
 * Thrown when a job cannot be added to a virtual group because the group's pending backlog
 * has reached its configured {@code maxSize} limit.
 */
public class GroupMaxSizeExceededException extends RuntimeException {

    private final String groupId;
    private final long maxSize;

    public GroupMaxSizeExceededException(String groupId, long maxSize) {
        super("Virtual group '" + groupId + "' backlog exceeded maximum allowed size of " + maxSize + " jobs");
        this.groupId = groupId;
        this.maxSize = maxSize;
    }

    public String getGroupId() {
        return groupId;
    }

    public long getMaxSize() {
        return maxSize;
    }
}
