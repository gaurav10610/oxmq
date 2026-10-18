package io.oxmq.oxguard.queue;

/**
 * Queue names and Redis keys used across OxGuard 365.
 */
public final class OxGuardQueueConstants {

    private OxGuardQueueConstants() {}

    public static final String QUEUE_TENANT_DISCOVERY = "tenant-discovery-queue";
    public static final String QUEUE_DRIVE_DELTA = "drive-delta-sync-queue";
    public static final String QUEUE_FILE_PERMISSION = "file-permission-queue";
    public static final String QUEUE_FILE_MALWARE = "file-malware-queue";
    public static final String QUEUE_FILE_AGGREGATE = "file-aggregate-queue";
}
