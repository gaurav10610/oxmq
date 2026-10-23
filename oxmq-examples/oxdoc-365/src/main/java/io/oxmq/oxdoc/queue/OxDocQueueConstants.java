package io.oxmq.oxdoc.queue;

/**
 * Queue names and Redis keys used across OxDoc 365.
 */
public final class OxDocQueueConstants {

    private OxDocQueueConstants() {}

    public static final String QUEUE_TENANT_DISCOVERY = "tenant-discovery-queue";
    public static final String QUEUE_DRIVE_DELTA = "drive-delta-sync-queue";
    public static final String QUEUE_CONTENT_CATEGORIZATION = "content-categorization-queue";
    public static final String QUEUE_DOCUMENT_EXTRACTION = "document-extraction-queue";
    public static final String QUEUE_CATALOG_INDEX = "catalog-index-queue";

    // Aliases for compatibility
    public static final String QUEUE_FILE_PERMISSION = QUEUE_CONTENT_CATEGORIZATION;
    public static final String QUEUE_FILE_MALWARE = QUEUE_DOCUMENT_EXTRACTION;
    public static final String QUEUE_FILE_AGGREGATE = QUEUE_CATALOG_INDEX;
}
