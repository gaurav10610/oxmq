package io.oxmq.oxdoc.connector;

import io.oxmq.oxdoc.model.ContentVisibility;
import io.oxmq.oxdoc.model.MonitoredDrive;
import io.oxmq.oxdoc.model.TenantProfile;
import java.io.InputStream;
import java.util.List;

/**
 * Contract for interacting with Microsoft Graph API (Live or Mocked).
 */
public interface GraphClient {

    /**
     * Discovers tenant license SKUs, paid seat counts, and organizational metadata from Microsoft Graph.
     * Calls GET /v1.0/subscribedSkus and GET /v1.0/organization.
     */
    TenantProfile discoverTenantProfile(String tenantId);

    /**
     * Discovers total enterprise user count as a heuristic fallback when /subscribedSkus is restricted.
     * Uses User.Read.All to query active tenant accounts.
     */
    default int discoverActiveUserCount(String tenantId) {
        return 0;
    }

    /**
     * Discovers all SharePoint Site Document Libraries for the tenant.
     */
    List<MonitoredDrive> discoverSharePointDrives(String tenantId);

    /**
     * Discovers all User OneDrive Document Libraries for active enterprise users.
     */
    List<MonitoredDrive> discoverUserOneDrives(String tenantId);

    /**
     * Executes incremental delta synchronization on a specific drive.
     */
    DeltaSyncResult fetchDeltaChanges(String driveId, String deltaToken);

    default DeltaSyncResult fetchDeltaChanges(String tenantId, String driveId, String deltaToken) {
        return fetchDeltaChanges(driveId, deltaToken);
    }

    default DeltaSyncResult fetchDriveDelta(String tenantId, String driveId, String deltaToken) {
        return fetchDeltaChanges(tenantId, driveId, deltaToken);
    }

    /**
     * Retrieves the Access Control List and resolves effective permissions for a drive item.
     */
    ContentVisibility fetchPermissions(String driveId, String itemId);

    default ContentVisibility fetchPermissions(String tenantId, String driveId, String itemId) {
        return fetchPermissions(driveId, itemId);
    }

    /**
     * Opens a streaming connection to download file bytes.
     * Uses direct pre-authenticated SAS URL (direct Azure Blob download) to bypass Graph quotas.
     */
    InputStream streamFileContent(String driveId, String itemId, String directDownloadUrl);

    default InputStream streamFileContent(String tenantId, String driveId, String itemId, String directDownloadUrl) {
        return streamFileContent(driveId, itemId, directDownloadUrl);
    }
}
