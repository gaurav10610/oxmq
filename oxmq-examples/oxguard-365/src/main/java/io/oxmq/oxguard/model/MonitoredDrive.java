package io.oxmq.oxguard.model;

/**
 * Representation of a monitored SharePoint document library or OneDrive.
 */
public record MonitoredDrive(
        String driveId,
        String tenantId,
        DriveType driveType,
        String name,
        String ownerPrincipal,
        String deltaToken,
        String webhookSubscriptionId
) {
    public MonitoredDrive withDeltaToken(String newDeltaToken) {
        return new MonitoredDrive(driveId, tenantId, driveType, name, ownerPrincipal, newDeltaToken, webhookSubscriptionId);
    }
}
