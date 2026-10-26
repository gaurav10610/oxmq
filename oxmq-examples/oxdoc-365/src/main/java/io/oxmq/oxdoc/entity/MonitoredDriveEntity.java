package io.oxmq.oxdoc.entity;

import io.oxmq.oxdoc.model.DriveType;
import io.oxmq.oxdoc.model.MonitoredDrive;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * PostgreSQL Entity persisting discovered monitored drives (SharePoint libraries &amp; user OneDrives).
 */
@Entity
@Table(name = "oxdoc_monitored_drives")
public class MonitoredDriveEntity {

    @Id
    @Column(nullable = false, unique = true)
    private String driveId;

    @Column(nullable = false)
    private String tenantId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private DriveType driveType;

    @Column(nullable = false)
    private String name;

    private String ownerPrincipal;

    @Column(length = 2048)
    private String deltaToken;

    private String webhookSubscriptionId;

    public MonitoredDriveEntity() {}

    public MonitoredDriveEntity(
            String driveId,
            String tenantId,
            DriveType driveType,
            String name,
            String ownerPrincipal,
            String deltaToken,
            String webhookSubscriptionId
    ) {
        this.driveId = driveId;
        this.tenantId = tenantId;
        this.driveType = driveType;
        this.name = name;
        this.ownerPrincipal = ownerPrincipal;
        this.deltaToken = deltaToken;
        this.webhookSubscriptionId = webhookSubscriptionId;
    }

    public static MonitoredDriveEntity fromModel(MonitoredDrive d) {
        return new MonitoredDriveEntity(
                d.driveId(),
                d.tenantId(),
                d.driveType(),
                d.name(),
                d.ownerPrincipal(),
                d.deltaToken(),
                d.webhookSubscriptionId()
        );
    }

    public MonitoredDrive toModel() {
        return new MonitoredDrive(
                driveId,
                tenantId,
                driveType,
                name,
                ownerPrincipal,
                deltaToken,
                webhookSubscriptionId
        );
    }

    public String getDriveId() { return driveId; }
    public void setDriveId(String driveId) { this.driveId = driveId; }
    public String getTenantId() { return tenantId; }
    public void setTenantId(String tenantId) { this.tenantId = tenantId; }
    public DriveType getDriveType() { return driveType; }
    public void setDriveType(DriveType driveType) { this.driveType = driveType; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getOwnerPrincipal() { return ownerPrincipal; }
    public void setOwnerPrincipal(String ownerPrincipal) { this.ownerPrincipal = ownerPrincipal; }
    public String getDeltaToken() { return deltaToken; }
    public void setDeltaToken(String deltaToken) { this.deltaToken = deltaToken; }
    public String getWebhookSubscriptionId() { return webhookSubscriptionId; }
    public void setWebhookSubscriptionId(String webhookSubscriptionId) { this.webhookSubscriptionId = webhookSubscriptionId; }
}
