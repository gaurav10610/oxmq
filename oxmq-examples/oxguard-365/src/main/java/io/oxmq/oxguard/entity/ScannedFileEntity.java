package io.oxmq.oxguard.entity;

import io.oxmq.oxguard.model.ExposureLevel;
import io.oxmq.oxguard.model.ScanOrigin;
import io.oxmq.oxguard.model.ScannedFileRecord;
import io.oxmq.oxguard.model.ThreatVerdict;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

/**
 * PostgreSQL Entity persisting security-scanned file records.
 */
@Entity
@Table(
        name = "oxguard_scanned_files",
        indexes = {
                @Index(name = "idx_file_id", columnList = "fileId", unique = true),
                @Index(name = "idx_exposure_level", columnList = "exposureLevel"),
                @Index(name = "idx_threat_verdict", columnList = "threatVerdict"),
                @Index(name = "idx_scan_origin", columnList = "scanOrigin"),
                @Index(name = "idx_scanned_at", columnList = "scannedAtTimestamp")
        }
)
public class ScannedFileEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String fileId;

    @Column(nullable = false)
    private String driveId;

    @Column(nullable = false)
    private String tenantId;

    @Column(nullable = false)
    private String fileName;

    @Column(nullable = false, length = 1024)
    private String filePath;

    private long fileSize;

    private String mimeType;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ExposureLevel exposureLevel;

    private boolean hasUniquePermissions;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ThreatVerdict threatVerdict;

    @Column(length = 1024)
    private String threatDetails;

    private String sha256;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ScanOrigin scanOrigin;

    @Column(nullable = false)
    private long scannedAtTimestamp;

    public ScannedFileEntity() {}

    public ScannedFileEntity(
            String fileId,
            String driveId,
            String tenantId,
            String fileName,
            String filePath,
            long fileSize,
            String mimeType,
            ExposureLevel exposureLevel,
            boolean hasUniquePermissions,
            ThreatVerdict threatVerdict,
            String threatDetails,
            String sha256,
            ScanOrigin scanOrigin,
            long scannedAtTimestamp
    ) {
        this.fileId = fileId;
        this.driveId = driveId;
        this.tenantId = tenantId;
        this.fileName = fileName;
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.mimeType = mimeType;
        this.exposureLevel = exposureLevel;
        this.hasUniquePermissions = hasUniquePermissions;
        this.threatVerdict = threatVerdict;
        this.threatDetails = threatDetails;
        this.sha256 = sha256;
        this.scanOrigin = scanOrigin;
        this.scannedAtTimestamp = scannedAtTimestamp;
    }

    public static ScannedFileEntity fromRecord(ScannedFileRecord r) {
        return new ScannedFileEntity(
                r.fileId(),
                r.driveId(),
                r.tenantId(),
                r.fileName(),
                r.filePath(),
                r.fileSize(),
                r.mimeType(),
                r.exposureLevel(),
                r.hasUniquePermissions(),
                r.threatVerdict(),
                r.threatDetails(),
                r.sha256(),
                r.scanOrigin(),
                r.scannedAtTimestamp()
        );
    }

    public ScannedFileRecord toRecord() {
        return new ScannedFileRecord(
                fileId,
                driveId,
                tenantId,
                fileName,
                filePath,
                fileSize,
                mimeType,
                exposureLevel,
                hasUniquePermissions,
                threatVerdict,
                threatDetails,
                sha256,
                scanOrigin,
                scannedAtTimestamp
        );
    }

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getFileId() { return fileId; }
    public void setFileId(String fileId) { this.fileId = fileId; }
    public String getDriveId() { return driveId; }
    public void setDriveId(String driveId) { this.driveId = driveId; }
    public String getTenantId() { return tenantId; }
    public void setTenantId(String tenantId) { this.tenantId = tenantId; }
    public String getFileName() { return fileName; }
    public void setFileName(String fileName) { this.fileName = fileName; }
    public String getFilePath() { return filePath; }
    public void setFilePath(String filePath) { this.filePath = filePath; }
    public long getFileSize() { return fileSize; }
    public void setFileSize(long fileSize) { this.fileSize = fileSize; }
    public String getMimeType() { return mimeType; }
    public void setMimeType(String mimeType) { this.mimeType = mimeType; }
    public ExposureLevel getExposureLevel() { return exposureLevel; }
    public void setExposureLevel(ExposureLevel exposureLevel) { this.exposureLevel = exposureLevel; }
    public boolean isHasUniquePermissions() { return hasUniquePermissions; }
    public void setHasUniquePermissions(boolean hasUniquePermissions) { this.hasUniquePermissions = hasUniquePermissions; }
    public ThreatVerdict getThreatVerdict() { return threatVerdict; }
    public void setThreatVerdict(ThreatVerdict threatVerdict) { this.threatVerdict = threatVerdict; }
    public String getThreatDetails() { return threatDetails; }
    public void setThreatDetails(String threatDetails) { this.threatDetails = threatDetails; }
    public String getSha256() { return sha256; }
    public void setSha256(String sha256) { this.sha256 = sha256; }
    public ScanOrigin getScanOrigin() { return scanOrigin; }
    public void setScanOrigin(ScanOrigin scanOrigin) { this.scanOrigin = scanOrigin; }
    public long getScannedAtTimestamp() { return scannedAtTimestamp; }
    public void setScannedAtTimestamp(long scannedAtTimestamp) { this.scannedAtTimestamp = scannedAtTimestamp; }
}
