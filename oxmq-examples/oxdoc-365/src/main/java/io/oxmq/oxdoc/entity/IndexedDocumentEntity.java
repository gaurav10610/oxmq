package io.oxmq.oxdoc.entity;

import io.oxmq.oxdoc.model.AccessScope;
import io.oxmq.oxdoc.model.IndexingVerdict;
import io.oxmq.oxdoc.model.IndexedDocumentRecord;
import io.oxmq.oxdoc.model.ScanOrigin;
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
 * PostgreSQL Entity persisting indexed document records.
 */
@Entity
@Table(
        name = "oxdoc_indexed_documents",
        indexes = {
                @Index(name = "idx_doc_file_id", columnList = "fileId", unique = true),
                @Index(name = "idx_doc_access_scope", columnList = "accessScope"),
                @Index(name = "idx_doc_indexing_verdict", columnList = "indexingVerdict"),
                @Index(name = "idx_doc_scan_origin", columnList = "scanOrigin"),
                @Index(name = "idx_doc_indexed_at", columnList = "indexedAtTimestamp")
        }
)
public class IndexedDocumentEntity {

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
    private AccessScope accessScope;

    private boolean hasUniquePermissions;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private IndexingVerdict indexingVerdict;

    @Column(length = 2048)
    private String analysisDetails;

    private String sha256;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ScanOrigin scanOrigin;

    @Column(nullable = false)
    private long indexedAtTimestamp;

    public IndexedDocumentEntity() {}

    public static IndexedDocumentEntity fromRecord(IndexedDocumentRecord record) {
        IndexedDocumentEntity entity = new IndexedDocumentEntity();
        entity.setFileId(record.fileId());
        entity.setDriveId(record.driveId());
        entity.setTenantId(record.tenantId());
        entity.setFileName(record.fileName());
        entity.setFilePath(record.filePath());
        entity.setFileSize(record.fileSize());
        entity.setMimeType(record.mimeType());
        entity.setAccessScope(record.accessScope());
        entity.setHasUniquePermissions(record.hasUniquePermissions());
        entity.setIndexingVerdict(record.indexingVerdict());
        entity.setAnalysisDetails(record.analysisDetails());
        entity.setSha256(record.sha256());
        entity.setScanOrigin(record.scanOrigin());
        entity.setIndexedAtTimestamp(record.indexedAtTimestamp());
        return entity;
    }

    public IndexedDocumentRecord toRecord() {
        return new IndexedDocumentRecord(
                fileId,
                driveId,
                tenantId,
                fileName,
                filePath,
                fileSize,
                mimeType,
                accessScope,
                hasUniquePermissions,
                indexingVerdict,
                analysisDetails,
                sha256,
                scanOrigin,
                indexedAtTimestamp
        );
    }

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
    public AccessScope getAccessScope() { return accessScope; }
    public void setAccessScope(AccessScope accessScope) { this.accessScope = accessScope; }
    public boolean isHasUniquePermissions() { return hasUniquePermissions; }
    public void setHasUniquePermissions(boolean hasUniquePermissions) { this.hasUniquePermissions = hasUniquePermissions; }
    public IndexingVerdict getIndexingVerdict() { return indexingVerdict; }
    public void setIndexingVerdict(IndexingVerdict indexingVerdict) { this.indexingVerdict = indexingVerdict; }
    public String getAnalysisDetails() { return analysisDetails; }
    public void setAnalysisDetails(String analysisDetails) { this.analysisDetails = analysisDetails; }
    public String getSha256() { return sha256; }
    public void setSha256(String sha256) { this.sha256 = sha256; }
    public ScanOrigin getScanOrigin() { return scanOrigin; }
    public void setScanOrigin(ScanOrigin scanOrigin) { this.scanOrigin = scanOrigin; }
    public long getIndexedAtTimestamp() { return indexedAtTimestamp; }
    public void setIndexedAtTimestamp(long indexedAtTimestamp) { this.indexedAtTimestamp = indexedAtTimestamp; }
}
