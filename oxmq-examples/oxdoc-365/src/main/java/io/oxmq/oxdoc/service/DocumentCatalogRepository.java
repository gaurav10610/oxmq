package io.oxmq.oxdoc.service;

import io.oxmq.oxdoc.entity.IndexedDocumentEntity;
import io.oxmq.oxdoc.entity.MonitoredDriveEntity;
import io.oxmq.oxdoc.model.AccessScope;
import io.oxmq.oxdoc.model.IndexingVerdict;
import io.oxmq.oxdoc.model.IndexedDocumentRecord;
import io.oxmq.oxdoc.model.MonitoredDrive;
import io.oxmq.oxdoc.model.ScanOrigin;
import io.oxmq.oxdoc.repository.SpringDataIndexedDocumentRepository;
import io.oxmq.oxdoc.repository.SpringDataMonitoredDriveRepository;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * Enterprise Repository for indexed documents and drive inventory backed by PostgreSQL.
 */
@Repository
public class DocumentCatalogRepository {

    private final SpringDataIndexedDocumentRepository docRepo;
    private final SpringDataMonitoredDriveRepository driveRepo;
    private final TenantSubscriptionService tenantSubscriptionService;

    public DocumentCatalogRepository(
            SpringDataIndexedDocumentRepository docRepo,
            SpringDataMonitoredDriveRepository driveRepo,
            TenantSubscriptionService tenantSubscriptionService
    ) {
        this.docRepo = docRepo;
        this.driveRepo = driveRepo;
        this.tenantSubscriptionService = tenantSubscriptionService;
    }

    @Transactional
    public void clear() {
        docRepo.deleteAll();
        driveRepo.deleteAll();
    }

    @Transactional
    public void saveDrive(MonitoredDrive drive) {
        driveRepo.save(MonitoredDriveEntity.fromModel(drive));
    }

    public List<MonitoredDrive> getAllDrives() {
        return driveRepo.findAll().stream()
                .map(MonitoredDriveEntity::toModel)
                .collect(Collectors.toList());
    }

    public Optional<MonitoredDrive> getDrive(String driveId) {
        if (driveId == null) return Optional.empty();
        return driveRepo.findById(driveId).map(MonitoredDriveEntity::toModel);
    }

    @Transactional
    public void updateDriveDeltaToken(String driveId, String newDeltaToken) {
        if (driveId == null || newDeltaToken == null) return;
        driveRepo.findById(driveId).ifPresent(entity -> {
            entity.setDeltaToken(newDeltaToken);
            driveRepo.save(entity);
        });
    }

    @Transactional
    public void saveDocument(IndexedDocumentRecord record) {
        Optional<IndexedDocumentEntity> existing = docRepo.findByFileId(record.fileId());
        if (existing.isPresent()) {
            IndexedDocumentEntity entity = existing.get();
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
            docRepo.save(entity);
        } else {
            docRepo.save(IndexedDocumentEntity.fromRecord(record));
        }
    }

    // Alias for backward compatibility
    @Transactional
    public void saveFile(IndexedDocumentRecord record) {
        saveDocument(record);
    }

    public List<IndexedDocumentRecord> getAllDocuments() {
        return docRepo.findAllByOrderByIndexedAtTimestampDesc().stream()
                .map(IndexedDocumentEntity::toRecord)
                .collect(Collectors.toList());
    }

    public List<IndexedDocumentRecord> getAllFiles() {
        return getAllDocuments();
    }

    public List<IndexedDocumentRecord> getDocumentsByTenant(String tenantId) {
        if (tenantId == null || tenantId.isBlank() || "all".equalsIgnoreCase(tenantId)) {
            return getAllDocuments();
        }
        return docRepo.findByTenantIdOrderByIndexedAtTimestampDesc(tenantId).stream()
                .map(IndexedDocumentEntity::toRecord)
                .collect(Collectors.toList());
    }

    public List<IndexedDocumentRecord> getFilesByTenant(String tenantId) {
        return getDocumentsByTenant(tenantId);
    }

    public IndexedDocumentRecord getDocument(String fileId) {
        return docRepo.findByFileId(fileId)
                .map(IndexedDocumentEntity::toRecord)
                .orElse(null);
    }

    public IndexedDocumentRecord getFile(String fileId) {
        return getDocument(fileId);
    }

    public Map<String, Object> getMetrics() {
        return getMetrics(null);
    }

    public Map<String, Object> getMetrics(String tenantId) {
        boolean filterTenant = tenantId != null && !tenantId.isBlank() && !"all".equalsIgnoreCase(tenantId);

        long total = filterTenant ? docRepo.countByTenantId(tenantId) : docRepo.count();
        long fwd = filterTenant ? docRepo.countByTenantIdAndScanOrigin(tenantId, ScanOrigin.FORWARD) : docRepo.countByScanOrigin(ScanOrigin.FORWARD);
        long bwd = filterTenant ? docRepo.countByTenantIdAndScanOrigin(tenantId, ScanOrigin.BACKWARD) : docRepo.countByScanOrigin(ScanOrigin.BACKWARD);

        long pub = filterTenant ? docRepo.countByTenantIdAndAccessScope(tenantId, AccessScope.PUBLIC_ANONYMOUS) : docRepo.countByAccessScope(AccessScope.PUBLIC_ANONYMOUS);
        long ext = filterTenant ? docRepo.countByTenantIdAndAccessScope(tenantId, AccessScope.EXTERNAL_GUEST) : docRepo.countByAccessScope(AccessScope.EXTERNAL_GUEST);
        long org = filterTenant ? docRepo.countByTenantIdAndAccessScope(tenantId, AccessScope.ORGANIZATION_WIDE) : docRepo.countByAccessScope(AccessScope.ORGANIZATION_WIDE);
        long internal = filterTenant ? docRepo.countByTenantIdAndAccessScope(tenantId, AccessScope.INTERNAL_RESTRICTED) : docRepo.countByAccessScope(AccessScope.INTERNAL_RESTRICTED);
        long priv = filterTenant ? docRepo.countByTenantIdAndAccessScope(tenantId, AccessScope.PRIVATE) : docRepo.countByAccessScope(AccessScope.PRIVATE);

        long searchable = filterTenant ? docRepo.countByTenantIdAndIndexingVerdict(tenantId, IndexingVerdict.INDEXED_SEARCHABLE) : docRepo.countByIndexingVerdict(IndexingVerdict.INDEXED_SEARCHABLE);
        long structured = filterTenant ? docRepo.countByTenantIdAndIndexingVerdict(tenantId, IndexingVerdict.STRUCTURED_DOCUMENT) : docRepo.countByIndexingVerdict(IndexingVerdict.STRUCTURED_DOCUMENT);
        long binary = filterTenant ? docRepo.countByTenantIdAndIndexingVerdict(tenantId, IndexingVerdict.BINARY_ARCHIVED) : docRepo.countByIndexingVerdict(IndexingVerdict.BINARY_ARCHIVED);
        long metadataOnly = filterTenant ? docRepo.countByTenantIdAndIndexingVerdict(tenantId, IndexingVerdict.PARSED_METADATA_ONLY) : docRepo.countByIndexingVerdict(IndexingVerdict.PARSED_METADATA_ONLY);
        long totalDrives = filterTenant ? driveRepo.countByTenantId(tenantId) : driveRepo.count();

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("tenantId", filterTenant ? tenantId : "all");
        map.put("totalFiles", total);
        map.put("totalDocuments", total);
        map.put("publicExposures", pub);
        map.put("publicKnowledgeBase", pub);
        map.put("externalGuestExposures", ext);
        map.put("orgWideExposures", org);
        map.put("internalExposures", internal);
        map.put("privateExposures", priv);

        map.put("searchableDocuments", searchable);
        map.put("structuredDocuments", structured);
        map.put("binaryDocuments", binary);
        map.put("metadataOnlyDocuments", metadataOnly);

        // Keep alias fields so dashboard metrics and tests have 100% parity
        map.put("totalThreats", structured + binary);
        map.put("malwareThreats", binary);
        map.put("entropyThreats", structured);
        map.put("mimeMismatchThreats", metadataOnly);

        map.put("forwardScans", fwd);
        map.put("backwardScans", bwd);
        map.put("totalDrives", totalDrives);

        if (filterTenant) {
            io.oxmq.oxdoc.model.TenantProfile profile = tenantSubscriptionService.getTenantProfile(tenantId);
            if (profile != null) {
                map.put("tier", profile.tier().name());
                map.put("tierDisplayName", profile.tier().getDisplayName());
                map.put("burstLimitRUsPerSec", tenantSubscriptionService.getTenantBurstLimit(tenantId));
                map.put("tierSource", profile.tierSource().name());
                map.put("isOverridePinned", profile.isOverridePinned());
            }
        }

        return map;
    }
}
