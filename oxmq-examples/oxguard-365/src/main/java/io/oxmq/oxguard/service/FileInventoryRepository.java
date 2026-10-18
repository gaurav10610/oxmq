package io.oxmq.oxguard.service;

import io.oxmq.oxguard.entity.MonitoredDriveEntity;
import io.oxmq.oxguard.entity.ScannedFileEntity;
import io.oxmq.oxguard.model.ExposureLevel;
import io.oxmq.oxguard.model.MonitoredDrive;
import io.oxmq.oxguard.model.ScanOrigin;
import io.oxmq.oxguard.model.ScannedFileRecord;
import io.oxmq.oxguard.model.ThreatVerdict;
import io.oxmq.oxguard.repository.SpringDataMonitoredDriveRepository;
import io.oxmq.oxguard.repository.SpringDataScannedFileRepository;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * Enterprise Repository for security-scanned files and drive inventory backed by PostgreSQL.
 */
@Repository
public class FileInventoryRepository {

    private final SpringDataScannedFileRepository fileRepo;
    private final SpringDataMonitoredDriveRepository driveRepo;

    public FileInventoryRepository(
            SpringDataScannedFileRepository fileRepo,
            SpringDataMonitoredDriveRepository driveRepo
    ) {
        this.fileRepo = fileRepo;
        this.driveRepo = driveRepo;
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

    @Transactional
    public void saveFile(ScannedFileRecord record) {
        Optional<ScannedFileEntity> existing = fileRepo.findByFileId(record.fileId());
        if (existing.isPresent()) {
            ScannedFileEntity entity = existing.get();
            entity.setFileName(record.fileName());
            entity.setFilePath(record.filePath());
            entity.setFileSize(record.fileSize());
            entity.setMimeType(record.mimeType());
            entity.setExposureLevel(record.exposureLevel());
            entity.setHasUniquePermissions(record.hasUniquePermissions());
            entity.setThreatVerdict(record.threatVerdict());
            entity.setThreatDetails(record.threatDetails());
            entity.setSha256(record.sha256());
            entity.setScanOrigin(record.scanOrigin());
            entity.setScannedAtTimestamp(record.scannedAtTimestamp());
            fileRepo.save(entity);
        } else {
            fileRepo.save(ScannedFileEntity.fromRecord(record));
        }
    }

    public List<ScannedFileRecord> getAllFiles() {
        return fileRepo.findAllByOrderByScannedAtTimestampDesc().stream()
                .map(ScannedFileEntity::toRecord)
                .collect(Collectors.toList());
    }

    public ScannedFileRecord getFile(String fileId) {
        return fileRepo.findByFileId(fileId)
                .map(ScannedFileEntity::toRecord)
                .orElse(null);
    }

    public Map<String, Object> getMetrics() {
        long total = fileRepo.count();
        long threats = fileRepo.countByThreatVerdictNot(ThreatVerdict.CLEAN);
        long fwd = fileRepo.countByScanOrigin(ScanOrigin.FORWARD);
        long bwd = fileRepo.countByScanOrigin(ScanOrigin.BACKWARD);

        long pub = fileRepo.countByExposureLevel(ExposureLevel.PUBLIC_ANONYMOUS);
        long ext = fileRepo.countByExposureLevel(ExposureLevel.EXTERNAL_GUEST);
        long org = fileRepo.countByExposureLevel(ExposureLevel.ORGANIZATION_WIDE);
        long internal = fileRepo.countByExposureLevel(ExposureLevel.INTERNAL_RESTRICTED);
        long priv = fileRepo.countByExposureLevel(ExposureLevel.PRIVATE);

        long malware = fileRepo.countByThreatVerdict(ThreatVerdict.MALWARE_DETECTED);
        long entropy = fileRepo.countByThreatVerdict(ThreatVerdict.SUSPICIOUS_ENTROPY);
        long mime = fileRepo.countByThreatVerdict(ThreatVerdict.MIME_MISMATCH);

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("totalFiles", total);
        map.put("publicExposures", pub);
        map.put("externalGuestExposures", ext);
        map.put("orgWideExposures", org);
        map.put("internalExposures", internal);
        map.put("privateExposures", priv);
        map.put("totalThreats", threats);
        map.put("malwareThreats", malware);
        map.put("suspiciousEntropyThreats", entropy);
        map.put("mimeMismatchThreats", mime);
        map.put("forwardScans", fwd);
        map.put("backwardScans", bwd);
        map.put("totalDrives", driveRepo.count());
        return map;
    }

    @Transactional
    public void clear() {
        fileRepo.deleteAllInBatch();
        driveRepo.deleteAllInBatch();
    }
}
