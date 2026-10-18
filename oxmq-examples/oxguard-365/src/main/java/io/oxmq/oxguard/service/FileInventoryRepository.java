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

    public Map<String, Object> getHierarchy() {
        List<MonitoredDrive> drives = getAllDrives();
        List<ScannedFileRecord> allFiles = getAllFiles();

        Map<String, List<ScannedFileRecord>> filesByDrive = allFiles.stream()
                .collect(Collectors.groupingBy(ScannedFileRecord::driveId));

        // If drives table is empty, synthesize from files
        if (drives.isEmpty()) {
            for (String driveId : filesByDrive.keySet()) {
                io.oxmq.oxguard.model.DriveType type = driveId.startsWith("od-") 
                        ? io.oxmq.oxguard.model.DriveType.USER_ONEDRIVE 
                        : io.oxmq.oxguard.model.DriveType.SHAREPOINT_LIBRARY;
                String name = driveId;
                if ("sp-drive-finance".equals(driveId)) name = "Finance & Operations / Financial Documents";
                else if ("sp-drive-hr".equals(driveId)) name = "Human Resources / Employee Records";
                else if ("sp-drive-legal".equals(driveId)) name = "Corporate Legal / Contracts & Compliance";
                else if ("sp-drive-it".equals(driveId)) name = "IT & Infrastructure / System Architecture";
                else if ("od-drive-ciso".equals(driveId)) name = "Alex Chen (CISO) / Personal OneDrive";
                else if ("od-drive-cfo".equals(driveId)) name = "Sarah Miller (CFO) / Personal OneDrive";
                else if ("od-drive-lead-architect".equals(driveId)) name = "Marcus Vance (Lead Architect) / Personal OneDrive";

                drives.add(new MonitoredDrive(driveId, "contoso-enterprise-tenant", type, name, "admin", null, null));
            }
        }

        List<Map<String, Object>> sharepointSites = new java.util.ArrayList<>();
        List<Map<String, Object>> oneDrives = new java.util.ArrayList<>();

        for (MonitoredDrive drive : drives) {
            List<ScannedFileRecord> driveFiles = filesByDrive.getOrDefault(drive.driveId(), java.util.Collections.emptyList());

            // Group files by folder path
            Map<String, List<ScannedFileRecord>> byFolder = new LinkedHashMap<>();
            for (ScannedFileRecord f : driveFiles) {
                String cleanPath = f.filePath();
                if (cleanPath.contains("root:")) {
                    cleanPath = cleanPath.substring(cleanPath.indexOf("root:") + 5);
                }
                int lastSlash = cleanPath.lastIndexOf('/');
                String folder = (lastSlash > 0) ? cleanPath.substring(0, lastSlash) : "/General";
                byFolder.computeIfAbsent(folder, k -> new java.util.ArrayList<>()).add(f);
            }

            List<Map<String, Object>> folderList = new java.util.ArrayList<>();
            for (Map.Entry<String, List<ScannedFileRecord>> entry : byFolder.entrySet()) {
                String folderPath = entry.getKey();
                List<ScannedFileRecord> files = entry.getValue();
                String folderName = folderPath.substring(folderPath.lastIndexOf('/') + 1);

                long folderThreats = files.stream().filter(f -> f.threatVerdict() != ThreatVerdict.CLEAN).count();
                long folderPublic = files.stream().filter(f -> f.exposureLevel() == ExposureLevel.PUBLIC_ANONYMOUS).count();

                Map<String, Object> folderNode = new LinkedHashMap<>();
                folderNode.put("folderName", folderName);
                folderNode.put("folderPath", folderPath);
                folderNode.put("totalFiles", files.size());
                folderNode.put("threatCount", folderThreats);
                folderNode.put("publicExposureCount", folderPublic);
                folderNode.put("files", files);
                folderList.add(folderNode);
            }

            long driveThreats = driveFiles.stream().filter(f -> f.threatVerdict() != ThreatVerdict.CLEAN).count();
            long drivePublic = driveFiles.stream().filter(f -> f.exposureLevel() == ExposureLevel.PUBLIC_ANONYMOUS).count();

            Map<String, Object> driveNode = new LinkedHashMap<>();
            driveNode.put("driveId", drive.driveId());
            driveNode.put("name", drive.name());
            driveNode.put("driveType", drive.driveType().name());
            driveNode.put("ownerPrincipal", drive.ownerPrincipal());
            driveNode.put("totalFiles", driveFiles.size());
            driveNode.put("threatCount", driveThreats);
            driveNode.put("publicExposureCount", drivePublic);
            driveNode.put("folders", folderList);

            if (drive.driveType() == io.oxmq.oxguard.model.DriveType.SHAREPOINT_LIBRARY) {
                String siteName = drive.name();
                String libName = "Document Library";
                if (siteName.contains("/")) {
                    String[] parts = siteName.split("/", 2);
                    siteName = parts[0].trim();
                    libName = parts[1].trim();
                }
                driveNode.put("siteName", siteName);
                driveNode.put("libraryName", libName);
                sharepointSites.add(driveNode);
            } else {
                driveNode.put("ownerName", drive.name().replace("'s OneDrive", "").replace("Personal OneDrive", "").replace("/", "").trim());
                driveNode.put("libraryName", "Personal OneDrive");
                oneDrives.add(driveNode);
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("tenantId", "contoso-enterprise-tenant");
        result.put("totalFiles", allFiles.size());
        result.put("totalThreats", allFiles.stream().filter(f -> f.threatVerdict() != ThreatVerdict.CLEAN).count());
        result.put("totalDrives", drives.size());
        result.put("sharepointSites", sharepointSites);
        result.put("oneDrives", oneDrives);
        return result;
    }

    @Transactional
    public void clear() {
        fileRepo.deleteAllInBatch();
        driveRepo.deleteAllInBatch();
    }
}
