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
    private final TenantSubscriptionService tenantSubscriptionService;

    public FileInventoryRepository(
            SpringDataScannedFileRepository fileRepo,
            SpringDataMonitoredDriveRepository driveRepo,
            TenantSubscriptionService tenantSubscriptionService
    ) {
        this.fileRepo = fileRepo;
        this.driveRepo = driveRepo;
        this.tenantSubscriptionService = tenantSubscriptionService;
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

    public List<ScannedFileRecord> getFilesByTenant(String tenantId) {
        if (tenantId == null || tenantId.isBlank() || "all".equalsIgnoreCase(tenantId)) {
            return getAllFiles();
        }
        return fileRepo.findByTenantIdOrderByScannedAtTimestampDesc(tenantId).stream()
                .map(ScannedFileEntity::toRecord)
                .collect(Collectors.toList());
    }

    public ScannedFileRecord getFile(String fileId) {
        return fileRepo.findByFileId(fileId)
                .map(ScannedFileEntity::toRecord)
                .orElse(null);
    }

    public Map<String, Object> getMetrics() {
        return getMetrics(null);
    }

    public Map<String, Object> getMetrics(String tenantId) {
        boolean filterTenant = tenantId != null && !tenantId.isBlank() && !"all".equalsIgnoreCase(tenantId);

        long total = filterTenant ? fileRepo.countByTenantId(tenantId) : fileRepo.count();
        long threats = filterTenant ? fileRepo.countByTenantIdAndThreatVerdictNot(tenantId, ThreatVerdict.CLEAN) : fileRepo.countByThreatVerdictNot(ThreatVerdict.CLEAN);
        long fwd = filterTenant ? fileRepo.countByTenantIdAndScanOrigin(tenantId, ScanOrigin.FORWARD) : fileRepo.countByScanOrigin(ScanOrigin.FORWARD);
        long bwd = filterTenant ? fileRepo.countByTenantIdAndScanOrigin(tenantId, ScanOrigin.BACKWARD) : fileRepo.countByScanOrigin(ScanOrigin.BACKWARD);

        long pub = filterTenant ? fileRepo.countByTenantIdAndExposureLevel(tenantId, ExposureLevel.PUBLIC_ANONYMOUS) : fileRepo.countByExposureLevel(ExposureLevel.PUBLIC_ANONYMOUS);
        long ext = filterTenant ? fileRepo.countByTenantIdAndExposureLevel(tenantId, ExposureLevel.EXTERNAL_GUEST) : fileRepo.countByExposureLevel(ExposureLevel.EXTERNAL_GUEST);
        long org = filterTenant ? fileRepo.countByTenantIdAndExposureLevel(tenantId, ExposureLevel.ORGANIZATION_WIDE) : fileRepo.countByExposureLevel(ExposureLevel.ORGANIZATION_WIDE);
        long internal = filterTenant ? fileRepo.countByTenantIdAndExposureLevel(tenantId, ExposureLevel.INTERNAL_RESTRICTED) : fileRepo.countByExposureLevel(ExposureLevel.INTERNAL_RESTRICTED);
        long priv = filterTenant ? fileRepo.countByTenantIdAndExposureLevel(tenantId, ExposureLevel.PRIVATE) : fileRepo.countByExposureLevel(ExposureLevel.PRIVATE);

        long malware = filterTenant ? fileRepo.countByTenantIdAndThreatVerdict(tenantId, ThreatVerdict.MALWARE_DETECTED) : fileRepo.countByThreatVerdict(ThreatVerdict.MALWARE_DETECTED);
        long entropy = filterTenant ? fileRepo.countByTenantIdAndThreatVerdict(tenantId, ThreatVerdict.SUSPICIOUS_ENTROPY) : fileRepo.countByThreatVerdict(ThreatVerdict.SUSPICIOUS_ENTROPY);
        long mime = filterTenant ? fileRepo.countByTenantIdAndThreatVerdict(tenantId, ThreatVerdict.MIME_MISMATCH) : fileRepo.countByThreatVerdict(ThreatVerdict.MIME_MISMATCH);
        long totalDrives = filterTenant ? driveRepo.countByTenantId(tenantId) : driveRepo.count();

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("tenantId", filterTenant ? tenantId : "all");
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
        map.put("totalDrives", totalDrives);

        // Include per-tenant file count breakdown when returning all
        if (!filterTenant) {
            Map<String, Long> perTenantCounts = new LinkedHashMap<>();
            for (io.oxmq.oxguard.model.TenantProfile profile : tenantSubscriptionService.getAllTenants()) {
                perTenantCounts.put(profile.tenantId(), fileRepo.countByTenantId(profile.tenantId()));
            }
            map.put("perTenantCounts", perTenantCounts);
        }

        return map;
    }

    public Map<String, Object> getHierarchy() {
        return getHierarchy(null);
    }

    public Map<String, Object> getHierarchy(String tenantId) {
        boolean filterTenant = tenantId != null && !tenantId.isBlank() && !"all".equalsIgnoreCase(tenantId);
        List<MonitoredDrive> drives = getAllDrives();
        List<ScannedFileRecord> allFiles = getAllFiles();

        if (filterTenant) {
            drives = drives.stream().filter(d -> tenantId.equals(d.tenantId())).toList();
            allFiles = allFiles.stream().filter(f -> tenantId.equals(f.tenantId())).toList();
        }

        Map<String, List<ScannedFileRecord>> filesByDrive = allFiles.stream()
                .collect(Collectors.groupingBy(ScannedFileRecord::driveId));

        // Group drives by tenant
        Map<String, List<MonitoredDrive>> drivesByTenant = drives.stream()
                .collect(Collectors.groupingBy(MonitoredDrive::tenantId, LinkedHashMap::new, Collectors.toList()));

        List<Map<String, Object>> tenantsList = new java.util.ArrayList<>();

        for (Map.Entry<String, List<MonitoredDrive>> tenantEntry : drivesByTenant.entrySet()) {
            String tId = tenantEntry.getKey();
            List<MonitoredDrive> tenantDrives = tenantEntry.getValue();
            io.oxmq.oxguard.model.TenantProfile profile = tenantSubscriptionService.getTenantProfile(tId);

            List<Map<String, Object>> sharepointSites = new java.util.ArrayList<>();
            List<Map<String, Object>> oneDrives = new java.util.ArrayList<>();

            long tenantTotalFiles = 0;
            long tenantTotalThreats = 0;

            for (MonitoredDrive drive : tenantDrives) {
                List<ScannedFileRecord> driveFiles = filesByDrive.getOrDefault(drive.driveId(), java.util.Collections.emptyList());
                tenantTotalFiles += driveFiles.size();

                Map<String, List<ScannedFileRecord>> byFolder = new LinkedHashMap<>();
                for (ScannedFileRecord f : driveFiles) {
                    if (f.threatVerdict() != ThreatVerdict.CLEAN) {
                        tenantTotalThreats++;
                    }
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

            Map<String, Object> tenantNode = new LinkedHashMap<>();
            tenantNode.put("tenantId", tId);
            tenantNode.put("organizationName", profile.organizationName());
            tenantNode.put("tier", profile.tier().name());
            tenantNode.put("tierDisplayName", profile.tier().getDisplayName());
            tenantNode.put("seatCount", profile.seatCount());
            tenantNode.put("burstLimitRUsPerSec", profile.burstLimitRUsPerSec());
            tenantNode.put("totalDrives", tenantDrives.size());
            tenantNode.put("totalFiles", tenantTotalFiles);
            tenantNode.put("totalThreats", tenantTotalThreats);
            tenantNode.put("sharepointSites", sharepointSites);
            tenantNode.put("oneDrives", oneDrives);
            tenantsList.add(tenantNode);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("tenantId", filterTenant ? tenantId : "all");
        result.put("totalFiles", allFiles.size());
        result.put("totalThreats", allFiles.stream().filter(f -> f.threatVerdict() != ThreatVerdict.CLEAN).count());
        result.put("totalDrives", drives.size());
        result.put("tenants", tenantsList);

        // Also provide top-level sharepointSites and oneDrives for backward compatibility
        if (filterTenant && !tenantsList.isEmpty()) {
            result.put("sharepointSites", tenantsList.get(0).get("sharepointSites"));
            result.put("oneDrives", tenantsList.get(0).get("oneDrives"));
        } else {
            List<Map<String, Object>> allSites = new java.util.ArrayList<>();
            List<Map<String, Object>> allOneDrives = new java.util.ArrayList<>();
            for (Map<String, Object> t : tenantsList) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> s = (List<Map<String, Object>>) t.get("sharepointSites");
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> o = (List<Map<String, Object>>) t.get("oneDrives");
                if (s != null) allSites.addAll(s);
                if (o != null) allOneDrives.addAll(o);
            }
            result.put("sharepointSites", allSites);
            result.put("oneDrives", allOneDrives);
        }

        return result;
    }

    @Transactional
    public void clear() {
        fileRepo.deleteAllInBatch();
        driveRepo.deleteAllInBatch();
    }
}

