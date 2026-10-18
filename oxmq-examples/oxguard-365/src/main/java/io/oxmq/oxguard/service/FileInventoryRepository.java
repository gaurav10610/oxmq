package io.oxmq.oxguard.service;

import io.oxmq.oxguard.model.ExposureLevel;
import io.oxmq.oxguard.model.MonitoredDrive;
import io.oxmq.oxguard.model.ScanOrigin;
import io.oxmq.oxguard.model.ScannedFileRecord;
import io.oxmq.oxguard.model.ThreatVerdict;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Repository;

/**
 * High-performance in-memory repository for security-scanned files and drive inventory.
 */
@Repository
public class FileInventoryRepository {

    private final Map<String, ScannedFileRecord> files = new ConcurrentHashMap<>();
    private final Map<String, MonitoredDrive> drives = new ConcurrentHashMap<>();

    private final AtomicLong totalFilesScanned = new AtomicLong(0);
    private final AtomicLong publicExposures = new AtomicLong(0);
    private final AtomicLong externalGuestExposures = new AtomicLong(0);
    private final AtomicLong orgWideExposures = new AtomicLong(0);
    private final AtomicLong internalExposures = new AtomicLong(0);
    private final AtomicLong privateExposures = new AtomicLong(0);

    private final AtomicLong malwareThreats = new AtomicLong(0);
    private final AtomicLong suspiciousEntropyThreats = new AtomicLong(0);
    private final AtomicLong mimeMismatchThreats = new AtomicLong(0);

    private final AtomicLong forwardScans = new AtomicLong(0);
    private final AtomicLong backwardScans = new AtomicLong(0);

    public void saveDrive(MonitoredDrive drive) {
        drives.put(drive.driveId(), drive);
    }

    public List<MonitoredDrive> getAllDrives() {
        return new ArrayList<>(drives.values());
    }

    public void saveFile(ScannedFileRecord record) {
        ScannedFileRecord previous = files.put(record.fileId(), record);
        if (previous == null) {
            totalFilesScanned.incrementAndGet();
            if (record.scanOrigin() == ScanOrigin.FORWARD) {
                forwardScans.incrementAndGet();
            } else {
                backwardScans.incrementAndGet();
            }

            if (record.exposureLevel() == ExposureLevel.PUBLIC_ANONYMOUS) publicExposures.incrementAndGet();
            else if (record.exposureLevel() == ExposureLevel.EXTERNAL_GUEST) externalGuestExposures.incrementAndGet();
            else if (record.exposureLevel() == ExposureLevel.ORGANIZATION_WIDE) orgWideExposures.incrementAndGet();
            else if (record.exposureLevel() == ExposureLevel.INTERNAL_RESTRICTED) internalExposures.incrementAndGet();
            else if (record.exposureLevel() == ExposureLevel.PRIVATE) privateExposures.incrementAndGet();

            if (record.threatVerdict() == ThreatVerdict.MALWARE_DETECTED) malwareThreats.incrementAndGet();
            else if (record.threatVerdict() == ThreatVerdict.SUSPICIOUS_ENTROPY) suspiciousEntropyThreats.incrementAndGet();
            else if (record.threatVerdict() == ThreatVerdict.MIME_MISMATCH) mimeMismatchThreats.incrementAndGet();
        }
    }

    public List<ScannedFileRecord> getAllFiles() {
        List<ScannedFileRecord> list = new ArrayList<>(files.values());
        list.sort((a, b) -> Long.compare(b.scannedAtTimestamp(), a.scannedAtTimestamp())); // Latest first
        return list;
    }

    public ScannedFileRecord getFile(String fileId) {
        return files.get(fileId);
    }

    public Map<String, Object> getMetrics() {
        long total = totalFilesScanned.get();
        long threats = malwareThreats.get() + suspiciousEntropyThreats.get() + mimeMismatchThreats.get();
        long fwd = forwardScans.get();
        long bwd = backwardScans.get();

        Map<String, Object> map = new java.util.LinkedHashMap<>();
        map.put("totalFiles", total);
        map.put("publicExposures", publicExposures.get());
        map.put("externalGuestExposures", externalGuestExposures.get());
        map.put("orgWideExposures", orgWideExposures.get());
        map.put("internalExposures", internalExposures.get());
        map.put("privateExposures", privateExposures.get());
        map.put("totalThreats", threats);
        map.put("malwareThreats", malwareThreats.get());
        map.put("suspiciousEntropyThreats", suspiciousEntropyThreats.get());
        map.put("mimeMismatchThreats", mimeMismatchThreats.get());
        map.put("forwardScans", fwd);
        map.put("backwardScans", bwd);
        map.put("totalDrives", drives.size());
        return map;
    }

    public void clear() {
        files.clear();
        drives.clear();
        totalFilesScanned.set(0);
        publicExposures.set(0);
        externalGuestExposures.set(0);
        orgWideExposures.set(0);
        internalExposures.set(0);
        privateExposures.set(0);
        malwareThreats.set(0);
        suspiciousEntropyThreats.set(0);
        mimeMismatchThreats.set(0);
        forwardScans.set(0);
        backwardScans.set(0);
    }
}
