package io.oxmq.oxguard.repository;

import io.oxmq.oxguard.entity.ScannedFileEntity;
import io.oxmq.oxguard.model.ExposureLevel;
import io.oxmq.oxguard.model.ScanOrigin;
import io.oxmq.oxguard.model.ThreatVerdict;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Spring Data JPA Repository for ScannedFileEntity backed by PostgreSQL.
 */
@Repository
public interface SpringDataScannedFileRepository extends JpaRepository<ScannedFileEntity, Long> {

    Optional<ScannedFileEntity> findByFileId(String fileId);

    List<ScannedFileEntity> findAllByOrderByScannedAtTimestampDesc();

    List<ScannedFileEntity> findByTenantIdOrderByScannedAtTimestampDesc(String tenantId);

    long countByExposureLevel(ExposureLevel exposureLevel);

    long countByThreatVerdict(ThreatVerdict threatVerdict);

    long countByThreatVerdictNot(ThreatVerdict threatVerdict);

    long countByScanOrigin(ScanOrigin scanOrigin);

    long countByTenantId(String tenantId);

    long countByTenantIdAndExposureLevel(String tenantId, ExposureLevel exposureLevel);

    long countByTenantIdAndThreatVerdict(String tenantId, ThreatVerdict threatVerdict);

    long countByTenantIdAndThreatVerdictNot(String tenantId, ThreatVerdict threatVerdict);

    long countByTenantIdAndScanOrigin(String tenantId, ScanOrigin scanOrigin);
}

