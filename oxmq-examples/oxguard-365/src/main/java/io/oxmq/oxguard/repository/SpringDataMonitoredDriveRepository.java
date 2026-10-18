package io.oxmq.oxguard.repository;

import io.oxmq.oxguard.entity.MonitoredDriveEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Spring Data JPA Repository for MonitoredDriveEntity backed by PostgreSQL.
 */
@Repository
public interface SpringDataMonitoredDriveRepository extends JpaRepository<MonitoredDriveEntity, String> {
    java.util.List<MonitoredDriveEntity> findByTenantId(String tenantId);
    long countByTenantId(String tenantId);
}

