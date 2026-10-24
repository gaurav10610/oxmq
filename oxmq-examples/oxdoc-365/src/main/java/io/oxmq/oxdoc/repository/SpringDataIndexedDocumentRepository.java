package io.oxmq.oxdoc.repository;

import io.oxmq.oxdoc.entity.IndexedDocumentEntity;
import io.oxmq.oxdoc.model.AccessScope;
import io.oxmq.oxdoc.model.IndexingVerdict;
import io.oxmq.oxdoc.model.ScanOrigin;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * Spring Data JPA Repository for IndexedDocumentEntity backed by PostgreSQL.
 */
@Repository
public interface SpringDataIndexedDocumentRepository extends JpaRepository<IndexedDocumentEntity, Long> {

    Optional<IndexedDocumentEntity> findByFileId(String fileId);

    List<IndexedDocumentEntity> findAllByOrderByIndexedAtTimestampDesc();

    List<IndexedDocumentEntity> findTop100ByOrderByIndexedAtTimestampDesc();

    List<IndexedDocumentEntity> findByTenantIdOrderByIndexedAtTimestampDesc(String tenantId);

    List<IndexedDocumentEntity> findTop100ByTenantIdOrderByIndexedAtTimestampDesc(String tenantId);

    long countByAccessScope(AccessScope accessScope);

    long countByIndexingVerdict(IndexingVerdict indexingVerdict);

    long countByIndexingVerdictNot(IndexingVerdict indexingVerdict);

    long countByScanOrigin(ScanOrigin scanOrigin);

    long countByTenantId(String tenantId);

    long countByTenantIdAndAccessScope(String tenantId, AccessScope accessScope);

    long countByTenantIdAndIndexingVerdict(String tenantId, IndexingVerdict indexingVerdict);

    long countByTenantIdAndIndexingVerdictNot(String tenantId, IndexingVerdict indexingVerdict);

    long countByTenantIdAndScanOrigin(String tenantId, ScanOrigin scanOrigin);
}
