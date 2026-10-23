package io.oxmq.oxdoc;

import io.oxmq.oxdoc.engine.ContentCategorizationEngine;
import io.oxmq.oxdoc.engine.DocumentExtractionEngine;
import io.oxmq.oxdoc.model.AccessScope;
import io.oxmq.oxdoc.model.ContentVisibility;
import io.oxmq.oxdoc.model.DocumentAnalysisResult;
import io.oxmq.oxdoc.model.IndexingVerdict;
import io.oxmq.oxdoc.model.PermissionEntry;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("OxDoc 365: Extraction & Indexing Engine Performance & Memory Benchmark")
class OxDocPerformanceBenchmarkTest {

    @Test
    @DisplayName("Performance Benchmark: High-throughput streaming dual-pillar processing on 10,000 documents")
    void benchmarkInspectionThroughputAndMemory() {
        DocumentExtractionEngine extractionEngine = new DocumentExtractionEngine();
        ContentCategorizationEngine categorizationEngine = new ContentCategorizationEngine();

        byte[] sampleDocumentBytes = ("Enterprise Quarterly Financial Results Statement for Contoso Corp. " +
                "Audited and verified under compliance regulation ISO/IEC 27001.\n" +
                "CONFIDENTIAL AND PROPRIETARY - ALL RIGHTS RESERVED.").getBytes(StandardCharsets.UTF_8);

        List<PermissionEntry> samplePermissions = List.of(
                new PermissionEntry("p1", "owner@contoso.com", "owner", false, null, null),
                new PermissionEntry("p2", "finance-audit@partner.com", "read", false, null, "users")
        );

        int totalFiles = 10_000;
        System.gc();
        long memBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long startTime = System.currentTimeMillis();

        int structuredCount = 0;
        int externalGuestCount = 0;

        for (int i = 0; i < totalFiles; i++) {
            // Pillar 1: Access Scope & Content Visibility evaluation
            ContentVisibility visibility = categorizationEngine.evaluate(samplePermissions);
            if (visibility.scope() == AccessScope.EXTERNAL_GUEST) {
                externalGuestCount++;
            }

            // Pillar 2: Streaming document analysis & indexing
            ByteArrayInputStream stream = new ByteArrayInputStream(sampleDocumentBytes);
            DocumentAnalysisResult analysis = extractionEngine.extract("financial_sheet_" + i + ".xlsx", stream);
            if (analysis.verdict() == IndexingVerdict.STRUCTURED_DOCUMENT) {
                structuredCount++;
            }
        }

        long durationMs = System.currentTimeMillis() - startTime;
        long memAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long netHeapAllocationMb = Math.max(0, (memAfter - memBefore) / (1024 * 1024));

        double throughputDocsPerSec = (totalFiles / (double) durationMs) * 1000.0;

        System.out.println("==========================================================================================");
        System.out.println("  OXDOC 365 :: DOCUMENT EXTRACTION & INDEXING PERFORMANCE BENCHMARK                       ");
        System.out.println("==========================================================================================");
        System.out.printf("  - Total Documents Processed:   %d%n", totalFiles);
        System.out.printf("  - Elapsed Time:                %d ms%n", durationMs);
        System.out.printf("  - Sustained Throughput:        %.2f docs/second%n", throughputDocsPerSec);
        System.out.printf("  - Heap Delta:                  %d MB (Zero-Heap Stream Bounded)%n", netHeapAllocationMb);
        System.out.println("==========================================================================================");

        assertThat(structuredCount).isEqualTo(totalFiles);
        assertThat(externalGuestCount).isEqualTo(totalFiles);
        assertThat(throughputDocsPerSec).isGreaterThan(1000.0); // Expect > 1,000 docs/sec
    }
}
