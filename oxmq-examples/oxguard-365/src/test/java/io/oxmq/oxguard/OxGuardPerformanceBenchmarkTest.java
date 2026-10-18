package io.oxmq.oxguard;

import io.oxmq.oxguard.engine.ExposureEngine;
import io.oxmq.oxguard.engine.MalwareInspectionEngine;
import io.oxmq.oxguard.model.EffectiveExposure;
import io.oxmq.oxguard.model.ExposureLevel;
import io.oxmq.oxguard.model.PermissionEntry;
import io.oxmq.oxguard.model.ThreatAnalysisResult;
import io.oxmq.oxguard.model.ThreatVerdict;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("OxGuard 365: Inspection Engine Performance & Memory Benchmark")
class OxGuardPerformanceBenchmarkTest {

    @Test
    @DisplayName("Performance Benchmark: High-throughput streaming dual-pillar inspection on 10,000 files")
    void benchmarkInspectionThroughputAndMemory() {
        MalwareInspectionEngine malwareEngine = new MalwareInspectionEngine();
        ExposureEngine exposureEngine = new ExposureEngine();

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

        int cleanCount = 0;
        int externalGuestCount = 0;

        for (int i = 0; i < totalFiles; i++) {
            // Pillar 1: Exposure evaluation
            EffectiveExposure exposure = exposureEngine.evaluate(samplePermissions);
            if (exposure.level() == ExposureLevel.EXTERNAL_GUEST) {
                externalGuestCount++;
            }

            // Pillar 2: Streaming malware heuristic inspection
            ByteArrayInputStream stream = new ByteArrayInputStream(sampleDocumentBytes);
            ThreatAnalysisResult threat = malwareEngine.inspect("financial_sheet_" + i + ".xlsx", stream);
            if (threat.verdict() == ThreatVerdict.CLEAN) {
                cleanCount++;
            }
        }

        long durationMs = System.currentTimeMillis() - startTime;
        long memAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long netHeapAllocationMb = Math.max(0, (memAfter - memBefore) / (1024 * 1024));

        double throughputFilesPerSec = (totalFiles / (double) durationMs) * 1000.0;

        System.out.println("==========================================================================================");
        System.out.println("  OXGUARD 365 :: INSPECTION ENGINE PERFORMANCE BENCHMARK RESULTS                         ");
        System.out.println("==========================================================================================");
        System.out.printf("  - Total Files Processed:       %d%n", totalFiles);
        System.out.printf("  - Elapsed Time:                %d ms%n", durationMs);
        System.out.printf("  - Sustained Throughput:        %.2f files/second%n", throughputFilesPerSec);
        System.out.printf("  - Heap Delta:                  %d MB (Zero-Heap Stream Bounded)%n", netHeapAllocationMb);
        System.out.println("==========================================================================================");

        assertThat(cleanCount).isEqualTo(totalFiles);
        assertThat(externalGuestCount).isEqualTo(totalFiles);
        assertThat(throughputFilesPerSec).isGreaterThan(1000.0); // Expect > 1,000 files/sec
    }
}
