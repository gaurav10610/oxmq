package io.oxmq.oxdoc.engine;

import io.oxmq.oxdoc.model.DocumentAnalysisResult;
import io.oxmq.oxdoc.model.IndexingVerdict;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("DocumentExtractionEngine: Zero-Heap Streaming Content Intelligence")
class DocumentExtractionEngineTest {

    private DocumentExtractionEngine engine;

    @BeforeEach
    void setUp() {
        engine = new DocumentExtractionEngine();
    }

    @Test
    @DisplayName("Clean document yields INDEXED_SEARCHABLE verdict and valid SHA-256")
    void testSearchableDocument() {
        byte[] content = "Normal text document containing structured enterprise knowledge and documentation.".getBytes(StandardCharsets.UTF_8);
        DocumentAnalysisResult res = engine.extract("Annual_Report.docx", new ByteArrayInputStream(content));

        assertThat(res.verdict()).isEqualTo(IndexingVerdict.INDEXED_SEARCHABLE);
        assertThat(res.sha256()).isNotNull().hasSize(64);
        assertThat(res.docFormat()).isEqualTo("DOCUMENT");
        assertThat(res.readingTimeMinutes()).isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("Structured spreadsheet yields STRUCTURED_DOCUMENT verdict")
    void testStructuredSpreadsheet() {
        byte[] content = "CostCenter,Category,Allocated,Actual\nCC-101,Engineering,500000,420000\nCC-102,Operations,300000,280000\n".getBytes(StandardCharsets.UTF_8);
        DocumentAnalysisResult res = engine.extract("Financial_Ledger.xlsx", new ByteArrayInputStream(content));

        assertThat(res.verdict()).isEqualTo(IndexingVerdict.STRUCTURED_DOCUMENT);
        assertThat(res.docFormat()).isEqualTo("SPREADSHEET");
    }

    @Test
    @DisplayName("High entropy dataset (> 7.5) triggers STRUCTURED_DOCUMENT verdict")
    void testHighEntropyDataset() {
        byte[] randomBytes = new byte[16384];
        new Random(12345).nextBytes(randomBytes); // Uniform random distribution -> H ~ 7.99

        DocumentAnalysisResult res = engine.extract("analytics_dataset.bin", new ByteArrayInputStream(randomBytes));
        assertThat(res.verdict()).isEqualTo(IndexingVerdict.STRUCTURED_DOCUMENT);
        assertThat(res.entropy()).isGreaterThan(7.5);
    }

    @Test
    @DisplayName("PE Executable 'MZ' header triggers BINARY_ARCHIVED verdict")
    void testPeBinaryAsset() {
        byte[] bytes = new byte[1024];
        bytes[0] = 0x4D; // 'M'
        bytes[1] = 0x5A; // 'Z'
        for (int i = 2; i < bytes.length; i++) {
            bytes[i] = (byte) (i % 128);
        }

        DocumentAnalysisResult res = engine.extract("system_utility.exe", new ByteArrayInputStream(bytes));
        assertThat(res.verdict()).isEqualTo(IndexingVerdict.BINARY_ARCHIVED);
        assertThat(res.docFormat()).isEqualTo("PE_BINARY");
    }

    @Test
    @DisplayName("Linux ELF binary header triggers BINARY_ARCHIVED verdict")
    void testElfBinaryAsset() {
        byte[] bytes = new byte[1024];
        bytes[0] = 0x7F;
        bytes[1] = 0x45; // 'E'
        bytes[2] = 0x4C; // 'L'
        bytes[3] = 0x46; // 'F'

        DocumentAnalysisResult res = engine.extract("system_service.bin", new ByteArrayInputStream(bytes));
        assertThat(res.verdict()).isEqualTo(IndexingVerdict.BINARY_ARCHIVED);
        assertThat(res.docFormat()).isEqualTo("ELF_BINARY");
    }
}
