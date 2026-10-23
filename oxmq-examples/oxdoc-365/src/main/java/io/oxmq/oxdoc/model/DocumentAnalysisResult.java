package io.oxmq.oxdoc.model;

/**
 * Detailed outcome of document analysis and text extraction.
 */
public record DocumentAnalysisResult(
        IndexingVerdict verdict,
        String sha256,
        double entropy,
        long wordCount,
        int readingTimeMinutes,
        String language,
        String docFormat,
        String details
) {
    public String documentType() {
        return docFormat;
    }

    public static DocumentAnalysisResult searchable(String sha256, double entropy, long wordCount, int readingTimeMinutes, String language, String docFormat, String details) {
        return new DocumentAnalysisResult(IndexingVerdict.INDEXED_SEARCHABLE, sha256, entropy, wordCount, readingTimeMinutes, language, docFormat, details);
    }

    public static DocumentAnalysisResult structured(String sha256, double entropy, long wordCount, String docFormat, String details) {
        return new DocumentAnalysisResult(IndexingVerdict.STRUCTURED_DOCUMENT, sha256, entropy, wordCount, Math.max(1, (int)(wordCount / 200)), "en", docFormat, details);
    }

    public static DocumentAnalysisResult binary(String sha256, double entropy, String docFormat, String details) {
        return new DocumentAnalysisResult(IndexingVerdict.BINARY_ARCHIVED, sha256, entropy, 0L, 0, "binary", docFormat, details);
    }

    public static DocumentAnalysisResult metadataOnly(String sha256, String docFormat, String details) {
        return new DocumentAnalysisResult(IndexingVerdict.PARSED_METADATA_ONLY, sha256, 0.0, 0L, 0, "unknown", docFormat, details);
    }
}
