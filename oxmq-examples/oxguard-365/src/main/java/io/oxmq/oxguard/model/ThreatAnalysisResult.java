package io.oxmq.oxguard.model;

/**
 * Detailed outcome of malware and heuristic inspection.
 */
public record ThreatAnalysisResult(
        ThreatVerdict verdict,
        String sha256,
        double entropy,
        String matchedPattern,
        String details
) {
    public static ThreatAnalysisResult clean(String sha256, double entropy) {
        return new ThreatAnalysisResult(ThreatVerdict.CLEAN, sha256, entropy, null, "File is clean, entropy within normal bounds");
    }

    public static ThreatAnalysisResult malware(String sha256, String pattern, String details) {
        return new ThreatAnalysisResult(ThreatVerdict.MALWARE_DETECTED, sha256, 0.0, pattern, details);
    }

    public static ThreatAnalysisResult suspiciousEntropy(String sha256, double entropy, String details) {
        return new ThreatAnalysisResult(ThreatVerdict.SUSPICIOUS_ENTROPY, sha256, entropy, "High Entropy", details);
    }

    public static ThreatAnalysisResult mimeMismatch(String sha256, String details) {
        return new ThreatAnalysisResult(ThreatVerdict.MIME_MISMATCH, sha256, 0.0, "PE/ELF Header in Non-Executable", details);
    }
}
