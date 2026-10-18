package io.oxmq.oxguard.model;

/**
 * Inspection verdict issued by the malware heuristic scanner.
 */
public enum ThreatVerdict {
    CLEAN,
    SUSPICIOUS_ENTROPY,
    MALWARE_DETECTED,
    MIME_MISMATCH
}
