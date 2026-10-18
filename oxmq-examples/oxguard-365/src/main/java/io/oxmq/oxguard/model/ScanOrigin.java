package io.oxmq.oxguard.model;

public enum ScanOrigin {
    FORWARD,   // Real-time webhook event / delta update (Priority 1)
    BACKWARD   // Historical initial discovery or periodic crawl (Priority 10)
}
