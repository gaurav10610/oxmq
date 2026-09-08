package io.cloudbridge.dto;

public class SyncRequest {
    private String source = "github:gaurav10610/ideas-notepad";
    private String destination = "dropbox:/backups/ideas-notepad";
    private int rateLimitPerSecond = 15;
    private boolean useVirtualThreads = true;

    public SyncRequest() {}

    public SyncRequest(String source, String destination, int rateLimitPerSecond, boolean useVirtualThreads) {
        this.source = source;
        this.destination = destination;
        this.rateLimitPerSecond = rateLimitPerSecond;
        this.useVirtualThreads = useVirtualThreads;
    }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    public String getDestination() { return destination; }
    public void setDestination(String destination) { this.destination = destination; }

    public int getRateLimitPerSecond() { return rateLimitPerSecond; }
    public void setRateLimitPerSecond(int rateLimitPerSecond) { this.rateLimitPerSecond = rateLimitPerSecond; }

    public boolean isUseVirtualThreads() { return useVirtualThreads; }
    public void setUseVirtualThreads(boolean useVirtualThreads) { this.useVirtualThreads = useVirtualThreads; }
}
