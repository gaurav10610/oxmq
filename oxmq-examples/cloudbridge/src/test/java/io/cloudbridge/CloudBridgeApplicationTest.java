package io.cloudbridge;

import io.cloudbridge.client.GitHubClient;
import io.cloudbridge.dto.StorageFile;
import io.cloudbridge.dto.SyncRequest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CloudBridgeApplicationTest {

    @Test
    void testGitHubClientSampleTree() {
        GitHubClient gitHubClient = new GitHubClient();
        List<StorageFile> files = gitHubClient.listFiles("github:gaurav10610/ideas-notepad");

        assertThat(files).isNotEmpty();
        assertThat(files).anyMatch(f -> f.path().equals("README.md"));
        assertThat(files).anyMatch(f -> f.path().equals("pom.xml"));
        assertThat(files).anyMatch(f -> f.path().endsWith(".tar.gz"));
    }

    @Test
    void testSyncRequestDefaults() {
        SyncRequest request = new SyncRequest();
        assertThat(request.getSource()).contains("github");
        assertThat(request.getDestination()).contains("dropbox");
        assertThat(request.getRateLimitPerSecond()).isEqualTo(15);
        assertThat(request.isUseVirtualThreads()).isTrue();
    }
}
