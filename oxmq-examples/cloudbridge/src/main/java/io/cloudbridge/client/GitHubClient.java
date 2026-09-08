package io.cloudbridge.client;

import io.cloudbridge.dto.StorageFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

@Component
public class GitHubClient implements StorageClient {
    private static final Logger log = LoggerFactory.getLogger(GitHubClient.class);

    @Value("${cloudbridge.github.client-id:}")
    private String clientId;

    @Value("${cloudbridge.github.client-secret:}")
    private String clientSecret;

    @Value("${cloudbridge.github.token:}")
    private String token;

    @Override
    public String getProviderName() {
        return "github";
    }

    @Override
    public List<StorageFile> listFiles(String repository) {
        log.info("Fetching file tree for GitHub repository: {} (OAuth Client ID: {})", repository, clientId);
        List<StorageFile> files = new ArrayList<>();
        files.add(createSampleFile("README.md", "# Ideas Notepad\nHigh performance idea incubation system.", "text/markdown"));
        files.add(createSampleFile("pom.xml", "<project>\n  <modelVersion>4.0.0</modelVersion>\n</project>", "application/xml"));
        files.add(createSampleFile("src/main/java/io/oxmq/App.java", "package io.oxmq;\npublic class App { public static void main(String[] args) {} }", "text/plain"));
        files.add(createSampleFile("docs/architecture.png", "PNG_BINARY_DATA_PAYLOAD_FOR_TESTING_1234567890", "image/png"));
        files.add(createSampleFile("docs/PRD.md", "# Product Requirements Document\nDetails of the architecture.", "text/markdown"));
        files.add(createSampleFile("releases/v1.0.0-bundle.tar.gz", "RELEASE_BINARY_ARCHIVE_DATA_CHUNK_SIMULATED", "application/gzip"));
        files.add(createSampleFile("src/test/java/io/oxmq/AppTest.java", "package io.oxmq;\nclass AppTest {}", "text/plain"));
        files.add(createSampleFile(".github/workflows/ci.yml", "name: CI\non: [push]", "text/yaml"));
        return files;
    }

    @Override
    public byte[] download(String repository, String filePath) {
        log.debug("Downloading file [{}] from repository [{}]", filePath, repository);
        return ("Simulated content for " + filePath + " from " + repository).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void upload(String location, String filePath, byte[] content, String mimeType) {
        throw new UnsupportedOperationException("GitHub client is read-only in this backup pipeline");
    }

    private StorageFile createSampleFile(String path, String content, String mimeType) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        return new StorageFile(path, bytes.length, sha256(bytes), mimeType);
    }

    private String sha256(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(data);
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            return "simulated-sha256";
        }
    }
}
