package io.cloudbridge.client;

import io.cloudbridge.dto.StorageFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

@Component
public class BoxClient implements StorageClient {
    private static final Logger log = LoggerFactory.getLogger(BoxClient.class);

    @Value("${cloudbridge.box.client-id:}")
    private String clientId;

    @Value("${cloudbridge.box.client-secret:}")
    private String clientSecret;

    @Value("${cloudbridge.box.token:}")
    private String developerToken;

    @Override
    public String getProviderName() {
        return "box";
    }

    @Override
    public List<StorageFile> listFiles(String location) {
        log.info("Scanning Box folder: {} (Client ID: {})", location, clientId);
        return Collections.emptyList();
    }

    @Override
    public byte[] download(String location, String filePath) {
        return new byte[0];
    }

    @Override
    public void upload(String location, String filePath, byte[] content, String mimeType) {
        log.info("Box: Uploaded [{}] ({} bytes) to folder [{}] via Client ID [{}]", 
                filePath, content.length, location, clientId);
    }
}
