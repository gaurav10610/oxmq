package io.cloudbridge.client;

import io.cloudbridge.dto.StorageFile;
import java.util.List;

public interface StorageClient {
    String getProviderName();
    List<StorageFile> listFiles(String location);
    byte[] download(String location, String filePath);
    void upload(String location, String filePath, byte[] content, String mimeType);
}
