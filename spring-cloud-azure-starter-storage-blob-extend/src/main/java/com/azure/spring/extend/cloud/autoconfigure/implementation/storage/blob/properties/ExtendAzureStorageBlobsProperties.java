package com.azure.spring.extend.cloud.autoconfigure.implementation.storage.blob.properties;

import com.azure.spring.cloud.autoconfigure.implementation.storage.blob.properties.AzureStorageBlobProperties;

import java.util.ArrayList;
import java.util.List;

public class ExtendAzureStorageBlobsProperties {

    public static final String PREFIX = "spring.cloud.azure.storage.blobs";

    private boolean enabled = true;

    private final List<AzureStorageBlobProperties> configurations = new ArrayList<>();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public List<AzureStorageBlobProperties> getConfigurations() {
        return configurations;
    }
}
