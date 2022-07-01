package com.azure.spring.extend.cloud.core.resource;

import com.azure.spring.cloud.core.resource.StorageType;
import org.junit.jupiter.api.Test;

import static com.azure.spring.extend.cloud.core.resource.ExtendAzureStorageUtils.getContainerName;
import static com.azure.spring.extend.cloud.core.resource.ExtendAzureStorageUtils.getStorageAccountName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExtendAzureStorageUtilsTests {

    @Test
    void testStorageBlobProtocolAccountName() {
        assertThrows(IllegalArgumentException.class, () -> getStorageAccountName("azure-blob", StorageType.BLOB));
        assertThrows(IllegalArgumentException.class, () -> getStorageAccountName("azure-blob-test", StorageType.BLOB));
        assertEquals("test", getStorageAccountName("azure-blob-test://", StorageType.BLOB));
        assertEquals("test", getStorageAccountName("azure-blob-test://test", StorageType.BLOB));
        assertEquals("test", getStorageAccountName("azure-blob-test://test/test.txt", StorageType.BLOB));
    }

    @Test
    void testStorageBlobProtocolContainerName() {
        assertThrows(IllegalArgumentException.class, () -> getContainerName("azure-blob", StorageType.BLOB));
        assertThrows(IllegalArgumentException.class, () -> getContainerName("azure-blob-test", StorageType.BLOB));
        assertThrows(IllegalArgumentException.class, () -> getContainerName("azure-blob-test://", StorageType.BLOB));
        assertThrows(IllegalArgumentException.class, () -> getContainerName("azure-blob-test://test", StorageType.BLOB));
        assertEquals("test", getContainerName("azure-blob-test://test/test.txt", StorageType.BLOB));
    }
}
