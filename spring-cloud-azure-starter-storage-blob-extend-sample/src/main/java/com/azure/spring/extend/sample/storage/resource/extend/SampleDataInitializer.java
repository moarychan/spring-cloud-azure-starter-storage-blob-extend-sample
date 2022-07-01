package com.azure.spring.extend.sample.storage.resource.extend;

import com.azure.spring.cloud.autoconfigure.implementation.storage.blob.properties.AzureStorageBlobProperties;
import com.azure.spring.extend.cloud.autoconfigure.implementation.storage.blob.properties.ExtendAzureStorageBlobsProperties;
import com.azure.spring.extend.cloud.core.resource.ExtendAzureStorageBlobProtocolResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.OutputStream;

import static com.azure.spring.extend.cloud.core.implementation.util.ExtendAzurePropertiesUtils.getStorageAccountName;

@Profile("extend")
@Component
public class SampleDataInitializer implements CommandLineRunner {
    final static Logger logger = LoggerFactory.getLogger(SampleDataInitializer.class);

    private final ConfigurableEnvironment env;

    private final ExtendAzureStorageBlobProtocolResolver resolver;
    private final ExtendAzureStorageBlobsProperties properties;

    public SampleDataInitializer(ConfigurableEnvironment env, ExtendAzureStorageBlobProtocolResolver resolver,
                                 ExtendAzureStorageBlobsProperties properties) {
        this.env = env;
        this.resolver = resolver;
        this.properties = properties;
    }

    /**
     * This is used to initialize some data for each Azure Storage Account Blob container.
     */
    @Override
    public void run(String... args) {
        properties.getConfigurations().forEach(this::writeDataByStorageAccount);
    }

    private void writeDataByStorageAccount(AzureStorageBlobProperties blobProperties) {
        String containerName = blobProperties.getContainerName();
        if (!StringUtils.hasText(containerName) || blobProperties.getAccountName() == null) {
            return;
        }

        String accountName = getStorageAccountName(blobProperties);
        logger.info("Begin to initialize the container {} of the account {}.", containerName, accountName);
        long currentTimeMillis = System.currentTimeMillis();
        String fileName = "fileName-" + currentTimeMillis;
        String data = "data" + currentTimeMillis;
        Resource storageBlobResource = resolver.getResource("azure-blob-" + accountName + "://" + containerName +"/" + fileName + ".txt");
        try (OutputStream os = ((WritableResource) storageBlobResource).getOutputStream()) {
            os.write(data.getBytes());
            logger.info("Write data to container={}, fileName={}.txt", containerName, fileName);
        } catch (IOException e) {
            logger.error("Write data exception", e);
        }
        logger.info("End to initialize the container {} of the account {}.", containerName, accountName);
    }
}
