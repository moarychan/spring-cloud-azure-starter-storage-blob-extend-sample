// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.spring.extend.sample.storage.resource.extend;

import com.azure.spring.extend.cloud.core.resource.ExtendAzureStorageBlobProtocolResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.WritableResource;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Profile("extend")
@RestController
@RequestMapping("/blob")
public class StorageAccountBlobController {

    final static Logger logger = LoggerFactory.getLogger(StorageAccountBlobController.class);
    private final ExtendAzureStorageBlobProtocolResolver protocolResolver;
    private final ResourceLoader resourceLoader;

    public StorageAccountBlobController(ResourceLoader resourceLoader,
                                        ExtendAzureStorageBlobProtocolResolver protocolResolver) {
        this.resourceLoader = resourceLoader;
        this.protocolResolver = protocolResolver;
    }

    /**
     * Using AzureStorageBlobProtocolResolver to get Azure Storage Blob resources with file pattern.
     *
     * @return fileNames in the container match pattern: *.txt
     */
    @GetMapping("/first")
    public List<String> listTxtFiles() throws IOException {
        Resource[] resources = protocolResolver.getResources("azure-blob-firstaccount://second-container/*.txt");
        logger.info("{} resources founded with pattern:*.txt", resources.length);
        return Stream.of(resources).map(Resource::getFilename).collect(Collectors.toList());
    }

    @GetMapping("/first/{fileName}")
    public String readBlobResource(@PathVariable("fileName") String fileName) throws IOException {
        // get a BlobResource
        Resource storageBlobResource = resourceLoader.getResource("azure-blob-firstaccount://second-container/" + fileName);
        return StreamUtils.copyToString(
            storageBlobResource.getInputStream(),
            Charset.defaultCharset());
    }

    @PostMapping("/first/{fileName}")
    public String writeBlobResource(@PathVariable("fileName") String fileName,
                                    @RequestBody String data) throws IOException {
        // get a BlobResource
        Resource storageBlobResource = protocolResolver.getResource("azure-blob-firstaccount://second-container/" + fileName);
        try (OutputStream os = ((WritableResource) storageBlobResource).getOutputStream()) {
            os.write(data.getBytes());
        }
        return "blob was updated";
    }

    @GetMapping("/second")
    public List<String> secondListTxtFiles() throws IOException {
        Resource[] resources = protocolResolver.getResources("azure-blob-secondaccountdev://second-container/*.txt");
        logger.info("{} resources founded with pattern:*.txt", resources.length);
        return Stream.of(resources).map(Resource::getFilename).collect(Collectors.toList());
    }

    @GetMapping("/second/{fileName}")
    public String secondReadBlobResource(@PathVariable("fileName") String fileName) throws IOException {
        // get a BlobResource
        Resource storageBlobResource = resourceLoader.getResource("azure-blob-secondaccountdev://second-container/" + fileName);
        return StreamUtils.copyToString(
            storageBlobResource.getInputStream(),
            Charset.defaultCharset());
    }

    @PostMapping("/second/{fileName}")
    public String secondWriteBlobResource(@PathVariable("fileName") String fileName, @RequestBody String data) throws IOException {
        // get a BlobResource
        Resource storageBlobResource = protocolResolver.getResource("azure-blob-secondaccountdev://second-container/" + fileName);
        try (OutputStream os = ((WritableResource) storageBlobResource).getOutputStream()) {
            os.write(data.getBytes());
        }
        return "blob was updated";
    }
}
