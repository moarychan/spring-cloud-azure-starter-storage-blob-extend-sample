// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.spring.extend.cloud.autoconfigure.storage.blob;

import com.azure.spring.extend.cloud.core.resource.ExtendAzureStorageBlobProtocolResolver;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(value = { "spring.cloud.azure.storage.blobs.enabled"}, havingValue = "true")
public class ExtendAzureStorageBlobResourceAutoConfiguration {

    public static final String EXTEND_STORAGE_BLOB_PROTOCOL_RESOLVER_BEAN_NAME_PREFIX = "extendAzureStorageBlobsProtocolResolver";

    @Bean(EXTEND_STORAGE_BLOB_PROTOCOL_RESOLVER_BEAN_NAME_PREFIX)
    @ConditionalOnMissingBean(name = EXTEND_STORAGE_BLOB_PROTOCOL_RESOLVER_BEAN_NAME_PREFIX)
    public ExtendAzureStorageBlobProtocolResolver extendAzureStorageBlobProtocolResolver() {
        return new ExtendAzureStorageBlobProtocolResolver();
    }
}
