package com.azure.spring.extend.cloud.autoconfigure.storage.blob;

import com.azure.spring.cloud.autoconfigure.context.AzureGlobalProperties;
import com.azure.spring.cloud.autoconfigure.implementation.properties.utils.AzureGlobalPropertiesUtils;
import com.azure.spring.cloud.autoconfigure.implementation.storage.blob.properties.AzureStorageBlobProperties;
import com.azure.spring.cloud.core.customizer.AzureServiceClientBuilderCustomizer;
import com.azure.spring.cloud.core.implementation.util.AzureSpringIdentifier;
import com.azure.spring.cloud.core.provider.connectionstring.ServiceConnectionStringProvider;
import com.azure.spring.cloud.core.provider.connectionstring.StaticConnectionStringProvider;
import com.azure.spring.cloud.core.service.AzureServiceType;
import com.azure.spring.cloud.service.implementation.storage.blob.BlobServiceClientBuilderFactory;
import com.azure.spring.extend.cloud.autoconfigure.implementation.storage.blob.properties.ExtendAzureStorageBlobsProperties;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ResolvableType;
import org.springframework.core.env.Environment;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import static com.azure.spring.extend.cloud.core.implementation.util.ExtendAzurePropertiesUtils.copyAzureCommonPropertiesIgnoreTargetNull;
import static com.azure.spring.extend.cloud.core.implementation.util.ExtendAzurePropertiesUtils.getStorageAccountName;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(value = { "spring.cloud.azure.storage.blobs.enabled"}, havingValue = "true")
public class ExtendStorageBlobsAutoConfiguration implements BeanDefinitionRegistryPostProcessor, EnvironmentAware {

    private Environment environment;

    public static final String EXTEND_STORAGE_BLOB_PROPERTIES_BEAN_NAME = "extendAzureStorageBlobsProperties";

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {

    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
//        ExtendAzureStorageBlobsProperties storageBlobsProperties = beanFactory.getBean(EXTEND_STORAGE_BLOB_PROPERTIES_BEAN_NAME,
//            ExtendAzureStorageBlobsProperties.class); // not bind data yet
        AzureGlobalProperties azureGlobalProperties =
            Binder.get(environment)
                  .bind(AzureGlobalProperties.PREFIX, AzureGlobalProperties.class)
                  .orElse(new AzureGlobalProperties());
        ExtendAzureStorageBlobsProperties blobsProperties =
            Binder.get(environment)
                  .bind(ExtendAzureStorageBlobsProperties.PREFIX, ExtendAzureStorageBlobsProperties.class)
                  .orElseThrow(() -> new IllegalArgumentException("Can not bind the azure storage blobs properties."));
        // merge properties
        for (AzureStorageBlobProperties azureStorageBlobProperties : blobsProperties.getConfigurations()) {
            AzureStorageBlobProperties transProperties = new AzureStorageBlobProperties();
            AzureGlobalPropertiesUtils.loadProperties(azureGlobalProperties, transProperties);
            copyAzureCommonPropertiesIgnoreTargetNull(transProperties, azureStorageBlobProperties);
        }

        DefaultListableBeanFactory factory = (DefaultListableBeanFactory) beanFactory;
        registryBeanExtendAzureStorageBlobsProperties(factory, blobsProperties);
        blobsProperties.getConfigurations().forEach(blobProperties -> registryBlobBeans(factory, blobProperties));
    }

    private void registryBeanExtendAzureStorageBlobsProperties(DefaultListableBeanFactory beanFactory,
                                                               ExtendAzureStorageBlobsProperties blobsProperties) {
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(ExtendAzureStorageBlobsProperties.class,
            () -> blobsProperties);
        AbstractBeanDefinition rawBeanDefinition = beanDefinitionBuilder.getRawBeanDefinition();
        beanFactory.registerBeanDefinition(EXTEND_STORAGE_BLOB_PROPERTIES_BEAN_NAME, rawBeanDefinition);
    }

    private void registryBlobBeans(DefaultListableBeanFactory beanFactory, AzureStorageBlobProperties blobProperties) {
        String accountName = getStorageAccountName(blobProperties);
        Assert.hasText(accountName, "accountName can not be null or empty.");
        registryBeanStaticConnectionStringProvider(beanFactory, blobProperties, accountName);
        registryBeanBlobServiceClientBuilderFactory(beanFactory, blobProperties, accountName);
        registryBeanBlobServiceClientBuilder(beanFactory, accountName);
        registryBeanBlobServiceClient(beanFactory, accountName);
        registryBeanBlobContainerClient(beanFactory, blobProperties, accountName);
        registryBeanBlobClient(beanFactory, blobProperties, accountName);
    }

    private void registryBeanBlobClient(DefaultListableBeanFactory beanFactory,
                                        AzureStorageBlobProperties blobProperties,
                                        String accountName) {
        if (StringUtils.hasText(blobProperties.getBlobName())) {
            BeanDefinitionBuilder beanDefinitionBuilder =
                BeanDefinitionBuilder.genericBeanDefinition(BlobClient.class,
                    () -> {
                        BlobContainerClient blobContainerClient =
                            beanFactory.getBean(accountName + BlobContainerClient.class.getSimpleName(),
                                BlobContainerClient.class);
                        return blobContainerClient.getBlobClient(blobProperties.getBlobName());
                    });
            AbstractBeanDefinition rawBeanDefinition = beanDefinitionBuilder.getRawBeanDefinition();
            beanFactory.registerBeanDefinition(
                accountName + BlobClient.class.getSimpleName(), rawBeanDefinition);
        }
    }

    private void registryBeanBlobContainerClient(DefaultListableBeanFactory beanFactory,
                                                 AzureStorageBlobProperties blobProperties,
                                                 String accountName) {
        if (StringUtils.hasText(blobProperties.getContainerName())) {
            BeanDefinitionBuilder beanDefinitionBuilder =
                BeanDefinitionBuilder.genericBeanDefinition(BlobContainerClient.class,
                    () -> {
                        BlobServiceClient blobServiceClient =
                            beanFactory.getBean(accountName + BlobServiceClient.class.getSimpleName(),
                                BlobServiceClient.class);
                        return blobServiceClient.getBlobContainerClient(blobProperties.getContainerName());
                    });
            AbstractBeanDefinition rawBeanDefinition = beanDefinitionBuilder.getRawBeanDefinition();
            beanFactory.registerBeanDefinition(
                accountName + BlobContainerClient.class.getSimpleName(), rawBeanDefinition);
        }
    }

    private void registryBeanBlobServiceClient(DefaultListableBeanFactory beanFactory,
                                               String accountName) {
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(BlobServiceClient.class,
            () -> {
                BlobServiceClientBuilder builder =
                    beanFactory.getBean(accountName + BlobServiceClientBuilder.class.getSimpleName(),
                        BlobServiceClientBuilder.class);
                return builder.buildClient();
            });
        AbstractBeanDefinition rawBeanDefinition = beanDefinitionBuilder.getRawBeanDefinition();
        beanFactory.registerBeanDefinition(
            accountName + BlobServiceClient.class.getSimpleName(), rawBeanDefinition);
    }

    private void registryBeanBlobServiceClientBuilder(DefaultListableBeanFactory beanFactory,
                                                      String accountName) {
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(BlobServiceClientBuilder.class,
            () -> {
                BlobServiceClientBuilderFactory builderFactory =
                    beanFactory.getBean(accountName + BlobServiceClientBuilderFactory.class.getSimpleName(),
                        BlobServiceClientBuilderFactory.class);
                return builderFactory.build();
            });
        AbstractBeanDefinition rawBeanDefinition = beanDefinitionBuilder.getRawBeanDefinition();
        beanFactory.registerBeanDefinition(
            accountName + BlobServiceClientBuilder.class.getSimpleName(), rawBeanDefinition);
    }

    private void registryBeanBlobServiceClientBuilderFactory(DefaultListableBeanFactory factory,
                                                             AzureStorageBlobProperties blobProperties,
                                                             String accountName) {
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(BlobServiceClientBuilderFactory.class,
            () -> {
                BlobServiceClientBuilderFactory builderFactory = new BlobServiceClientBuilderFactory(blobProperties);

                ResolvableType providerResolvableType = ResolvableType.forClassWithGenerics(StaticConnectionStringProvider.class, AzureServiceType.StorageBlob.class);
                ObjectProvider<ServiceConnectionStringProvider<AzureServiceType.StorageBlob>> connectionStringProviders = factory.getBeanProvider(providerResolvableType);

                ResolvableType customizerResolverType = ResolvableType.forClassWithGenerics(AzureServiceClientBuilderCustomizer.class, BlobServiceClientBuilder.class);
                ObjectProvider<AzureServiceClientBuilderCustomizer<BlobServiceClientBuilder>> customizers = factory.getBeanProvider(customizerResolverType);

                builderFactory.setSpringIdentifier(AzureSpringIdentifier.AZURE_SPRING_STORAGE_BLOB);
                connectionStringProviders.orderedStream().findFirst().ifPresent(builderFactory::setConnectionStringProvider);
                customizers.orderedStream().forEach(builderFactory::addBuilderCustomizer);
                return builderFactory;
            });
        AbstractBeanDefinition rawBeanDefinition = beanDefinitionBuilder.getRawBeanDefinition();
        factory.registerBeanDefinition(
            accountName + BlobServiceClientBuilderFactory.class.getSimpleName(), rawBeanDefinition);
    }

    private void registryBeanStaticConnectionStringProvider(DefaultListableBeanFactory beanFactory,
                                                            AzureStorageBlobProperties blobProperties,
                                                            String accountName) {
        if (StringUtils.hasText(blobProperties.getConnectionString())) {
            BeanDefinitionBuilder beanDefinitionBuilder =
                BeanDefinitionBuilder.genericBeanDefinition(StaticConnectionStringProvider.class,
                () -> new StaticConnectionStringProvider<>(AzureServiceType.STORAGE_BLOB,
                    blobProperties.getConnectionString()));
            AbstractBeanDefinition rawBeanDefinition = beanDefinitionBuilder.getRawBeanDefinition();
            beanFactory.registerBeanDefinition(
                accountName + StaticConnectionStringProvider.class.getSimpleName(), rawBeanDefinition);
        }
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }
}
