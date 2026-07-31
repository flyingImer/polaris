/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.extension.io;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.function.Function;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.BaseMetaStoreManager;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.StorageCredentialVendor;
import org.apache.polaris.core.storage.StorageCredentialVendorFactory;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.AwsStorageCredentialVendor;
import org.apache.polaris.core.storage.aws.StsClientProvider;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * CDI-selectable {@link StorageCredentialVendorFactory} for S3-backed storage configurations.
 * Registered under {@code @Identifier("s3")}, matching {@link
 * PolarisStorageConfigurationInfo.StorageType#S3}'s lower-cased name -- the string key {@link
 * org.apache.polaris.core.storage.CredentialVendingCoordinator} derives from a resolved entity's
 * storage type to pick this bean out of the {@code @Any Instance<StorageCredentialVendorFactory>}.
 */
@ApplicationScoped
@Identifier("s3")
public class AwsStorageCredentialVendorFactory implements StorageCredentialVendorFactory {

  private final PolarisDiagnostics diagnostics;
  private final Function<AwsStorageConfigurationInfo, AwsStorageCredentialVendor> vendorFactory;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  public AwsStorageCredentialVendorFactory(
      StorageCredentialVendorConfig storageCredentialVendorConfig,
      StsClientProvider stsClientProvider,
      RealmConfig realmConfig,
      StorageCredentialCache cache,
      PolarisDiagnostics diagnostics) {
    this.diagnostics = diagnostics;
    this.vendorFactory =
        storageConfig ->
            new AwsStorageCredentialVendor(
                stsClientProvider,
                config -> {
                  if (realmConfig.getConfig(
                      FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME)) {
                    return Optional.of(
                        storageCredentialVendorConfig.stsCredentials(config.getStorageName()));
                  }
                  return Optional.of(storageCredentialVendorConfig.stsCredentials());
                },
                cache,
                storageConfig,
                realmConfig);
  }

  /**
   * Test constructor -- bypasses {@link StorageCredentialVendorConfig} with a fixed credentials
   * result.
   */
  public AwsStorageCredentialVendorFactory(
      StsClientProvider stsClientProvider,
      Optional<AwsCredentialsProvider> stsCredentials,
      StorageCredentialCache cache,
      RealmConfig realmConfig,
      PolarisDiagnostics diagnostics) {
    this.diagnostics = diagnostics;
    this.vendorFactory =
        storageConfig ->
            new AwsStorageCredentialVendor(
                stsClientProvider, config -> stsCredentials, cache, storageConfig, realmConfig);
  }

  @Override
  public StorageCredentialVendor createVendor(PolarisEntity resolvedStorageEntity) {
    PolarisStorageConfigurationInfo storageConfig =
        BaseMetaStoreManager.extractStorageConfiguration(diagnostics, resolvedStorageEntity);
    return vendorFactory.apply((AwsStorageConfigurationInfo) storageConfig);
  }
}
