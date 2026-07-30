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
package org.apache.polaris.service.storage;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.BaseMetaStoreManager;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.StorageCredentialVendor;
import org.apache.polaris.core.storage.StorageCredentialVendorFactory;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.AzureStorageCredentialVendor;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;

/**
 * CDI-selectable {@link StorageCredentialVendorFactory} for Azure-backed storage configurations.
 * Registered under {@code @Identifier("azure")}, matching {@link
 * PolarisStorageConfigurationInfo.StorageType#AZURE}'s lower-cased name -- the string key {@link
 * org.apache.polaris.core.storage.CredentialVendingCoordinator} derives from a resolved entity's
 * storage type to pick this bean out of the {@code @Any Instance<StorageCredentialVendorFactory>}.
 *
 * <p>Unlike AWS and GCP, Azure credential vending needs no request-scoped Azure SDK client or
 * application-level {@link StorageConfiguration}, so this factory only needs the shared cache and
 * realm config.
 */
@ApplicationScoped
@Identifier("azure")
public class AzureStorageCredentialVendorFactory implements StorageCredentialVendorFactory {

  private final StorageCredentialCache cache;
  private final RealmConfig realmConfig;
  private final PolarisDiagnostics diagnostics;

  @Inject
  public AzureStorageCredentialVendorFactory(
      StorageCredentialCache cache, RealmConfig realmConfig, PolarisDiagnostics diagnostics) {
    this.cache = cache;
    this.realmConfig = realmConfig;
    this.diagnostics = diagnostics;
  }

  @Override
  public StorageCredentialVendor createVendor(PolarisEntity resolvedStorageEntity) {
    PolarisStorageConfigurationInfo storageConfig =
        BaseMetaStoreManager.extractStorageConfiguration(diagnostics, resolvedStorageEntity);
    return new AzureStorageCredentialVendor(
        cache, (AzureStorageConfigurationInfo) storageConfig, realmConfig);
  }
}
