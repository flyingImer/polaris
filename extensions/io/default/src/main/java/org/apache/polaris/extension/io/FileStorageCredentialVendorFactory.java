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
import java.util.List;
import java.util.Optional;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.LocationGrant;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageCredentialVendor;
import org.apache.polaris.core.storage.StorageCredentialVendorFactory;
import org.jspecify.annotations.NonNull;

/**
 * CDI-selectable {@link StorageCredentialVendorFactory} for FILE-backed storage configurations.
 * Registered under {@code @Identifier("file")}, matching {@link
 * PolarisStorageConfigurationInfo.StorageType#FILE}'s lower-cased name -- the string key {@link
 * org.apache.polaris.core.storage.CredentialVendingCoordinator} derives from a resolved entity's
 * storage type to pick this bean out of the {@code @Any Instance<StorageCredentialVendorFactory>}.
 *
 * <p>FILE backends do not support credential vending, so there's no per-config state and no caching
 * to worry about: this factory needs no injected collaborators and always returns the same no-vend
 * singleton vendor.
 */
@ApplicationScoped
@Identifier("file")
public class FileStorageCredentialVendorFactory implements StorageCredentialVendorFactory {

  /**
   * Singleton integration for FILE storage. FILE backends do not support credential vending, so
   * there's no per-config state and no caching to worry about.
   */
  private static final StorageCredentialVendor FILE_VENDOR =
      new StorageCredentialVendor() {
        @Override
        public StorageAccessConfig getStorageAccessConfig(
            @NonNull List<LocationGrant> grants,
            @NonNull Optional<String> refreshEndpoint,
            @NonNull CredentialVendingContext context) {
          return StorageAccessConfig.builder().supportsCredentialVending(false).build();
        }
      };

  @Override
  public StorageCredentialVendor createVendor(PolarisEntity resolvedStorageEntity) {
    return FILE_VENDOR;
  }
}
