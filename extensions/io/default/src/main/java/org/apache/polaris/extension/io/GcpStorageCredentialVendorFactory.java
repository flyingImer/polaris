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

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.BaseMetaStoreManager;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.StorageCredentialVendor;
import org.apache.polaris.core.storage.StorageCredentialVendorFactory;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.core.storage.gcp.GcpStorageConfigurationInfo;
import org.apache.polaris.core.storage.gcp.GcpStorageCredentialVendor;

/**
 * CDI-selectable {@link StorageCredentialVendorFactory} for GCS-backed storage configurations.
 * Registered under {@code @Identifier("gcs")}, matching {@link
 * PolarisStorageConfigurationInfo.StorageType#GCS}'s lower-cased name -- the string key {@link
 * org.apache.polaris.core.storage.CredentialVendingCoordinator} derives from a resolved entity's
 * storage type to pick this bean out of the {@code @Any Instance<StorageCredentialVendorFactory>}.
 */
@ApplicationScoped
@Identifier("gcs")
public class GcpStorageCredentialVendorFactory implements StorageCredentialVendorFactory {

  private final PolarisDiagnostics diagnostics;
  private final Supplier<GoogleCredentials> gcpCredsProvider;
  private final HttpTransportFactory gcpTransportFactory;
  private final StorageCredentialCache cache;
  private final RealmConfig realmConfig;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  public GcpStorageCredentialVendorFactory(
      StorageCredentialVendorConfig storageCredentialVendorConfig,
      Clock clock,
      StorageCredentialCache cache,
      RealmConfig realmConfig,
      PolarisDiagnostics diagnostics) {
    this.diagnostics = diagnostics;
    this.gcpCredsProvider = storageCredentialVendorConfig.gcpCredentialsSupplier(clock);
    this.gcpTransportFactory =
        ServiceOptions.getFromServiceLoader(HttpTransportFactory.class, NetHttpTransport::new);
    this.cache = cache;
    this.realmConfig = realmConfig;
  }

  /**
   * Test constructor -- bypasses {@link StorageCredentialVendorConfig} with a fixed credentials
   * supplier.
   */
  public GcpStorageCredentialVendorFactory(
      Supplier<GoogleCredentials> gcpCredsProvider,
      StorageCredentialCache cache,
      RealmConfig realmConfig,
      PolarisDiagnostics diagnostics) {
    this.diagnostics = diagnostics;
    this.gcpCredsProvider = gcpCredsProvider;
    this.gcpTransportFactory =
        ServiceOptions.getFromServiceLoader(HttpTransportFactory.class, NetHttpTransport::new);
    this.cache = cache;
    this.realmConfig = realmConfig;
  }

  @Override
  public StorageCredentialVendor createVendor(PolarisEntity resolvedStorageEntity) {
    PolarisStorageConfigurationInfo storageConfig =
        BaseMetaStoreManager.extractStorageConfiguration(diagnostics, resolvedStorageEntity);
    return new GcpStorageCredentialVendor(
        gcpCredsProvider.get(),
        gcpTransportFactory,
        cache,
        (GcpStorageConfigurationInfo) storageConfig,
        realmConfig);
  }
}
