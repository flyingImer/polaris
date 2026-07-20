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
package org.apache.polaris.extension.catalog.iceberg;

import jakarta.enterprise.inject.Instance;
import java.time.Clock;
import java.util.Optional;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.catalog.FederatedCatalogFactory;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.events.EventAttributeMap;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.spi.durable.DurableManager;
import org.apache.polaris.spi.feature.CatalogPrefixParser;
import org.apache.polaris.spi.feature.catalog.AccessDelegationModeResolver;
import org.apache.polaris.spi.feature.catalog.ETagPayload;
import org.apache.polaris.spi.substrate.EntityResolver;
import org.apache.polaris.spi.substrate.PolarisAuthorizer;
import org.apache.polaris.spi.substrate.PolarisEventDispatcher;
import org.apache.polaris.spi.substrate.PolarisEventMetadataFactory;
import org.apache.polaris.spi.substrate.PolarisMetricsReporter;
import org.apache.polaris.spi.substrate.ReservedProperties;
import org.apache.polaris.spi.substrate.StorageAccessConfigProvider;
import org.apache.polaris.spi.substrate.StorageIoProvider;
import org.apache.polaris.spi.substrate.TaskExecutor;

/**
 * The OSS default concrete Iceberg catalog feature-SPI implementation: {@link
 * BasePolarisIcebergCatalog} instantiated with {@code E = }{@link ETagPayload} (Issue 29 Rework S
 * Stage S3b). This is the class the OSS runtime actually constructs (via {@code
 * LocalIcebergCatalogFactory}); a provider that needs its own provider-private extension payload
 * extends {@link BasePolarisIcebergCatalog} directly with its own {@code ExtensionPayload} subtype
 * instead of this class.
 *
 * <p>Note the name: this class occupies the identifier "PolarisIcebergCatalog" that, before this
 * stage, named the Iceberg-SDK-shaped bridge (now {@link BridgeBaseMetastoreViewCatalog}, and
 * package-private). The rename is intentional -- "PolarisIcebergCatalog" now names the OSS default
 * concrete feature-SPI catalog, not the bridge.
 */
public class PolarisIcebergCatalog extends BasePolarisIcebergCatalog<ETagPayload> {

  /**
   * Mirrors {@link BasePolarisIcebergCatalog}'s legacy view-taking constructor, supplying {@link
   * ETagPayload#NONE} as the extension value.
   */
  public PolarisIcebergCatalog(
      PolarisDiagnostics diagnostics,
      EntityResolver entityResolver,
      DurableManager metaStoreManager,
      CallContext callContext,
      PolarisResolutionManifestCatalogView resolvedEntityView,
      PolarisPrincipal principal,
      TaskExecutor taskExecutor,
      StorageAccessConfigProvider storageAccessConfigProvider,
      StorageIoProvider storageIoProvider,
      PolarisEventDispatcher polarisEventDispatcher,
      PolarisEventMetadataFactory eventMetadataFactory) {
    super(
        diagnostics,
        entityResolver,
        metaStoreManager,
        callContext,
        resolvedEntityView,
        principal,
        taskExecutor,
        storageAccessConfigProvider,
        storageIoProvider,
        polarisEventDispatcher,
        eventMetadataFactory,
        ETagPayload.NONE);
  }

  /**
   * Mirrors {@link BasePolarisIcebergCatalog}'s feature-SPI constructor, supplying {@link
   * ETagPayload#NONE} as the extension value.
   */
  public PolarisIcebergCatalog(
      String catalogName,
      PolarisPrincipal principal,
      CallContext callContext,
      PolarisDiagnostics diagnostics,
      EntityResolver entityResolver,
      PolarisAuthorizer authorizer,
      DurableManager metaStoreManager,
      TaskExecutor taskExecutor,
      StorageAccessConfigProvider storageAccessConfigProvider,
      StorageIoProvider storageIoProvider,
      PolarisEventDispatcher polarisEventDispatcher,
      PolarisEventMetadataFactory eventMetadataFactory,
      PolarisCredentialManager credentialManager,
      Instance<FederatedCatalogFactory> federatedCatalogFactories,
      ReservedProperties reservedProperties,
      CatalogHandlerUtils catalogHandlerUtils,
      EventAttributeMap eventAttributeMap,
      Clock clock,
      AccessDelegationModeResolver accessDelegationModeResolver,
      PolarisMetricsReporter polarisMetricsReporter,
      CatalogPrefixParser prefixParser) {
    super(
        catalogName,
        principal,
        callContext,
        diagnostics,
        entityResolver,
        authorizer,
        metaStoreManager,
        taskExecutor,
        storageAccessConfigProvider,
        storageIoProvider,
        polarisEventDispatcher,
        eventMetadataFactory,
        credentialManager,
        federatedCatalogFactories,
        reservedProperties,
        catalogHandlerUtils,
        eventAttributeMap,
        clock,
        accessDelegationModeResolver,
        polarisMetricsReporter,
        prefixParser,
        ETagPayload.NONE);
  }

  @Override
  protected ETagPayload withEtag(Optional<String> etag) {
    return new ETagPayload(etag);
  }
}
