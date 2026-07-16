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
package org.apache.polaris.service.context.catalog;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.catalog.LocalCatalogFactory;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
// FIXME: this class appears to have no remaining CDI consumer (LocalCatalogFactory has no other
// injector) now that BasePolarisIcebergCatalog.ensureBaseInitialized() builds its own local
// delegate directly via createBridgeBaseMetastoreViewCatalog(...), bypassing this factory.
import org.apache.polaris.extension.catalog.iceberg.BridgeBaseMetastoreViewCatalog;
import org.apache.polaris.spi.durable.DurableManager;
import org.apache.polaris.spi.substrate.EntityResolver;
import org.apache.polaris.spi.substrate.PolarisEventDispatcher;
import org.apache.polaris.spi.substrate.PolarisEventMetadataFactory;
import org.apache.polaris.spi.substrate.StorageAccessConfigProvider;
import org.apache.polaris.spi.substrate.StorageIoProvider;
import org.apache.polaris.spi.substrate.TaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequestScoped
public class PolarisLocalCatalogFactory implements LocalCatalogFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisLocalCatalogFactory.class);

  private final PolarisDiagnostics diagnostics;
  private final TaskExecutor taskExecutor;
  private final StorageAccessConfigProvider storageAccessConfigProvider;
  private final StorageIoProvider storageIoProvider;
  private final EntityResolver entityResolver;
  private final PolarisEventDispatcher polarisEventDispatcher;
  private final PolarisEventMetadataFactory eventMetadataFactory;
  private final DurableManager metaStoreManager;
  private final CallContext callContext;
  private final PolarisPrincipal principal;

  @Inject
  public PolarisLocalCatalogFactory(
      PolarisDiagnostics diagnostics,
      EntityResolver entityResolver,
      TaskExecutor taskExecutor,
      StorageAccessConfigProvider storageAccessConfigProvider,
      StorageIoProvider storageIoProvider,
      PolarisEventDispatcher polarisEventDispatcher,
      PolarisEventMetadataFactory eventMetadataFactory,
      DurableManager metaStoreManager,
      CallContext callContext,
      PolarisPrincipal principal) {
    this.diagnostics = diagnostics;
    this.entityResolver = entityResolver;
    this.taskExecutor = taskExecutor;
    this.storageAccessConfigProvider = storageAccessConfigProvider;
    this.storageIoProvider = storageIoProvider;
    this.polarisEventDispatcher = polarisEventDispatcher;
    this.eventMetadataFactory = eventMetadataFactory;
    this.metaStoreManager = metaStoreManager;
    this.callContext = callContext;
    this.principal = principal;
  }

  @Override
  public Catalog createCatalog(final PolarisResolutionManifestCatalogView resolvedEntityView) {
    CatalogEntity catalog = resolvedEntityView.getResolvedCatalogEntity();
    String catalogName = catalog.getName();

    String realm = callContext.getRealmContext().getRealmIdentifier();
    String catalogKey = realm + "/" + catalogName;
    LOGGER.debug("Initializing new BasePolarisCatalog for key: {}", catalogKey);

    BridgeBaseMetastoreViewCatalog catalogInstance =
        new BridgeBaseMetastoreViewCatalog(
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
            eventMetadataFactory);

    Map<String, String> catalogProperties = new HashMap<>(catalog.getPropertiesAsMap());
    String defaultBaseLocation = catalog.getBaseLocation();
    LOGGER.debug(
        "Looked up defaultBaseLocation {} for catalog {}", defaultBaseLocation, catalogKey);

    if (defaultBaseLocation == null) {
      throw new IllegalStateException(
          String.format(
              "Catalog '%s' does not have a configured warehouse location. "
                  + "Please configure a default base location for this catalog.",
              catalogKey));
    }

    catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, defaultBaseLocation);

    // TODO: The initialize properties might need to take more from CallContext and the
    // CatalogEntity.
    catalogInstance.initialize(catalogName, catalogProperties);

    return catalogInstance;
  }
}
