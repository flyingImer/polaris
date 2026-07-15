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
package org.apache.polaris.service.catalog.iceberg;

import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Clock;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.catalog.FederatedCatalogFactory;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.events.EventAttributeMap;
import org.apache.polaris.service.catalog.AccessDelegationModeResolver;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.events.PolarisEventDispatcher;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.reporting.PolarisMetricsReporter;
import org.apache.polaris.spi.durable.DurableManager;
import org.apache.polaris.spi.feature.CatalogPrefixParser;
import org.apache.polaris.spi.substrate.EntityResolver;
import org.apache.polaris.spi.substrate.PolarisAuthorizer;
import org.apache.polaris.spi.substrate.StorageIoProvider;
import org.apache.polaris.spi.substrate.TaskExecutor;

/**
 * Builds the OSS-default Iceberg catalog feature-SPI instance ({@link LocalIcebergCatalog}) per
 * request from the per-request {@code (catalogName, principal)} plus the injected substrate
 * collaborators (Issue 29).
 *
 * <p>Successor to {@code IcebergCatalogHandlerFactory}: the handler and catalog layers merged, so
 * this hands {@link IcebergCatalogAdapter} the merged feature-SPI instance directly rather than a
 * separate handler. Federated-vs-local selection is NOT done here -- it is internal to the merged
 * instance ({@code ensureBaseInitialized()}, post-authorization).
 *
 * <p>{@code @RequestScoped} so a provider can @{@code Alternative}-override it (e.g. managed) the
 * same way the handler factory was overridable.
 */
@RequestScoped
public class LocalIcebergCatalogFactory {

  @Inject PolarisDiagnostics diagnostics;
  @Inject CallContext callContext;
  @Inject CatalogPrefixParser prefixParser;
  @Inject EntityResolver entityResolver;
  @Inject PolarisAuthorizer authorizer;
  @Inject DurableManager metaStoreManager;
  @Inject TaskExecutor taskExecutor;
  @Inject StorageAccessConfigProvider storageAccessConfigProvider;
  @Inject StorageIoProvider storageIoProvider;
  @Inject PolarisEventDispatcher polarisEventDispatcher;
  @Inject PolarisEventMetadataFactory eventMetadataFactory;
  @Inject PolarisCredentialManager credentialManager;
  @Inject @Any Instance<FederatedCatalogFactory> federatedCatalogFactories;
  @Inject ReservedProperties reservedProperties;
  @Inject CatalogHandlerUtils catalogHandlerUtils;
  @Inject EventAttributeMap eventAttributeMap;
  @Inject Clock clock;
  @Inject AccessDelegationModeResolver accessDelegationModeResolver;
  @Inject PolarisMetricsReporter metricsReporter;

  public LocalIcebergCatalog create(String catalogName, PolarisPrincipal principal) {
    return new LocalIcebergCatalog(
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
        metricsReporter,
        prefixParser);
  }
}
