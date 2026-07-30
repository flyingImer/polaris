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
package org.apache.polaris.service.task;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.storage.CredentialVendingCoordinator;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.spi.substrate.StorageIoProvider;
import org.apache.polaris.storage.model.VendedClientStorageAccess;
import org.apache.polaris.storage.model.VendedServerStorageAccess;

@RequestScoped
public class TaskFileIOSupplier {
  private final StorageIoProvider storageIoProvider;
  private final CredentialVendingCoordinator accessConfigProvider;
  private final CallContext callContext;
  private final PolarisPrincipal principal;

  @Inject
  public TaskFileIOSupplier(
      StorageIoProvider storageIoProvider,
      CredentialVendingCoordinator storageAccessConfigProvider,
      CallContext callContext,
      PolarisPrincipal principal) {
    this.storageIoProvider = storageIoProvider;
    this.accessConfigProvider = storageAccessConfigProvider;
    this.callContext = callContext;
    this.principal = principal;
  }

  public FileIO apply(TaskEntity task, TableIdentifier identifier) {
    Map<String, String> internalProperties = task.getInternalPropertiesAsMap();
    Map<String, String> properties = new HashMap<>(internalProperties);

    String location = properties.get(PolarisTaskConstants.STORAGE_LOCATION);
    Set<String> locations = Set.of(location);
    Set<PolarisStorageActions> storageActions = Set.of(PolarisStorageActions.ALL);
    ResolvedPolarisEntity resolvedTaskEntity =
        new ResolvedPolarisEntity(task, List.of(), List.of());
    PolarisResolvedPathWrapper resolvedPath =
        new PolarisResolvedPathWrapper(List.of(resolvedTaskEntity));
    StorageAccessConfig cfg =
        accessConfigProvider.getStorageAccessConfig(
            identifier,
            locations,
            storageActions,
            Optional.empty(),
            resolvedPath,
            callContext,
            principal);

    String ioImpl =
        properties.getOrDefault(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.io.ResolvingFileIO");

    VendedClientStorageAccess clientView =
        new VendedClientStorageAccess(cfg.credentials(), cfg.extraProperties(), cfg.expiresAt());
    Map<String, String> internalProps = new LinkedHashMap<>(properties);
    internalProps.putAll(cfg.internalProperties());
    VendedServerStorageAccess access =
        new VendedServerStorageAccess(clientView, internalProps, ioImpl);
    return storageIoProvider.fileIoFor(access);
  }
}
