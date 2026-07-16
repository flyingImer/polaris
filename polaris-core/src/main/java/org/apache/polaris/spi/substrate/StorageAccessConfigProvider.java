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
package org.apache.polaris.spi.substrate;

import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.jspecify.annotations.NonNull;

/**
 * Storage-access seam: vend scoped storage credentials for a resolved entity path. Resolves the
 * storage integration for the given entity path, builds a request-scoped {@link
 * org.apache.polaris.core.storage.CredentialVendingContext} from the caller/realm state, and
 * delegates to the integration to produce a {@link StorageAccessConfig} for the requested locations
 * and actions.
 */
public interface StorageAccessConfigProvider {

  StorageAccessConfig getStorageAccessConfig(
      @NonNull TableIdentifier tableIdentifier,
      @NonNull Set<String> tableLocations,
      @NonNull Set<PolarisStorageActions> storageActions,
      @NonNull Optional<String> refreshCredentialsEndpoint,
      @NonNull PolarisResolvedPathWrapper resolvedPath);
}
