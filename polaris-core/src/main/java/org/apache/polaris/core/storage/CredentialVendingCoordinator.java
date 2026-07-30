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
package org.apache.polaris.core.storage;

import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.jspecify.annotations.NonNull;

/**
 * Storage-access seam: vend scoped storage credentials for a resolved entity path. Resolves the
 * {@link StorageCredentialVendor} for the given entity path (via the string-keyed {@link
 * StorageCredentialVendorFactory} registered for that entity's storage type), builds a
 * request-scoped {@link CredentialVendingContext} from the caller/realm state, and delegates to the
 * vendor to produce a {@link StorageAccessConfig} for the requested locations and actions.
 *
 * <p>This is a Substrate non-SPI contract: unlike a true SPI, it is not a pluggable extension point
 * that external implementers register with Polaris. It is the internal coordination seam between
 * the request-scoped catalog/task code that knows *which* entity path and locations need
 * credentials, and the {@link StorageCredentialVendor} implementations that know *how* to mint them
 * for a given cloud backend. {@link org.apache.polaris.core.credentials.PolarisCredentialManager}
 * is the analogous coordinator on the connection-credential side; this interface plays the same
 * role for storage credentials.
 *
 * <p>{@code callContext} and {@code principal} are passed per call rather than injected once at
 * construction, so an implementation of this interface need not itself be request-scoped -- only
 * the request-scoped state that a given call needs travels with that call.
 */
public interface CredentialVendingCoordinator {

  StorageAccessConfig getStorageAccessConfig(
      @NonNull TableIdentifier tableIdentifier,
      @NonNull Set<String> tableLocations,
      @NonNull Set<PolarisStorageActions> storageActions,
      @NonNull Optional<String> refreshCredentialsEndpoint,
      @NonNull PolarisResolvedPathWrapper resolvedPath,
      @NonNull CallContext callContext,
      @NonNull PolarisPrincipal principal);
}
