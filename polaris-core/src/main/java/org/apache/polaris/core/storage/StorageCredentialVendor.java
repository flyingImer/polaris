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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.jspecify.annotations.NonNull;

/**
 * SPI for a storage integration bound to a particular storage configuration. An integration vends
 * scoped storage credentials for requests against its configured backend.
 *
 * <p>Instances are constructed fresh per call by a {@link StorageCredentialVendorFactory}, which
 * {@link CredentialVendingCoordinator} selects by storage-type key for a given resolved entity
 * path. The default cloud integrations (AWS, GCP, Azure) extend {@link
 * CachingStorageCredentialVendor}, which adds in-memory caching of vended credentials. Other
 * implementations — e.g. persistence-backed credential pools — may implement this interface
 * directly without extending the caching base class.
 */
public interface StorageCredentialVendor {

  /**
   * Vend a scoped {@link StorageAccessConfig} for the given list of {@link LocationGrant}s.
   *
   * <p>The AWS and GCP implementations honor per-grant action separation: a grant of {@code (loc,
   * {WRITE})} does not cause {@code loc} to receive read or list permissions in the resulting
   * credentials. The Azure implementation cannot fully honor per-grant per-prefix separation
   * because a SAS token is monolithic at the container (or, for hierarchical ADLS, single-path)
   * level; its action flags reflect the union of requested actions across all grants.
   *
   * @param grants per-location action requests; each grant pairs a set of storage location URIs
   *     with the operations (READ, WRITE, LIST, DELETE, ALL) the credentials should permit on those
   *     locations
   * @param refreshEndpoint optional endpoint URL for clients to refresh credentials
   * @param context metadata (catalog, principal, roles, trace id, etc.) attached to the vending
   *     call — used for audit tagging and cache keying
   */
  StorageAccessConfig getStorageAccessConfig(
      @NonNull List<LocationGrant> grants,
      @NonNull Optional<String> refreshEndpoint,
      @NonNull CredentialVendingContext context);

  /**
   * Contract rule every implementer of {@link #getStorageAccessConfig} MUST honor: a {@link
   * LocationGrant} whose {@link LocationGrant#actions()} is empty means "grant read access to these
   * locations", identical to an explicit {@code Set.of(PolarisStorageActions.READ)} grant. Callers
   * construct empty-actions grants when they only need extra properties (endpoint, region,
   * path-style, etc.) resolved for a location and are not requesting scoped write, list, or delete
   * permissions.
   *
   * <p>This normalization is the vendor's responsibility, not the caller's: implementations that
   * derive per-action read/write/list location sets from a grant list (e.g. {@link
   * CachingStorageCredentialVendor#buildCacheKey}) MUST run the incoming grants through this method
   * before inspecting {@link LocationGrant#actions()}, so an empty-actions grant is never silently
   * dropped from every action bucket.
   *
   * @param grants the raw grants as received by {@link #getStorageAccessConfig}
   * @return an equivalent list of grants where every empty {@link LocationGrant#actions()} set has
   *     been replaced with {@code Set.of(PolarisStorageActions.READ)}; grants with non-empty
   *     actions are returned unchanged
   */
  static List<LocationGrant> normalizeEmptyActionsToRead(@NonNull List<LocationGrant> grants) {
    return grants.stream()
        .map(
            grant ->
                grant.actions().isEmpty()
                    ? new LocationGrant(grant.locations(), Set.of(PolarisStorageActions.READ))
                    : grant)
        .collect(Collectors.toList());
  }
}
