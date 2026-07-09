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
package org.apache.polaris.core.persistence.resolver;

import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.jspecify.annotations.Nullable;

/**
 * Bridges the {@link EntityResolver} SPI to the {@link PolarisResolutionManifestCatalogView}
 * catalog-implementation consumers ({@code LocalIcebergCatalog}, {@code PolicyCatalog}, {@code
 * PolarisGenericTableCatalog}, and their managed counterparts) so those classes need no changes:
 * they already depend on the view interface, not the retired {@code PolarisResolutionManifest}
 * concrete class (ADR-0008 Decision 6).
 *
 * <p>{@link #getResolvedPath} and {@link #getResolvedReferenceCatalogEntity} read the
 * request-scoped snapshot passed at construction, mirroring the manifest's
 * no-root-container-prepend reads (chain = {@code [referenceCatalog, ...path]} — root-container
 * prepending is an authorization-chain concern, not a data read, per {@link
 * org.apache.polaris.core.auth.AuthorizationChain}). {@link #getPassthroughResolvedPath} instead
 * issues a fresh {@link EntityResolver#resolve} call for just that one path, matching the retired
 * manifest's single-use-resolver semantics for passthrough reads (deliberately bypassing the
 * request-scoped snapshot for read-your-own-write after a mutation within the same request, e.g.
 * reading a namespace's properties right after creating it).
 */
public final class EntityResolverManifestView implements PolarisResolutionManifestCatalogView {
  private final EntityResolver entityResolver;
  private final PolarisPrincipal principal;
  private final @Nullable String referenceCatalogName;
  private final ResolutionResult snapshot;

  public EntityResolverManifestView(
      EntityResolver entityResolver,
      PolarisPrincipal principal,
      @Nullable String referenceCatalogName,
      ResolutionResult snapshot) {
    this.entityResolver = entityResolver;
    this.principal = principal;
    this.referenceCatalogName = referenceCatalogName;
    this.snapshot = snapshot;
  }

  @Override
  public @Nullable PolarisResolvedPathWrapper getResolvedReferenceCatalogEntity() {
    ResolvedPolarisEntity catalog = snapshot.resolvedReferenceCatalog();
    return catalog == null ? null : new PolarisResolvedPathWrapper(List.of(catalog));
  }

  @Override
  public @Nullable PolarisResolvedPathWrapper getResolvedPath(ResolvedPathKey key) {
    return resolvedPath(snapshot, key);
  }

  @Override
  public @Nullable PolarisResolvedPathWrapper getResolvedPath(
      ResolvedPathKey key, PolarisEntitySubType subType) {
    return filterSubType(getResolvedPath(key), subType);
  }

  @Override
  public @Nullable PolarisResolvedPathWrapper getPassthroughResolvedPath(ResolvedPathKey key) {
    ResolutionResult fresh =
        entityResolver.resolve(
            new ResolutionRequest(
                principal, referenceCatalogName, List.of(new ResolverPath(key, true)), List.of()));
    return resolvedPath(fresh, key);
  }

  @Override
  public @Nullable PolarisResolvedPathWrapper getPassthroughResolvedPath(
      ResolvedPathKey key, PolarisEntitySubType subType) {
    return filterSubType(getPassthroughResolvedPath(key), subType);
  }

  private @Nullable PolarisResolvedPathWrapper resolvedPath(
      ResolutionResult resolution, ResolvedPathKey key) {
    List<ResolvedPolarisEntity> resolvedPath = resolution.resolvedPath(key);
    if (resolvedPath == null) {
      return null;
    }
    // A partially-resolved optional path (some but not all levels found) is "not found", except
    // for a passthrough-facade catalog, which may legitimately only resolve as much of the parent
    // path as exists in the remote system.
    if (!isPassthroughFacade(resolution) && resolvedPath.size() != key.entityNames().size()) {
      return null;
    }
    List<ResolvedPolarisEntity> chain = new ArrayList<>();
    ResolvedPolarisEntity referenceCatalog = resolution.resolvedReferenceCatalog();
    if (referenceCatalog != null) {
      chain.add(referenceCatalog);
    }
    chain.addAll(resolvedPath);
    return new PolarisResolvedPathWrapper(chain);
  }

  private @Nullable PolarisResolvedPathWrapper filterSubType(
      @Nullable PolarisResolvedPathWrapper path, PolarisEntitySubType subType) {
    if (path == null) {
      return null;
    }
    if (!isPassthroughFacade(snapshot)
        && path.getRawLeafEntity() != null
        && subType != PolarisEntitySubType.ANY_SUBTYPE
        && path.getRawLeafEntity().getSubType() != subType) {
      return null;
    }
    return path;
  }

  private boolean isPassthroughFacade(ResolutionResult resolution) {
    ResolvedPolarisEntity catalog = resolution.resolvedReferenceCatalog();
    return catalog != null && CatalogEntity.of(catalog.getEntity()).isPassthroughFacade();
  }
}
