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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.internal.EntityCache;
import org.apache.polaris.spi.durable.DurableManager;
import org.apache.polaris.spi.substrate.EntityResolver;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * OSS default {@link EntityResolver} (ADR-0008 Decision 1). It adapts a {@link ResolutionRequest}
 * onto the existing stateful {@link Resolver} engine: build a {@link Resolver} for the request's
 * principal and reference catalog, add the requested top-level names and paths, run {@code
 * resolveAll()}, and materialize the resolver's positional outputs into a request-addressable
 * {@link ResolutionResult}.
 *
 * <p>Keeping today's multi-pass revalidation as the default impl is exactly what ADR-0008 Decision
 * 4 sanctions ("the OSS default keeps today's multi-pass revalidation, a remote provider does one
 * consistent read"). The functional SPI is the reshape; the proven local-metastore engine is reused
 * behind it, as an internal detail of this impl (there is no {@code ResolverFactory} SPI: the
 * engine is not a provider seam). A remote provider implements {@link EntityResolver} directly
 * instead of wrapping {@link Resolver}, retiring the {@code extends Resolver} / cache-trick /
 * prefetch workarounds (ADR-0008 Decision 5).
 *
 * <p>Path addressing: {@link Resolver#getResolvedPaths()} returns resolved paths in add order, so
 * this maps them back to the {@link ResolvedPathKey} of each requested {@link ResolverPath} in the
 * same order.
 */
public class DefaultEntityResolver implements EntityResolver {

  private final PolarisDiagnostics diagnostics;
  private final PolarisCallContext callContext;
  private final DurableManager metaStoreManager;
  private final @Nullable EntityCache entityCache;

  public DefaultEntityResolver(
      @NonNull PolarisDiagnostics diagnostics,
      @NonNull PolarisCallContext callContext,
      @NonNull DurableManager metaStoreManager,
      @Nullable EntityCache entityCache) {
    this.diagnostics = Objects.requireNonNull(diagnostics, "diagnostics must be non-null");
    this.callContext = Objects.requireNonNull(callContext, "callContext must be non-null");
    this.metaStoreManager =
        Objects.requireNonNull(metaStoreManager, "metaStoreManager must be non-null");
    this.entityCache = entityCache;
  }

  /**
   * Builds the per-call {@link Resolver} engine this default impl runs behind the SPI. The engine
   * is an internal detail (not a provider seam); this is a {@code protected} factory method only so
   * unit tests can supply a mock engine to exercise the request/result translation without a
   * metastore.
   */
  protected Resolver createResolver(
      @NonNull PolarisPrincipal principal, @Nullable String referenceCatalogName) {
    return new Resolver(
        diagnostics, callContext, metaStoreManager, principal, entityCache, referenceCatalogName);
  }

  @Override
  public @NonNull ResolutionResult resolve(@NonNull ResolutionRequest request) {
    Resolver resolver = createResolver(request.principal(), request.referenceCatalogName());

    for (ResolverEntityName topLevel : request.topLevelNames()) {
      if (topLevel.optional()) {
        resolver.addOptionalEntityByName(topLevel.entityType(), topLevel.entityName());
      } else {
        resolver.addEntityByName(topLevel.entityType(), topLevel.entityName());
      }
    }

    // Preserve add order so the resolver's positional getResolvedPaths() maps back to each key.
    List<ResolvedPathKey> keysInAddOrder = new ArrayList<>(request.paths().size());
    for (ResolverPath path : request.paths()) {
      resolver.addPath(path);
      keysInAddOrder.add(path.key());
    }

    ResolverStatus status = resolver.resolveAll();
    if (status.getStatus() != ResolverStatus.StatusEnum.SUCCESS) {
      return ResolutionResult.failure(status);
    }

    Map<ResolvedPathKey, List<ResolvedPolarisEntity>> resolvedPaths = new LinkedHashMap<>();
    if (!keysInAddOrder.isEmpty()) {
      // getResolvedPaths() asserts at least one path was resolved, so only call it when we added
      // paths; it returns them in add order, aligned with keysInAddOrder.
      List<List<ResolvedPolarisEntity>> positional = resolver.getResolvedPaths();
      if (positional.size() != keysInAddOrder.size()) {
        throw new IllegalStateException(
            "Resolved path count mismatch: expected "
                + keysInAddOrder.size()
                + " got "
                + positional.size());
      }
      for (int i = 0; i < keysInAddOrder.size(); i++) {
        resolvedPaths.put(keysInAddOrder.get(i), positional.get(i));
      }
    }

    Map<ResolutionResult.TopLevelKey, ResolvedPolarisEntity> resolvedTopLevel =
        new LinkedHashMap<>();
    for (ResolverEntityName topLevel : request.topLevelNames()) {
      ResolvedPolarisEntity resolved =
          resolver.getResolvedEntity(topLevel.entityType(), topLevel.entityName());
      if (resolved != null) {
        resolvedTopLevel.put(
            new ResolutionResult.TopLevelKey(topLevel.entityType(), topLevel.entityName()),
            resolved);
      }
    }

    return new ResolutionResult(
        status,
        resolver.getResolvedCallerPrincipal(),
        resolver.getResolvedCallerPrincipalRoles(),
        resolver.getResolvedReferenceCatalog(),
        resolver.getResolvedCatalogRoles(),
        resolvedPaths,
        resolvedTopLevel);
  }
}
