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
package org.apache.polaris.service.catalog;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.EntityResolver;
import org.apache.polaris.core.persistence.resolver.EntityResolverManifestView;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolutionRequest;
import org.apache.polaris.core.persistence.resolver.ResolutionResult;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;

/**
 * For test purposes or for elevated-privilege scenarios where entity resolution is allowed to
 * directly access the durable layer without being part of an authorization-gated resolution, this
 * view resolves each requested path on demand through the {@link EntityResolver} SPI (ADR-0008),
 * without defining a fixed set of resolved entities checked against authorizable operations. Each
 * read is an independent single-call resolution, matching the retired manifest's
 * single-use-resolver semantics.
 */
public class PolarisPassthroughResolutionView implements PolarisResolutionManifestCatalogView {
  private final EntityResolver entityResolver;
  private final PolarisPrincipal polarisPrincipal;
  private final String catalogName;

  public PolarisPassthroughResolutionView(
      EntityResolver entityResolver, PolarisPrincipal polarisPrincipal, String catalogName) {
    this.entityResolver = entityResolver;
    this.polarisPrincipal = polarisPrincipal;
    this.catalogName = catalogName;
  }

  private EntityResolverManifestView freshView(List<ResolverPath> paths) {
    ResolutionResult result =
        entityResolver.resolve(
            new ResolutionRequest(polarisPrincipal, catalogName, paths, List.of()));
    return new EntityResolverManifestView(entityResolver, polarisPrincipal, catalogName, result);
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedReferenceCatalogEntity() {
    return freshView(List.of()).getResolvedReferenceCatalogEntity();
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedPath(ResolvedPathKey key) {
    Preconditions.checkState(
        key.entityType() == PolarisEntityType.NAMESPACE,
        "Trying to getResolvedPath(key) for non-namespace key %s",
        key);
    return freshView(List.of(new ResolverPath(key.entityNames(), key.entityType())))
        .getResolvedPath(key);
  }

  @Override
  public PolarisResolvedPathWrapper getResolvedPath(
      ResolvedPathKey key, PolarisEntitySubType subType) {
    Preconditions.checkState(
        key.entityType() == PolarisEntityType.TABLE_LIKE
            || key.entityType() == PolarisEntityType.POLICY,
        "Trying to getResolvedPath(key, subType) for unsupported key %s",
        key);
    return freshView(List.of(new ResolverPath(key.entityNames(), key.entityType())))
        .getResolvedPath(key, subType);
  }

  @Override
  public PolarisResolvedPathWrapper getPassthroughResolvedPath(ResolvedPathKey key) {
    Preconditions.checkState(
        key.entityType() == PolarisEntityType.NAMESPACE,
        "Trying to getPassthroughResolvedPath(key) for non-namespace key %s",
        key);
    return freshView(List.of()).getPassthroughResolvedPath(key);
  }

  @Override
  public PolarisResolvedPathWrapper getPassthroughResolvedPath(
      ResolvedPathKey key, PolarisEntitySubType subType) {
    Preconditions.checkState(
        key.entityType() == PolarisEntityType.TABLE_LIKE
            || key.entityType() == PolarisEntityType.POLICY,
        "Trying to getPassthroughResolvedPath(key, subType) for unsupported key %s",
        key);
    return freshView(List.of()).getPassthroughResolvedPath(key, subType);
  }
}
