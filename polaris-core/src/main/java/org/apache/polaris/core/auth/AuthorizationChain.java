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
package org.apache.polaris.core.auth;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.resolver.ImmutableResolverEntityName;
import org.apache.polaris.core.persistence.resolver.ResolutionRequest;
import org.apache.polaris.core.persistence.resolver.ResolutionResult;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverEntityName;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Authorization-side composition over a {@link ResolutionResult} (ADR-0008 Decision 6).
 *
 * <p>ADR-0008 splits resolution from authorization: the {@link
 * org.apache.polaris.spi.substrate.EntityResolver} returns clean, name-addressable data ({@link
 * ResolutionResult}), and the RBAC authorization chain, prepending the reference catalog and (for
 * root-rooted operations) the root container to a resolved path so grants cascade correctly, is an
 * authorization concern that lives here rather than on the result data type. This is the same
 * ordered chain the retired {@code PolarisResolutionManifest} produced (root container, then
 * reference catalog, then the in-catalog path), moved to the authz side.
 *
 * <p>The root container is obtained through the generic top-level lookup ({@code
 * resolvedTopLevelEntity(ROOT, rootContainerName)}), which the authorizer requests via {@link
 * #buildResolutionRequest} rather than baking a root-container concept into the resolver. Both the
 * OSS RBAC authorizer and provider authorizers that resolve locally (for example Snowflake's
 * Horizon, which needs resolved entity identity for its remote decision call) compose chains
 * through this one helper so the security-critical prepend logic is not duplicated.
 */
public final class AuthorizationChain {

  private AuthorizationChain() {}

  /**
   * Build the names-only {@link ResolutionRequest} the authorizer resolves for one {@link
   * AuthorizationRequest}: every securable across the request's intents becomes a resolver path (or
   * top-level name), plus the root container as an optional top-level name so the authorization
   * chain can be rooted. The reference catalog is taken from the catalog segment carried by the
   * securables.
   *
   * <p>Securables are resolved as required; per-operation optional/required refinement (which the
   * REST handler owns today) lands when the handlers are rewired to drive this path.
   */
  public static @NonNull ResolutionRequest buildResolutionRequest(
      @NonNull AuthorizationRequest request) {
    String referenceCatalogName = null;
    Set<ResolverPath> paths = new LinkedHashSet<>();
    Set<ResolverEntityName> topLevelNames = new LinkedHashSet<>();
    for (AuthorizationIntent intent : request.intents()) {
      for (PolarisSecurable securable : securablesOf(intent)) {
        for (PathSegment segment : securable.getPathSegments()) {
          if (segment.entityType() == PolarisEntityType.CATALOG) {
            referenceCatalogName = segment.name();
          }
        }
        PathSegment leaf = securable.getLeaf();
        if (leaf.entityType().isTopLevel()) {
          topLevelNames.add(ImmutableResolverEntityName.of(leaf.entityType(), leaf.name(), false));
        } else {
          paths.add(new ResolverPath(pathNamesWithinCatalog(securable), leaf.entityType()));
        }
      }
    }
    // Root container is resolved as optional, mirroring the manifest's constructor-time
    // registration; the authz chain reads it back through resolvedTopLevelEntity(ROOT, name).
    topLevelNames.add(
        ImmutableResolverEntityName.of(
            PolarisEntityType.ROOT, PolarisEntityConstants.getRootContainerName(), true));
    return new ResolutionRequest(
        request.principal(), referenceCatalogName, List.copyOf(paths), List.copyOf(topLevelNames));
  }

  /**
   * Resolve one securable into its authorization chain, choosing top-level vs in-catalog-path
   * composition exactly as the RBAC authorizer's per-intent dispatch expects. Returns {@code null}
   * when the securable did not resolve (the caller decides whether that is a denial or a server
   * error), matching the retired manifest lookups.
   */
  public static @Nullable PolarisResolvedPathWrapper resolvedSecurable(
      @NonNull ResolutionResult resolution,
      @NonNull PolarisSecurable securable,
      boolean prependRootContainer) {
    PathSegment leaf = securable.getLeaf();
    if (leaf.entityType().isTopLevel()) {
      return resolvedTopLevel(resolution, leaf.entityType(), leaf.name());
    }
    return resolvedPath(
        resolution,
        ResolvedPathKey.of(pathNamesWithinCatalog(securable), leaf.entityType()),
        prependRootContainer);
  }

  /**
   * The authorization chain for an in-catalog path: {@code [rootContainer?, referenceCatalog,
   * ...resolvedPath]}. The reference catalog is always prepended (grants on a catalog cascade to
   * its children); the root container is prepended only for root-rooted operations. The root
   * container element may be {@code null} in the chain when it did not resolve, matching the
   * manifest's null-tolerant path composition.
   */
  public static @Nullable PolarisResolvedPathWrapper resolvedPath(
      @NonNull ResolutionResult resolution,
      @NonNull ResolvedPathKey key,
      boolean prependRootContainer) {
    List<ResolvedPolarisEntity> resolvedPath = resolution.resolvedPath(key);
    if (resolvedPath == null) {
      return null;
    }
    List<ResolvedPolarisEntity> chain = new ArrayList<>();
    if (prependRootContainer) {
      chain.add(rootContainer(resolution));
    }
    chain.add(resolution.resolvedReferenceCatalog());
    chain.addAll(resolvedPath);
    return new PolarisResolvedPathWrapper(chain);
  }

  /**
   * The authorization chain for a top-level securable (catalog, principal, principal role): {@code
   * [rootContainer?, entity]}. The root container is prepended whenever it resolved, independent of
   * the operation's rooting, matching the manifest's top-level lookup.
   */
  public static @Nullable PolarisResolvedPathWrapper resolvedTopLevel(
      @NonNull ResolutionResult resolution,
      @NonNull PolarisEntityType entityType,
      @NonNull String entityName) {
    ResolvedPolarisEntity entity = resolution.resolvedTopLevelEntity(entityType, entityName);
    if (entity == null) {
      return null;
    }
    ResolvedPolarisEntity rootContainer = rootContainer(resolution);
    return rootContainer == null
        ? new PolarisResolvedPathWrapper(List.of(entity))
        : new PolarisResolvedPathWrapper(List.of(rootContainer, entity));
  }

  /** The root container as a single-element authorization chain, for root-rooted targetless ops. */
  public static @NonNull PolarisResolvedPathWrapper rootContainerAsPath(
      @NonNull ResolutionResult resolution) {
    return new PolarisResolvedPathWrapper(List.of(rootContainer(resolution)));
  }

  /**
   * The activated grantees (principal roles and catalog roles) whose grants the authorizer matches
   * against the required privileges, taken from the resolved snapshot.
   */
  public static @NonNull Set<PolarisBaseEntity> activatedRoles(
      @NonNull ResolutionResult resolution) {
    Set<PolarisBaseEntity> activatedRoles = new LinkedHashSet<>();
    for (ResolvedPolarisEntity role : resolution.resolvedCallerPrincipalRoles()) {
      activatedRoles.add(role.getEntity());
    }
    if (resolution.resolvedCatalogRoles() != null) {
      for (ResolvedPolarisEntity role : resolution.resolvedCatalogRoles().values()) {
        activatedRoles.add(role.getEntity());
      }
    }
    return activatedRoles;
  }

  private static @Nullable ResolvedPolarisEntity rootContainer(ResolutionResult resolution) {
    return resolution.resolvedTopLevelEntity(
        PolarisEntityType.ROOT, PolarisEntityConstants.getRootContainerName());
  }

  private static List<String> pathNamesWithinCatalog(PolarisSecurable securable) {
    // Resolver path keys are scoped within the reference catalog, so the explicit catalog
    // path segment is omitted from the PolarisSecurable path before lookup.
    return securable.getPathSegments().stream()
        .filter(segment -> segment.entityType() != PolarisEntityType.CATALOG)
        .map(PathSegment::name)
        .toList();
  }

  private static List<PolarisSecurable> securablesOf(AuthorizationIntent intent) {
    if (intent instanceof TargetlessAuthorizationIntent) {
      return List.of();
    } else if (intent instanceof SingleTargetAuthorizationIntent i) {
      return List.of(i.target());
    } else if (intent instanceof RenameAuthorizationIntent i) {
      return List.of(i.from(), i.to());
    } else if (intent instanceof PolicyAttachmentAuthorizationIntent i) {
      return List.of(i.policy(), i.attachedTo());
    } else if (intent instanceof RoleAssignmentAuthorizationIntent i) {
      return List.of(i.role(), i.assignee());
    } else if (intent instanceof PrivilegeGrantAuthorizationIntent i) {
      return List.of(i.grantTarget(), i.grantee());
    } else if (intent instanceof RootPrivilegeGrantAuthorizationIntent i) {
      return List.of(i.grantee());
    }
    throw new IllegalStateException("Unsupported authorization intent: " + intent.getClass());
  }
}
