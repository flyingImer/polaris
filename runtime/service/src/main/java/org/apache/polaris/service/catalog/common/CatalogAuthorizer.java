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
package org.apache.polaris.service.catalog.common;

import static org.apache.polaris.service.catalog.common.ExceptionUtils.alreadyExistsExceptionWithSameNameForTableLikeEntity;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.entityNameForSubType;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.noSuchNamespaceException;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.notFoundExceptionForTableLikeEntity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationIntent;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.PathSegment;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisSecurable;
import org.apache.polaris.core.auth.RenameAuthorizationIntent;
import org.apache.polaris.core.auth.SingleTargetAuthorizationIntent;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.EntityResolverManifestView;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolutionRequest;
import org.apache.polaris.core.persistence.resolver.ResolutionResult;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.service.types.PolicyIdentifier;
import org.apache.polaris.spi.substrate.EntityResolver;
import org.apache.polaris.spi.substrate.PolarisAuthorizer;

/**
 * The composable authorization + resolution logic for catalog operations, extracted from {@link
 * CatalogHandler} so the merged Iceberg catalog feature-SPI implementation can compose it directly
 * instead of extending a base handler class (Issue 29; ADR-0005 names-only {@code
 * authorize(AuthorizationRequest)} decision contract).
 *
 * <p>Each operation resolves its data paths (populating {@link #resolvedEntityView()}), checks
 * not-found, and drives {@link PolarisAuthorizer#authorizeOrThrow}/{@link
 * PolarisAuthorizer#authorize} against a names-only {@link AuthorizationRequest}. Unlike {@link
 * CatalogHandler}, this helper does NOT call any {@code initializeCatalog()} template hook: the
 * composing feature-SPI implementation is itself the catalog, and initializes its own state from
 * {@link #resolvedEntityView()} after authorization returns.
 *
 * <p>This is deliberately a standalone, composable object (not a base class). {@link
 * CatalogHandler} retains its own copy of this logic for {@code PolicyCatalogHandler}/{@code
 * GenericTableCatalogHandler}, which are not converted by Issue 29; their future feature-SPI
 * rebuilds are expected to compose this same helper, at which point the duplication is removed.
 */
public class CatalogAuthorizer {

  private final EntityResolver entityResolver;
  private final PolarisAuthorizer authorizer;
  private final PolarisPrincipal polarisPrincipal;
  private final String catalogName;

  // Populated by resolveForOperation during an authorize call; read by the composing feature-SPI
  // implementation (to build its catalog state and read resolved entities) via
  // resolvedEntityView().
  private PolarisResolutionManifestCatalogView resolvedEntityView = null;

  public CatalogAuthorizer(
      EntityResolver entityResolver,
      PolarisAuthorizer authorizer,
      PolarisPrincipal polarisPrincipal,
      String catalogName) {
    this.entityResolver = entityResolver;
    this.authorizer = authorizer;
    this.polarisPrincipal = polarisPrincipal;
    this.catalogName = catalogName;
  }

  /**
   * The resolved-entity view populated by the most recent authorize call, or {@code null} if no
   * authorize call has run yet. The composing feature-SPI implementation reads this to construct
   * its catalog and to read resolved entities/storage.
   */
  public PolarisResolutionManifestCatalogView resolvedEntityView() {
    return resolvedEntityView;
  }

  /**
   * Resolves {@code paths} for this operation's own data needs and populates {@link
   * #resolvedEntityView}. Independent of (and, per ADR-0008 Decision 4, a second snapshot from) the
   * resolve the authorizer performs internally inside {@code authorize(AuthorizationRequest)} to
   * reach its decision.
   */
  private ResolutionResult resolveForOperation(List<ResolverPath> paths) {
    ResolutionResult result =
        entityResolver.resolve(
            new ResolutionRequest(polarisPrincipal, catalogName, paths, List.of()));
    resolvedEntityView =
        new EntityResolverManifestView(entityResolver, polarisPrincipal, catalogName, result);
    return result;
  }

  private PolarisSecurable namespaceSecurable(Namespace namespace) {
    String[] levels = namespace.levels();
    PathSegment[] segments = new PathSegment[levels.length];
    for (int i = 0; i < levels.length; i++) {
      segments[i] = new PathSegment(PolarisEntityType.NAMESPACE, levels[i]);
    }
    return PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, catalogName), segments);
  }

  private PolarisSecurable tableLikeSecurable(TableIdentifier identifier) {
    String[] levels = identifier.namespace().levels();
    PathSegment[] segments = new PathSegment[levels.length + 1];
    for (int i = 0; i < levels.length; i++) {
      segments[i] = new PathSegment(PolarisEntityType.NAMESPACE, levels[i]);
    }
    segments[levels.length] = new PathSegment(PolarisEntityType.TABLE_LIKE, identifier.name());
    return PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, catalogName), segments);
  }

  public void authorizeBasicNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, Namespace namespace) {
    authorizeBasicNamespaceOperationOrThrow(op, namespace, null, null, null);
  }

  public void authorizeBasicNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op,
      Namespace namespace,
      List<Namespace> extraPassthroughNamespaces,
      List<TableIdentifier> extraPassthroughTableLikes,
      List<PolicyIdentifier> extraPassThroughPolicies) {
    List<ResolverPath> paths = new ArrayList<>();
    paths.add(new ResolverPath(Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE));

    if (extraPassthroughNamespaces != null) {
      for (Namespace ns : extraPassthroughNamespaces) {
        paths.add(
            new ResolverPath(
                Arrays.asList(ns.levels()), PolarisEntityType.NAMESPACE, true /* optional */));
      }
    }
    if (extraPassthroughTableLikes != null) {
      for (TableIdentifier id : extraPassthroughTableLikes) {
        paths.add(
            new ResolverPath(
                PolarisCatalogHelpers.tableIdentifierToList(id),
                PolarisEntityType.TABLE_LIKE,
                true /* optional */));
      }
    }
    if (extraPassThroughPolicies != null) {
      for (PolicyIdentifier id : extraPassThroughPolicies) {
        paths.add(
            new ResolverPath(
                PolarisCatalogHelpers.identifierToList(id.namespace(), id.name()),
                PolarisEntityType.POLICY,
                true /* optional */));
      }
    }

    resolveForOperation(paths);
    PolarisResolvedPathWrapper target =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(namespace));
    if (target == null) {
      throw noSuchNamespaceException(namespace);
    }
    authorizer.authorizeOrThrow(
        new AuthorizationRequest(
            polarisPrincipal,
            List.of(new SingleTargetAuthorizationIntent(op, namespaceSecurable(namespace)))));
  }

  public void authorizeCreateNamespaceUnderNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, Namespace namespace) {
    Namespace parentNamespace = PolarisCatalogHelpers.getParentNamespace(namespace);
    List<ResolverPath> paths = new ArrayList<>();
    paths.add(
        new ResolverPath(Arrays.asList(parentNamespace.levels()), PolarisEntityType.NAMESPACE));
    // When creating an entity under a namespace, the authz target is the parentNamespace, but we
    // must also add the actual path that will be created as an "optional" path to indicate that
    // the underlying catalog is "allowed" to check the creation path for a conflicting entity.
    paths.add(
        new ResolverPath(
            Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE, true /* optional */));

    resolveForOperation(paths);
    PolarisResolvedPathWrapper target =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(parentNamespace));
    if (target == null) {
      throw noSuchNamespaceException(parentNamespace);
    }
    authorizer.authorizeOrThrow(
        new AuthorizationRequest(
            polarisPrincipal,
            List.of(new SingleTargetAuthorizationIntent(op, namespaceSecurable(parentNamespace)))));
  }

  public void authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, TableIdentifier identifier) {
    Namespace namespace = identifier.namespace();

    List<ResolverPath> paths = new ArrayList<>();
    paths.add(new ResolverPath(Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE));
    // When creating an entity under a namespace, the authz target is the namespace, but we must
    // also add the actual path that will be created as an "optional" path to indicate that the
    // underlying catalog is "allowed" to check the creation path for a conflicting entity.
    paths.add(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(identifier),
            PolarisEntityType.TABLE_LIKE,
            true /* optional */));

    resolveForOperation(paths);
    PolarisResolvedPathWrapper target =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(namespace));
    if (target == null) {
      throw noSuchNamespaceException(namespace);
    }
    authorizer.authorizeOrThrow(
        new AuthorizationRequest(
            polarisPrincipal,
            List.of(new SingleTargetAuthorizationIntent(op, namespaceSecurable(namespace)))));
  }

  /**
   * Authorizes a register-table-with-overwrite operation. If the table already exists, {@code
   * overwriteOp} is authorized against the table entity. If the table does not exist, {@code
   * fallbackOp} is authorized against the parent namespace.
   */
  public void authorizeRegisterTableOverwriteOrThrow(
      PolarisAuthorizableOperation overwriteOp,
      PolarisAuthorizableOperation fallbackOp,
      TableIdentifier identifier) {
    Namespace namespace = identifier.namespace();
    List<ResolverPath> paths = new ArrayList<>();
    paths.add(new ResolverPath(Arrays.asList(namespace.levels()), PolarisEntityType.NAMESPACE));
    paths.add(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(identifier),
            PolarisEntityType.TABLE_LIKE,
            true /* optional */));
    resolveForOperation(paths);

    // Early check so that a caller that has table-level REGISTER_TABLE_OVERWRITE but not
    // namespace-level REGISTER_TABLE doesn't get a permission error instead of
    // "View with same name already exists"
    PolarisResolvedPathWrapper existing =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofTableLike(identifier));
    if (existing != null
        && existing.getRawLeafEntity().getSubType() == PolarisEntitySubType.ICEBERG_VIEW) {
      throw alreadyExistsExceptionWithSameNameForTableLikeEntity(
          identifier, PolarisEntitySubType.ICEBERG_VIEW);
    }

    PolarisResolvedPathWrapper tableTarget =
        resolvedEntityView.getResolvedPath(
            ResolvedPathKey.ofTableLike(identifier), PolarisEntitySubType.ICEBERG_TABLE);

    if (tableTarget != null) {
      authorizer.authorizeOrThrow(
          new AuthorizationRequest(
              polarisPrincipal,
              List.of(
                  new SingleTargetAuthorizationIntent(
                      overwriteOp, tableLikeSecurable(identifier)))));
    } else {
      PolarisResolvedPathWrapper namespaceTarget =
          resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(namespace));
      if (namespaceTarget == null) {
        throw noSuchNamespaceException(namespace);
      }
      authorizer.authorizeOrThrow(
          new AuthorizationRequest(
              polarisPrincipal,
              List.of(
                  new SingleTargetAuthorizationIntent(fallbackOp, namespaceSecurable(namespace)))));
    }
  }

  /**
   * Ensures the resolved-entity view is initialized for a table identifier. This allows checking
   * catalog-level feature flags or other resolved entities before authorization. If already
   * initialized, this is a no-op.
   */
  public void ensureResolutionManifestForTable(TableIdentifier identifier) {
    if (resolvedEntityView == null) {
      // The underlying Catalog is also allowed to fetch "fresh" versions of the target entity.
      resolveForOperation(
          List.of(
              new ResolverPath(
                  PolarisCatalogHelpers.tableIdentifierToList(identifier),
                  PolarisEntityType.TABLE_LIKE,
                  true /* optional */)));
    }
  }

  public void authorizeBasicTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op, PolarisEntitySubType subType, TableIdentifier identifier) {
    authorizeBasicTableLikeOperationsOrThrow(EnumSet.of(op), subType, identifier);
  }

  public void authorizeBasicTableLikeOperationsOrThrow(
      EnumSet<PolarisAuthorizableOperation> ops,
      PolarisEntitySubType subType,
      TableIdentifier identifier) {
    ensureResolutionManifestForTable(identifier);
    PolarisResolvedPathWrapper target =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofTableLike(identifier), subType);
    if (target == null) {
      throw notFoundExceptionForTableLikeEntity(identifier, subType);
    }

    for (PolarisAuthorizableOperation op : ops) {
      authorizer.authorizeOrThrow(
          new AuthorizationRequest(
              polarisPrincipal,
              List.of(new SingleTargetAuthorizationIntent(op, tableLikeSecurable(identifier)))));
    }
  }

  /**
   * Decision-native counterpart of {@link #authorizeBasicTableLikeOperationOrThrow}: returns the
   * allow/deny decision for a single operation instead of throwing, so callers can branch on the
   * outcome (for example, probing for a more-privileged operation) without exception-driven control
   * flow. Resolution and not-found handling match the throwing form.
   */
  public AuthorizationDecision authorizeBasicTableLikeOperation(
      PolarisAuthorizableOperation op, PolarisEntitySubType subType, TableIdentifier identifier) {
    ensureResolutionManifestForTable(identifier);
    PolarisResolvedPathWrapper target =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofTableLike(identifier), subType);
    if (target == null) {
      throw notFoundExceptionForTableLikeEntity(identifier, subType);
    }
    return authorizer.authorize(
        new AuthorizationRequest(
            polarisPrincipal,
            List.of(new SingleTargetAuthorizationIntent(op, tableLikeSecurable(identifier)))));
  }

  public void authorizeCollectionOfTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op,
      final PolarisEntitySubType subType,
      List<TableIdentifier> ids) {
    List<ResolverPath> paths = new ArrayList<>();
    ids.forEach(
        identifier ->
            paths.add(
                new ResolverPath(
                    PolarisCatalogHelpers.tableIdentifierToList(identifier),
                    PolarisEntityType.TABLE_LIKE)));
    ResolutionResult result = resolveForOperation(paths);

    // If one of the paths failed to resolve, throw exception based on the one that
    // we first failed to resolve.
    if (result.status().getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED) {
      TableIdentifier identifier =
          PolarisCatalogHelpers.listToTableIdentifier(
              result.status().getFailedToResolvePath().entityNames());
      throw notFoundExceptionForTableLikeEntity(identifier, subType);
    }

    ids.forEach(
        identifier -> {
          if (resolvedEntityView.getResolvedPath(ResolvedPathKey.ofTableLike(identifier), subType)
              == null) {
            throw notFoundExceptionForTableLikeEntity(identifier, subType);
          }
        });
    authorizer.authorizeOrThrow(
        new AuthorizationRequest(
            polarisPrincipal,
            ids.stream()
                .map(
                    identifier ->
                        (AuthorizationIntent)
                            new SingleTargetAuthorizationIntent(op, tableLikeSecurable(identifier)))
                .toList()));
  }

  public void authorizeRenameTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op,
      PolarisEntitySubType subType,
      TableIdentifier src,
      TableIdentifier dst) {
    List<ResolverPath> paths = new ArrayList<>();
    // Add src, dstParent, and dst(optional)
    paths.add(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(src), PolarisEntityType.TABLE_LIKE));
    paths.add(
        new ResolverPath(Arrays.asList(dst.namespace().levels()), PolarisEntityType.NAMESPACE));
    paths.add(
        new ResolverPath(
            PolarisCatalogHelpers.tableIdentifierToList(dst),
            PolarisEntityType.TABLE_LIKE,
            true /* optional */));
    ResolutionResult result = resolveForOperation(paths);
    if (result.status().getStatus() == ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED
        && result.status().getFailedToResolvePath().lastEntityType()
            == PolarisEntityType.NAMESPACE) {
      throw noSuchNamespaceException(dst.namespace());
    } else if (resolvedEntityView.getResolvedPath(ResolvedPathKey.ofTableLike(src), subType)
        == null) {
      throw notFoundExceptionForTableLikeEntity(src, subType);
    }

    // Normally, since we added the dst as an optional path, we'd expect it to only get resolved
    // up to its parent namespace, and for there to be no TABLE_LIKE already in the dst in which
    // case there is no conflicting entity.
    // If there is a conflicting TABLE or VIEW, dstTarget resolves to it.
    // TODO: Possibly modify the exception thrown depending on whether the caller has privileges
    // on the parent namespace.
    PolarisResolvedPathWrapper dstTarget =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofTableLike(dst));
    if (dstTarget != null) {
      PolarisEntitySubType dstLeafSubType = dstTarget.getRawLeafEntity().getSubType();
      switch (dstLeafSubType) {
        case ICEBERG_TABLE:
        case PolarisEntitySubType.ICEBERG_VIEW:
        case PolarisEntitySubType.GENERIC_TABLE:
          throw new AlreadyExistsException(
              "Cannot rename %s to %s. %s already exists",
              src, dst, entityNameForSubType(dstLeafSubType));

        default:
          break;
      }
    }

    authorizer.authorizeOrThrow(
        new AuthorizationRequest(
            polarisPrincipal,
            List.of(
                new RenameAuthorizationIntent(
                    op, tableLikeSecurable(src), namespaceSecurable(dst.namespace())))));
  }
}
