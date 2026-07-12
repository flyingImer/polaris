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
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.spi.substrate.EntityResolver;
import org.apache.polaris.core.persistence.resolver.EntityResolverManifestView;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolutionRequest;
import org.apache.polaris.core.persistence.resolver.ResolutionResult;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.service.types.PolicyIdentifier;
import org.apache.polaris.spi.durable.DurableManager;
import org.apache.polaris.spi.substrate.PolarisAuthorizer;
import org.immutables.value.Value;

/**
 * An ABC for catalog wrappers which provides authorize methods that should be called before a
 * request is actually forwarded to a catalog. Child types must implement `initializeCatalog` which
 * will be called after a successful authorization.
 */
public abstract class CatalogHandler {

  public abstract String catalogName();

  public abstract PolarisPrincipal polarisPrincipal();

  public abstract CallContext callContext();

  @Value.Derived
  public RealmConfig realmConfig() {
    return callContext().getRealmConfig();
  }

  @Value.Derived
  public RealmContext realmContext() {
    return callContext().getRealmContext();
  }

  public abstract DurableManager metaStoreManager();

  public abstract EntityResolver entityResolver();

  public abstract PolarisAuthorizer authorizer();

  // Initialized in the authorize methods. A thin bridge over the request-scoped ResolutionResult
  // so LocalIcebergCatalog/PolicyCatalog/PolarisGenericTableCatalog (which depend only on this view
  // interface, never the retired PolarisResolutionManifest concrete class) need no changes
  // (ADR-0008 Decision 6).
  @SuppressWarnings("immutables:incompat")
  protected PolarisResolutionManifestCatalogView resolvedEntityView = null;

  /** Initialize the catalog once authorized. Called after all `authorize...` methods. */
  protected abstract void initializeCatalog();

  /**
   * Resolves {@code paths} for this operation's own data needs and populates {@link
   * #resolvedEntityView}. Independent of (and, per ADR-0008 Decision 4, a second snapshot from) the
   * resolve the authorizer performs internally inside {@code authorize(AuthorizationRequest)} to
   * reach its decision — accepted per the forkless PoC design-fork resolution (two resolve() calls
   * per op is contract-correct; a request-scoped memoizing EntityResolver impl can collapse the
   * physical round-trips as a provider-side implementation detail, not a contract change).
   */
  protected ResolutionResult resolveForOperation(List<ResolverPath> paths) {
    ResolutionResult result =
        entityResolver()
            .resolve(new ResolutionRequest(polarisPrincipal(), catalogName(), paths, List.of()));
    resolvedEntityView =
        new EntityResolverManifestView(entityResolver(), polarisPrincipal(), catalogName(), result);
    return result;
  }

  /**
   * Names-only securable for a namespace, for the decision-native {@code
   * authorize(AuthorizationRequest)} SPI (ADR-0005 Decision 4). Existence/not-found handling stays
   * on {@link #resolvedEntityView} above; this only feeds the authorization decision.
   */
  protected PolarisSecurable namespaceSecurable(Namespace namespace) {
    String[] levels = namespace.levels();
    PathSegment[] segments = new PathSegment[levels.length];
    for (int i = 0; i < levels.length; i++) {
      segments[i] = new PathSegment(PolarisEntityType.NAMESPACE, levels[i]);
    }
    return PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, catalogName()), segments);
  }

  protected PolarisSecurable tableLikeSecurable(TableIdentifier identifier) {
    String[] levels = identifier.namespace().levels();
    PathSegment[] segments = new PathSegment[levels.length + 1];
    for (int i = 0; i < levels.length; i++) {
      segments[i] = new PathSegment(PolarisEntityType.NAMESPACE, levels[i]);
    }
    segments[levels.length] = new PathSegment(PolarisEntityType.TABLE_LIKE, identifier.name());
    return PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, catalogName()), segments);
  }

  protected PolarisSecurable catalogSecurable() {
    return PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, catalogName()));
  }

  protected void authorizeBasicNamespaceOperationOrThrow(
      PolarisAuthorizableOperation op, Namespace namespace) {
    authorizeBasicNamespaceOperationOrThrow(op, namespace, null, null, null);
  }

  protected void authorizeBasicNamespaceOperationOrThrow(
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
    authorizer()
        .authorizeOrThrow(
            new AuthorizationRequest(
                polarisPrincipal(),
                List.of(new SingleTargetAuthorizationIntent(op, namespaceSecurable(namespace)))));

    initializeCatalog();
  }

  protected void authorizeCreateNamespaceUnderNamespaceOperationOrThrow(
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
    authorizer()
        .authorizeOrThrow(
            new AuthorizationRequest(
                polarisPrincipal(),
                List.of(
                    new SingleTargetAuthorizationIntent(op, namespaceSecurable(parentNamespace)))));

    initializeCatalog();
  }

  protected void authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
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
    authorizer()
        .authorizeOrThrow(
            new AuthorizationRequest(
                polarisPrincipal(),
                List.of(new SingleTargetAuthorizationIntent(op, namespaceSecurable(namespace)))));

    initializeCatalog();
  }

  /**
   * Authorizes a register-table-with-overwrite operation. If the table already exists, {@code
   * overwriteOp} is authorized against the table entity. If the table does not exist, {@code
   * fallbackOp} is authorized against the parent namespace.
   */
  protected void authorizeRegisterTableOverwriteOrThrow(
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
      authorizer()
          .authorizeOrThrow(
              new AuthorizationRequest(
                  polarisPrincipal(),
                  List.of(
                      new SingleTargetAuthorizationIntent(
                          overwriteOp, tableLikeSecurable(identifier)))));
    } else {
      PolarisResolvedPathWrapper namespaceTarget =
          resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(namespace));
      if (namespaceTarget == null) {
        throw noSuchNamespaceException(namespace);
      }
      authorizer()
          .authorizeOrThrow(
              new AuthorizationRequest(
                  polarisPrincipal(),
                  List.of(
                      new SingleTargetAuthorizationIntent(
                          fallbackOp, namespaceSecurable(namespace)))));
    }

    initializeCatalog();
  }

  /**
   * Ensures the resolved-entity view is initialized for a table identifier. This allows checking
   * catalog-level feature flags or other resolved entities before authorization. If already
   * initialized, this is a no-op.
   */
  protected void ensureResolutionManifestForTable(TableIdentifier identifier) {
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

  protected void authorizeBasicTableLikeOperationOrThrow(
      PolarisAuthorizableOperation op, PolarisEntitySubType subType, TableIdentifier identifier) {
    authorizeBasicTableLikeOperationsOrThrow(EnumSet.of(op), subType, identifier);
  }

  protected void authorizeBasicTableLikeOperationsOrThrow(
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
      authorizer()
          .authorizeOrThrow(
              new AuthorizationRequest(
                  polarisPrincipal(),
                  List.of(
                      new SingleTargetAuthorizationIntent(op, tableLikeSecurable(identifier)))));
    }

    initializeCatalog();
  }

  /**
   * Decision-native counterpart of {@link #authorizeBasicTableLikeOperationOrThrow}: returns the
   * allow/deny decision for a single operation instead of throwing, so callers can branch on the
   * outcome (for example, probing for a more-privileged operation) without exception-driven control
   * flow. Resolution and not-found handling match the throwing form; the catalog is initialized
   * only when the decision allows the operation, mirroring the throw form's post-authz
   * initialization.
   */
  protected AuthorizationDecision authorizeBasicTableLikeOperation(
      PolarisAuthorizableOperation op, PolarisEntitySubType subType, TableIdentifier identifier) {
    ensureResolutionManifestForTable(identifier);
    PolarisResolvedPathWrapper target =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofTableLike(identifier), subType);
    if (target == null) {
      throw notFoundExceptionForTableLikeEntity(identifier, subType);
    }
    AuthorizationDecision decision =
        authorizer()
            .authorize(
                new AuthorizationRequest(
                    polarisPrincipal(),
                    List.of(
                        new SingleTargetAuthorizationIntent(op, tableLikeSecurable(identifier)))));
    if (decision.isAllowed()) {
      initializeCatalog();
    }
    return decision;
  }

  protected void authorizeCollectionOfTableLikeOperationOrThrow(
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
    authorizer()
        .authorizeOrThrow(
            new AuthorizationRequest(
                polarisPrincipal(),
                ids.stream()
                    .map(
                        identifier ->
                            (AuthorizationIntent)
                                new SingleTargetAuthorizationIntent(
                                    op, tableLikeSecurable(identifier)))
                    .toList()));

    initializeCatalog();
  }

  protected void authorizeRenameTableLikeOperationOrThrow(
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

    authorizer()
        .authorizeOrThrow(
            new AuthorizationRequest(
                polarisPrincipal(),
                List.of(
                    new RenameAuthorizationIntent(
                        op, tableLikeSecurable(src), namespaceSecurable(dst.namespace())))));

    initializeCatalog();
  }
}
