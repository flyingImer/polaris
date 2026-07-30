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
package org.apache.polaris.service.catalog.iceberg;

import static org.apache.polaris.spi.feature.catalog.AccessDelegationMode.VENDED_CREDENTIALS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import java.time.Clock;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.events.EventAttributeMap;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.resolver.EntityResolverManifestView;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolutionResult;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.extension.catalog.iceberg.BridgeBaseMetastoreViewCatalog;
import org.apache.polaris.extension.catalog.iceberg.CatalogHandlerUtils;
import org.apache.polaris.extension.catalog.iceberg.PolarisIcebergCatalog;
import org.apache.polaris.spi.durable.DurableManager;
import org.apache.polaris.spi.feature.CatalogPrefixParser;
import org.apache.polaris.spi.feature.catalog.AccessDelegationModeResolver;
import org.apache.polaris.spi.substrate.EntityResolver;
import org.apache.polaris.spi.substrate.PolarisAuthorizer;
import org.apache.polaris.spi.substrate.PolarisEventDispatcher;
import org.apache.polaris.spi.substrate.PolarisEventMetadataFactory;
import org.apache.polaris.spi.substrate.PolarisMetricsReporter;
import org.apache.polaris.spi.substrate.ReservedProperties;
import org.apache.polaris.spi.substrate.StorageAccessConfigProvider;
import org.apache.polaris.spi.substrate.StorageIoProvider;
import org.apache.polaris.spi.substrate.TaskExecutor;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the credential-vending branches of the merged Iceberg catalog feature-SPI
 * implementation ({@link PolarisIcebergCatalog}), which absorbed the retired {@code
 * IcebergCatalogHandler} (Issue 29). These drive the three {@code loadCredentials} branches the
 * handler exposed:
 *
 * <ul>
 *   <li>federated/external catalog -> full-loadTable fallback (the retired handler branched on
 *       whether {@code baseCatalog} was the native local-catalog delegate (now {@link
 *       BridgeBaseMetastoreViewCatalog}); the merged class branches on {@code isFederated}, so
 *       "external (non-Polaris)" maps to a federated catalog here)
 *   <li>native catalog with the table location in entity internal properties -> optimized path (no
 *       loadTable on the delegate)
 *   <li>native catalog missing the location property -> full-loadTable fallback
 * </ul>
 *
 * <p>The merge removed the handler's mockable {@code localCatalogFactory} seam (the merged instance
 * IS the local catalog), so {@link #buildMergedCatalog} pins the post-authorization local-vs-
 * federated dispatch state (isFederated + delegate baseCatalog + resolved view) via an anonymous
 * {@code ensureBaseInitialized} override instead, and the tests verify the delegated loadTable call
 * against that injected delegate exactly as they did against the handler's baseCatalog.
 */
@SuppressWarnings("resource")
class IcebergCatalogHandlerTest {

  private static final String CATALOG_NAME = "test";
  private static final Namespace NS1 = Namespace.of("ns1");
  private static final TableIdentifier TABLE2 = TableIdentifier.of(NS1, "table2");

  private final PolarisResolvedPathWrapper resolvedPath = mock(PolarisResolvedPathWrapper.class);
  private final CallContext callContext = mock(CallContext.class);
  private final RealmConfig realmConfig = mock(RealmConfig.class);
  private final AccessDelegationModeResolver accessDelegationModeResolver =
      mock(AccessDelegationModeResolver.class);
  private final StorageAccessConfigProvider storageAccessConfigProvider =
      mock(StorageAccessConfigProvider.class);

  /**
   * Two-argument form for the tests that need the catalog's operational {@code
   * resolvedEntityView} to be the same instance the authorizer populated, exactly as the real
   * {@code ensureBaseInitialized} does (BasePolarisIcebergCatalog.java:764).
   */
  private PolarisIcebergCatalog buildMergedCatalog(
      boolean federated, Catalog underlyingBaseCatalog) {
    return buildMergedCatalog(federated, underlyingBaseCatalog, null);
  }

  /**
   * Builds the merged Iceberg catalog the same way {@link LocalIcebergCatalogFactory} does, but
   * with mock substrate collaborators. The anonymous {@code ensureBaseInitialized} override pins
   * the dispatch state a real resolve would establish, without a live metastore/FileIO: it reuses
   * the authorizer's resolved view (already populated by the preceding {@code authorizeLoadTable})
   * and injects the delegate {@code baseCatalog}, standing in for the retired handler's {@code
   * localCatalogFactory} seam.
   *
   * @param federated whether the resolved catalog behaves as a federated/external catalog; the
   *     merged {@code loadCredentials} branches on this to take the full-loadTable fallback
   * @param underlyingBaseCatalog the catalog the merged instance delegates {@code loadTable} to
   *     once initialized (a mock, so the tests can verify or deny the delegated call)
   * @param operationalViewOverride when non-null, the view installed as the catalog's operational
   *     {@code resolvedEntityView} INSTEAD of the authorizer's. Real {@code ensureBaseInitialized}
   *     shares one instance (BasePolarisIcebergCatalog.java:764), and
   *     CatalogUtils.findResolvedStorageEntity's first lookup is byte-for-byte the one {@code
   *     CatalogAuthorizer.authorizeBasicTableLikeOperation} already made and threw {@code
   *     NoSuchTableException} on -- so on a shared view, authorization passing implies that
   *     lookup is non-null. Diverging the two references is the only seam that reaches the "no
   *     resolved storage entity" branch at all; that branch is otherwise unreachable through
   *     every public entry point (defensive code), which is what {@link
   *     #loadCredentialsErrorsWhenNoStorageEntityResolves} and {@link
   *     #loadTableWithDelegationReturnsTableWithoutCredentialsWhenNoStorageEntityResolves}
   *     deliberately pin.
   */
  @SuppressWarnings("unchecked")
  private PolarisIcebergCatalog buildMergedCatalog(
      boolean federated,
      Catalog underlyingBaseCatalog,
      PolarisResolutionManifestCatalogView operationalViewOverride) {
    when(callContext.getRealmConfig()).thenReturn(realmConfig);
    when(callContext.getRealmContext()).thenReturn(mock(RealmContext.class));

    // Authorization + operational data path: entityResolver.resolve(...) always returns a
    // ResolutionResult wrapping resolvedPath's current leaf entity (read now, after any per-test
    // stub already ran) plus a bare, non-federated catalog entity, so the not-found check in
    // CatalogAuthorizer#authorizeBasicTableLikeOperation passes and the resolved view exposes the
    // table path to getTableEntity() / vendCredentials().
    PolarisDiagnostics diagnostics = mock(PolarisDiagnostics.class);
    PolarisEntity leafEntity = resolvedPath.getRawLeafEntity();
    if (leafEntity == null) {
      leafEntity =
          new PolarisEntity(
              new PolarisBaseEntity.Builder()
                  .typeCode(PolarisEntityType.TABLE_LIKE.getCode())
                  .subTypeCode(PolarisEntitySubType.ICEBERG_TABLE.getCode())
                  .name(TABLE2.name())
                  .build());
    }
    ResolvedPolarisEntity resolvedTableEntity =
        new ResolvedPolarisEntity(diagnostics, leafEntity, List.of(), 0);
    // The resolved-path list must have one entry per ResolvedPathKey.entityNames() segment (here,
    // the "ns1" namespace and the "table2" leaf) or the manifest treats it as a not-found path.
    PolarisEntity namespaceEntity =
        new PolarisEntity(
            new PolarisBaseEntity.Builder()
                .typeCode(PolarisEntityType.NAMESPACE.getCode())
                .name(NS1.levels()[0])
                .build());
    ResolvedPolarisEntity resolvedNamespaceEntity =
        new ResolvedPolarisEntity(diagnostics, namespaceEntity, List.of(), 0);
    PolarisEntity catalogRawEntity =
        new PolarisEntity(
            new PolarisBaseEntity.Builder()
                .typeCode(PolarisEntityType.CATALOG.getCode())
                .name(CATALOG_NAME)
                .build());
    ResolvedPolarisEntity resolvedCatalogEntity =
        new ResolvedPolarisEntity(diagnostics, catalogRawEntity, List.of(), 0);
    ResolutionResult resolutionResult =
        new ResolutionResult(
            new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS),
            null,
            List.of(),
            resolvedCatalogEntity,
            null,
            Map.of(
                ResolvedPathKey.ofTableLike(TABLE2),
                List.of(resolvedNamespaceEntity, resolvedTableEntity)),
            Map.of());
    EntityResolver entityResolver = mock(EntityResolver.class);
    when(entityResolver.resolve(any())).thenReturn(resolutionResult);

    // Grant the decision-native per-op check so authorizeLoadTable's write-delegation probe yields
    // an allow decision; these tests exercise credential loading, not the authz outcome.
    PolarisAuthorizer authorizer = mock(PolarisAuthorizer.class);
    when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenReturn(AuthorizationDecision.allow());

    return new PolarisIcebergCatalog(
        CATALOG_NAME,
        PolarisPrincipal.of("test", Map.of(), Set.of()),
        callContext,
        diagnostics,
        entityResolver,
        authorizer,
        mock(DurableManager.class),
        mock(TaskExecutor.class),
        storageAccessConfigProvider,
        mock(StorageIoProvider.class),
        mock(PolarisEventDispatcher.class),
        mock(PolarisEventMetadataFactory.class),
        mock(PolarisCredentialManager.class),
        mock(Instance.class),
        mock(ReservedProperties.class),
        mock(CatalogHandlerUtils.class),
        mock(EventAttributeMap.class),
        mock(Clock.class),
        accessDelegationModeResolver,
        mock(PolarisMetricsReporter.class),
        mock(CatalogPrefixParser.class)) {
      @Override
      protected void ensureBaseInitialized() {
        // Pin the local-vs-federated dispatch state ensureBaseInitialized() would establish after a
        // real resolve, but without a live metastore/FileIO: reuse the authorizer's resolved view
        // (populated by the preceding authorizeLoadTable) and inject the delegate catalog. When
        // operationalViewOverride is set, install it instead -- see the javadoc above for why that
        // is the only way to reach the "no resolved storage entity" defensive branch.
        this.resolvedEntityView =
            operationalViewOverride != null ? operationalViewOverride : authz.resolvedEntityView();
        this.isFederated = federated;
        this.baseCatalog = underlyingBaseCatalog;
        this.baseInitialized = true;
      }
    };
  }

  /**
   * For federated (external, non-Polaris) catalogs, loadCredentials must skip the optimized
   * entity-properties-based path and fall through to a full loadTable on the delegate catalog,
   * propagating the credentials the storage provider returns for that table.
   */
  @Test
  void loadCredentialsFallsBackForExternalCatalog() {
    String tableLocation = "s3://fake-bucket/tables/table2";
    Map<String, String> fakeCredentials =
        Map.of("fake.access.key", "AKIAFAKE", "fake.secret.key", "fakeSecret");

    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.location()).thenReturn(tableLocation);
    when(metadata.properties()).thenReturn(Map.of());
    TableOperations ops = mock(TableOperations.class);
    when(ops.current()).thenReturn(metadata);
    BaseTable table = mock(BaseTable.class);
    when(table.operations()).thenReturn(ops);

    Catalog externalCatalog = mock(Catalog.class);
    when(externalCatalog.loadTable(TABLE2)).thenReturn(table);

    // A federated catalog forces loadCredentials to skip the optimized path and fall through to a
    // full loadTable on the delegate. Enable federated credential vending so the fallback still
    // attaches the vended credentials to the response.
    when(realmConfig.getConfig(
            eq(FeatureConfiguration.ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING),
            any(CatalogEntity.class)))
        .thenReturn(true);

    // VENDED_CREDENTIALS is what triggers the fallback to attach credentials to the response.
    when(accessDelegationModeResolver.resolve(any(), any()))
        .thenReturn(Optional.of(VENDED_CREDENTIALS));

    StorageAccessConfig storageAccessConfig =
        StorageAccessConfig.builder()
            .putCredential("fake.access.key", "AKIAFAKE")
            .putCredential("fake.secret.key", "fakeSecret")
            .build();
    when(storageAccessConfigProvider.getStorageAccessConfig(any(), any(), any(), any(), any()))
        .thenReturn(storageAccessConfig);

    PolarisIcebergCatalog catalog = buildMergedCatalog(true, externalCatalog);

    ImmutableLoadCredentialsResponse response =
        catalog.loadCredentials(TABLE2, Optional.empty()).body();

    verify(externalCatalog).loadTable(TABLE2);
    assertThat(response.credentials())
        .singleElement()
        .satisfies(
            (Credential c) -> {
              assertThat(c.prefix()).isEqualTo(tableLocation);
              assertThat(c.config()).containsExactlyInAnyOrderEntriesOf(fakeCredentials);
            });
  }

  /**
   * For native Polaris (Iceberg) catalogs, loadCredentials takes the optimized path: it reads the
   * table location from the entity's internal properties and vends credentials without loading the
   * full table metadata. The underlying catalog's loadTable must NOT be invoked.
   */
  @Test
  void loadCredentialsUsesOptimizedPathForIcebergCatalog() {
    String tableLocation = "s3://fake-bucket/tables/table2";
    Map<String, String> fakeCredentials =
        Map.of("fake.access.key", "AKIAFAKE", "fake.secret.key", "fakeSecret");

    // The entity resolved via the manifest carries the table location in its internal properties;
    // that's what lets the optimized path skip a full loadTable.
    PolarisEntity leafEntity =
        new PolarisEntity(
            new PolarisBaseEntity.Builder()
                .typeCode(PolarisEntityType.TABLE_LIKE.getCode())
                .subTypeCode(PolarisEntitySubType.ICEBERG_TABLE.getCode())
                .name(TABLE2.name())
                .internalPropertiesAsMap(Map.of(IcebergTableLikeEntity.LOCATION, tableLocation))
                .build());
    when(resolvedPath.getRawLeafEntity()).thenReturn(leafEntity);

    BridgeBaseMetastoreViewCatalog icebergCatalog = mock(BridgeBaseMetastoreViewCatalog.class);

    StorageAccessConfig storageAccessConfig =
        StorageAccessConfig.builder()
            .putCredential("fake.access.key", "AKIAFAKE")
            .putCredential("fake.secret.key", "fakeSecret")
            .build();
    when(storageAccessConfigProvider.getStorageAccessConfig(any(), any(), any(), any(), any()))
        .thenReturn(storageAccessConfig);

    PolarisIcebergCatalog catalog = buildMergedCatalog(false, icebergCatalog);

    ImmutableLoadCredentialsResponse response =
        catalog.loadCredentials(TABLE2, Optional.empty()).body();

    // The whole point of the optimized path is to skip loadTable on the underlying catalog.
    verify(icebergCatalog, never()).loadTable(any());
    assertThat(response.credentials())
        .singleElement()
        .satisfies(
            (Credential c) -> {
              assertThat(c.prefix()).isEqualTo(tableLocation);
              assertThat(c.config()).containsExactlyInAnyOrderEntriesOf(fakeCredentials);
            });
  }

  /**
   * If the entity's internal properties are missing the LOCATION key, the optimized path cannot
   * vend credentials (it has nothing to scope them to), so loadCredentials must fall back to a full
   * loadTable on the underlying catalog. This guards the backfill path noted in the merged catalog:
   * an entity that pre-dates the location-in-properties write should still serve credentials.
   */
  @Test
  void loadCredentialsFallsBackWhenEntityLocationMissing() {
    String tableLocation = "s3://fake-bucket/tables/table2";
    Map<String, String> fakeCredentials =
        Map.of("fake.access.key", "AKIAFAKE", "fake.secret.key", "fakeSecret");

    // Leaf entity is an Iceberg table-like entity but has no LOCATION in its internal properties,
    // forcing the optimized path to fall back.
    PolarisEntity leafEntity =
        new PolarisEntity(
            new PolarisBaseEntity.Builder()
                .typeCode(PolarisEntityType.TABLE_LIKE.getCode())
                .subTypeCode(PolarisEntitySubType.ICEBERG_TABLE.getCode())
                .name(TABLE2.name())
                .internalPropertiesAsMap(Map.of())
                .build());
    when(resolvedPath.getRawLeafEntity()).thenReturn(leafEntity);

    // The fallback path calls loadTable on the underlying catalog and reads location from the
    // returned table metadata, so we need a BaseTable with a current() TableMetadata.
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.location()).thenReturn(tableLocation);
    when(metadata.properties()).thenReturn(Map.of());
    TableOperations ops = mock(TableOperations.class);
    when(ops.current()).thenReturn(metadata);
    BaseTable table = mock(BaseTable.class);
    when(table.operations()).thenReturn(ops);

    BridgeBaseMetastoreViewCatalog icebergCatalog = mock(BridgeBaseMetastoreViewCatalog.class);
    when(icebergCatalog.loadTable(TABLE2)).thenReturn(table);

    when(accessDelegationModeResolver.resolve(any(), any()))
        .thenReturn(Optional.of(VENDED_CREDENTIALS));

    StorageAccessConfig storageAccessConfig =
        StorageAccessConfig.builder()
            .putCredential("fake.access.key", "AKIAFAKE")
            .putCredential("fake.secret.key", "fakeSecret")
            .build();
    when(storageAccessConfigProvider.getStorageAccessConfig(any(), any(), any(), any(), any()))
        .thenReturn(storageAccessConfig);

    PolarisIcebergCatalog catalog = buildMergedCatalog(false, icebergCatalog);

    ImmutableLoadCredentialsResponse response =
        catalog.loadCredentials(TABLE2, Optional.empty()).body();

    // Missing LOCATION on the entity must force the fallback — loadTable is the proof.
    verify(icebergCatalog).loadTable(TABLE2);
    assertThat(response.credentials())
        .singleElement()
        .satisfies(
            (Credential c) -> {
              assertThat(c.prefix()).isEqualTo(tableLocation);
              assertThat(c.config()).containsExactlyInAnyOrderEntriesOf(fakeCredentials);
            });
  }

  /**
   * A SECOND, deliberately divergent resolved view for the catalog's operational {@code
   * resolvedEntityView} field. Uses the real {@link EntityResolverManifestView} -- the only impl in
   * this chain (CatalogAuthorizer.java:117-118) -- over a second snapshot, so the null that
   * CatalogUtils.findResolvedStorageEntity sees comes from real view semantics, not a mock default.
   *
   * @param tableLikeLeaf leaf for ResolvedPathKey.ofTableLike(TABLE2), or null for an EMPTY path map
   *     (neither a table-like path nor a namespace path resolves)
   */
  private PolarisResolutionManifestCatalogView divergentOperationalView(
      PolarisEntity tableLikeLeaf) {
    PolarisDiagnostics diagnostics = mock(PolarisDiagnostics.class);
    ResolvedPolarisEntity resolvedCatalogEntity =
        new ResolvedPolarisEntity(
            diagnostics,
            new PolarisEntity(
                new PolarisBaseEntity.Builder()
                    .typeCode(PolarisEntityType.CATALOG.getCode())
                    .name(CATALOG_NAME)
                    .build()),
            List.of(),
            0);
    Map<ResolvedPathKey, List<ResolvedPolarisEntity>> resolvedPaths;
    if (tableLikeLeaf == null) {
      resolvedPaths = Map.of();
    } else {
      ResolvedPolarisEntity resolvedNamespaceEntity =
          new ResolvedPolarisEntity(
              diagnostics,
              new PolarisEntity(
                  new PolarisBaseEntity.Builder()
                      .typeCode(PolarisEntityType.NAMESPACE.getCode())
                      .name(NS1.levels()[0])
                      .build()),
              List.of(),
              0);
      resolvedPaths =
          Map.of(
              ResolvedPathKey.ofTableLike(TABLE2),
              List.of(
                  resolvedNamespaceEntity,
                  new ResolvedPolarisEntity(diagnostics, tableLikeLeaf, List.of(), 0)));
    }
    return new EntityResolverManifestView(
        mock(EntityResolver.class), // getPassthroughResolvedPath is never called on these paths
        PolarisPrincipal.of("test", Map.of(), Set.of()),
        CATALOG_NAME,
        new ResolutionResult(
            new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS),
            null,
            List.of(),
            resolvedCatalogEntity, // bare CATALOG -> isPassthroughFacade() false (CatalogEntity.java
            // :227-230 keys on the connection-config-info internal property), so BOTH the
            // partial-path rule and the ICEBERG_TABLE subtype filter stay live
            null,
            resolvedPaths,
            Map.of()));
  }

  /**
   * loadCredentials is the explicit credentials endpoint: credentials are the entire point of the
   * call, so when CatalogUtils.findResolvedStorageEntity finds no resolved path ("not resolvable
   * here" -- neither a table-like path nor a fallback namespace path) the request must ERROR, not
   * return an empty credential set. The mechanism is the locally fabricated StorageAccessConfig
   * carrying supportsCredentialVending(true): an empty credential set plus {@code true} trips the
   * Preconditions check.
   *
   * <p>This branch is defensive: authorizeLoadTable makes the identical ICEBERG_TABLE-filtered
   * lookup first, on the view the catalog normally shares with the authorizer, and throws
   * NoSuchTableException on the same null before this method's own check would ever run. So the
   * fixture below deliberately diverges the catalog's operational view from the authorizer's
   * (see {@link #buildMergedCatalog(boolean, Catalog, PolarisResolutionManifestCatalogView)}) to
   * reach the branch at all. The pin is "if this branch is ever reached, here is the contract" --
   * both existing REST behaviors on this shared check are frozen (Issue 13's locked design).
   */
  @Test
  void loadCredentialsErrorsWhenNoStorageEntityResolves() {
    String tableLocation = "s3://fake-bucket/tables/table2";

    // TABLE_LIKE leaf with subtype ICEBERG_VIEW: the no-filter lookup getTableEntity() makes finds
    // it and IcebergTableLikeEntity.of() accepts it (its constructor allows ICEBERG_TABLE or
    // ICEBERG_VIEW only), while the ICEBERG_TABLE-FILTERED lookup findResolvedStorageEntity makes
    // rejects it (EntityResolverManifestView#filterSubType). NULL_SUBTYPE/GENERIC_TABLE cannot be
    // used -- IcebergTableLikeEntity's constructor throws IllegalStateException("Invalid entity sub
    // type") on those. LOCATION keeps loadCredentials on the optimized path so it reaches
    // vendCredentials instead of falling back to a full loadTable.
    PolarisEntity viewLeafEntity =
        new PolarisEntity(
            new PolarisBaseEntity.Builder()
                .typeCode(PolarisEntityType.TABLE_LIKE.getCode())
                .subTypeCode(PolarisEntitySubType.ICEBERG_VIEW.getCode())
                .name(TABLE2.name())
                .internalPropertiesAsMap(Map.of(IcebergTableLikeEntity.LOCATION, tableLocation))
                .build());

    BridgeBaseMetastoreViewCatalog icebergCatalog = mock(BridgeBaseMetastoreViewCatalog.class);

    // MANDATORY. RealmConfig#getConfig(PolarisConfiguration<T>) erases to Object, so an unstubbed
    // mock returns null and the `!supportsCredentialVending() || skipCredIndirection` check NPEs on
    // unboxing instead of throwing the IllegalArgumentException under test.
    when(realmConfig.getConfig(FeatureConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION))
        .thenReturn(false);

    PolarisIcebergCatalog catalog =
        buildMergedCatalog(false, icebergCatalog, divergentOperationalView(viewLeafEntity));

    assertThatThrownBy(() -> catalog.loadCredentials(TABLE2, Optional.empty()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Credential vending was requested for table ns1.table2, "
                + "but no credentials are available");

    // Proof the error came from the unresolvable-path branch and not from the vendor: the config
    // was fabricated locally, and the optimized path was taken (no full loadTable fallback).
    verify(storageAccessConfigProvider, never())
        .getStorageAccessConfig(any(), any(), any(), any(), any());
    verify(icebergCatalog, never()).loadTable(any());
  }

  /**
   * loadTable-with-delegation is the sibling contract and must keep differing: the client asked for
   * a table and only OPTIONALLY for credentials, so the same CatalogUtils.findResolvedStorageEntity
   * == null condition returns the table with no credentials instead of erroring. The early return
   * sits before the isFederated / ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING check and before the
   * VENDED_CREDENTIALS check, so neither gates reaching it.
   *
   * <p>Same defensive-branch caveat as {@link #loadCredentialsErrorsWhenNoStorageEntityResolves}:
   * this branch is unreachable through the public entry point on the authorizer's own view (the
   * table-like lookup authorizeLoadTable makes is the same one CatalogUtils.findResolvedStorageEntity
   * makes), so the fixture diverges the catalog's operational view from the authorizer's. Both
   * existing REST behaviors on this shared check are frozen (Issue 13's locked design).
   */
  @Test
  void loadTableWithDelegationReturnsTableWithoutCredentialsWhenNoStorageEntityResolves() {
    String metadataFileLocation =
        "s3://fake-bucket/tables/table2/metadata/00000-abcdef.metadata.json";

    // Only metadataFileLocation() is stubbed: LoadTableResponse.Builder#withTableMetadata reads it,
    // and response.metadataLocation() is the only observable proof the response carries OUR table
    // (LoadTableResponse#tableMetadata() lazily REBUILDS via TableMetadata.buildFrom(metadata), so
    // asserting on a mock's rebuilt tableMetadata() is not meaningful here). location()/properties()
    // are left UNSTUBBED on purpose as tripwires: if the early return were ever removed,
    // StorageUtil.getLocationsUsedByTable(tableMetadata) would NPE loudly on the null properties map
    // rather than silently proceed.
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.metadataFileLocation()).thenReturn(metadataFileLocation);
    TableOperations ops = mock(TableOperations.class);
    when(ops.current()).thenReturn(metadata);
    BaseTable table = mock(BaseTable.class);
    when(table.operations()).thenReturn(ops);

    BridgeBaseMetastoreViewCatalog icebergCatalog = mock(BridgeBaseMetastoreViewCatalog.class);
    when(icebergCatalog.loadTable(TABLE2)).thenReturn(table);

    // Delegation was genuinely requested -- that is what makes "response carries no credentials" a
    // meaningful assertion rather than a vacuous one.
    when(accessDelegationModeResolver.resolve(any(), any()))
        .thenReturn(Optional.of(VENDED_CREDENTIALS));

    // Empty path map: neither ofTableLike(TABLE2) nor ofNamespace(NS1) is present, so both lookups
    // in CatalogUtils.findResolvedStorageEntity miss and it returns null.
    PolarisIcebergCatalog catalog =
        buildMergedCatalog(false, icebergCatalog, divergentOperationalView(null));

    LoadTableResponse response =
        catalog
            .loadTable(TABLE2, "all", null, EnumSet.of(VENDED_CREDENTIALS), Optional.empty())
            .body();

    assertThat(response).isNotNull();
    assertThat(response.metadataLocation()).isEqualTo(metadataFileLocation);
    assertThat(response.credentials()).isEmpty();
    assertThat(response.config()).isEmpty();
    verify(icebergCatalog).loadTable(TABLE2);
    // The early return happened: no vendor call, so nothing could have attached credentials.
    verify(storageAccessConfigProvider, never())
        .getStorageAccessConfig(any(), any(), any(), any(), any());
  }
}
