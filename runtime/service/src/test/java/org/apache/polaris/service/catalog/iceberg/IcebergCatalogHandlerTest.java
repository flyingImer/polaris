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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import java.time.Clock;
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
   */
  @SuppressWarnings("unchecked")
  private PolarisIcebergCatalog buildMergedCatalog(
      boolean federated, Catalog underlyingBaseCatalog) {
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
        // (populated by the preceding authorizeLoadTable) and inject the delegate catalog.
        this.resolvedEntityView = authz.resolvedEntityView();
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
}
