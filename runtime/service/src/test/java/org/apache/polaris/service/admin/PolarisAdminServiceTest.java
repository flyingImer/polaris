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
package org.apache.polaris.service.admin;

import static org.apache.polaris.core.entity.PolarisEntitySubType.ICEBERG_TABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisGrantManager;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisSecretsManager;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.GenerateEntityIdResult;
import org.apache.polaris.core.persistence.dao.entity.PrivilegeResult;
import org.apache.polaris.core.persistence.resolver.EntityResolver;
import org.apache.polaris.core.persistence.resolver.ResolutionRequest;
import org.apache.polaris.core.persistence.resolver.ResolutionResult;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.service.config.ReservedProperties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class PolarisAdminServiceTest {
  @Mock private CallContext callContext;
  @Mock private PolarisCallContext polarisCallContext;
  @Mock private PolarisMetaStoreManager metaStoreManager;
  @Mock private PolarisSecretsManager secretsManager;
  @Mock private PolarisGrantManager grantManager;
  @Mock private UserSecretsManager userSecretsManager;
  @Mock private ServiceIdentityProvider identityProvider;
  @Mock private PolarisAuthorizer authorizer;
  @Mock private ReservedProperties reservedProperties;
  @Mock private PolarisPrincipal authenticatedPrincipal;
  @Mock private RealmConfig realmConfig;
  @Mock private EntityResolver entityResolver;

  private PolarisAdminService adminService;

  /**
   * Per-test resolution stubs, keyed by whether the requesting {@link ResolverPath} was optional.
   * The production code registers a securable's own resolution as a required path (optional =false,
   * e.g. the namespace/table-like path inside {@code
   * authorizeGrantOnNamespaceOperationOrThrow}/{@code authorizeGrantOnTableLikeOperationOrThrow})
   * and re-reads it via a fresh, always-optional passthrough resolve inside {@code
   * createSyntheticNamespaceEntities}/{@code createSyntheticTableLikeEntities} (ADR-0008's
   * read-your-own-write escape hatch) -- the two maps let a test set up "partially resolved before
   * synthetic creation" and "fully resolved after" independently, mirroring how the two production
   * call sites are genuinely different resolves, not the same manifest read twice.
   */
  private final Map<ResolvedPathKey, List<ResolvedPolarisEntity>> requiredPathStubs =
      new HashMap<>();

  private final Map<ResolvedPathKey, List<ResolvedPolarisEntity>> passthroughPathStubs =
      new HashMap<>();

  private PolarisEntity catalogEntityStub;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    when(callContext.getPolarisCallContext()).thenReturn(polarisCallContext);
    when(callContext.getRealmConfig()).thenReturn(realmConfig);

    // Default feature configuration - enabled by default
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS))
        .thenReturn(true);
    when(realmConfig.getConfig(
            eq(FeatureConfiguration.ENABLE_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS),
            (CatalogEntity) Mockito.any()))
        .thenReturn(true);

    when(entityResolver.resolve(any()))
        .thenAnswer(inv -> buildResolutionResult(inv.getArgument(0)));

    adminService =
        new PolarisAdminService(
            callContext,
            entityResolver,
            metaStoreManager,
            secretsManager,
            grantManager,
            userSecretsManager,
            identityProvider,
            authenticatedPrincipal,
            authorizer,
            reservedProperties);
  }

  private ResolutionResult buildResolutionResult(ResolutionRequest request) {
    Map<ResolvedPathKey, List<ResolvedPolarisEntity>> resolvedPaths = new HashMap<>();
    for (ResolverPath path : request.paths()) {
      if (path.key().entityNames().isEmpty()) {
        // Nothing to look up (e.g. a table directly under the catalog root) -- trivially
        // resolved, matching real resolver behavior for a zero-length path.
        resolvedPaths.put(path.key(), List.of());
        continue;
      }
      Map<ResolvedPathKey, List<ResolvedPolarisEntity>> source =
          path.optional() ? passthroughPathStubs : requiredPathStubs;
      List<ResolvedPolarisEntity> chain = source.get(path.key());
      if (chain != null) {
        resolvedPaths.put(path.key(), chain);
      } else if (!path.optional()) {
        // Mirrors the real resolver: a required path with no matching stub is unresolved, so the
        // overall status reports PATH_COULD_NOT_BE_FULLY_RESOLVED rather than silently SUCCESS.
        return ResolutionResult.failure(new ResolverStatus(path, 0));
      }
    }
    ResolvedPolarisEntity catalog =
        catalogEntityStub == null ? null : toResolved(catalogEntityStub);
    return new ResolutionResult(
        new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS),
        null,
        List.of(),
        catalog,
        null,
        resolvedPaths,
        Map.of());
  }

  private static ResolvedPolarisEntity toResolved(PolarisEntity entity) {
    return new ResolvedPolarisEntity(mock(PolarisDiagnostics.class), entity, List.of(), 0);
  }

  private static List<ResolvedPolarisEntity> toResolvedChain(PolarisEntity... entities) {
    List<ResolvedPolarisEntity> chain = new ArrayList<>();
    for (PolarisEntity entity : entities) {
      chain.add(toResolved(entity));
    }
    return chain;
  }

  /**
   * Marks the catalog itself, and optionally the connection-config property that makes {@link
   * CatalogEntity#isPassthroughFacade()} true for the resolved reference catalog.
   */
  private void stubCatalog(String catalogName, boolean passthroughFacade) {
    PolarisEntity.Builder builder =
        new PolarisEntity.Builder()
            .setName(catalogName)
            .setType(PolarisEntityType.CATALOG)
            .setId(1L);
    if (passthroughFacade) {
      builder.addInternalProperty(
          PolarisEntityConstants.getConnectionConfigInfoPropertyName(), "{}");
    }
    catalogEntityStub = builder.build();
  }

  private void stubCatalogRole(String catalogRoleName, PolarisEntity catalogRoleEntity) {
    requiredPathStubs.put(
        ResolvedPathKey.ofCatalogRole(catalogRoleName), toResolvedChain(catalogRoleEntity));
  }

  private void stubRequiredNamespace(Namespace namespace, PolarisEntity... resolvedLevels) {
    requiredPathStubs.put(
        ResolvedPathKey.of(List.of(namespace.levels()), PolarisEntityType.NAMESPACE),
        toResolvedChain(resolvedLevels));
  }

  private void stubPassthroughNamespace(Namespace namespace, PolarisEntity... resolvedLevels) {
    passthroughPathStubs.put(
        ResolvedPathKey.of(List.of(namespace.levels()), PolarisEntityType.NAMESPACE),
        toResolvedChain(resolvedLevels));
  }

  private void stubRequiredTableLike(TableIdentifier identifier, PolarisEntity... resolvedLevels) {
    requiredPathStubs.put(
        ResolvedPathKey.of(
            PolarisCatalogHelpers.tableIdentifierToList(identifier), PolarisEntityType.TABLE_LIKE),
        toResolvedChain(resolvedLevels));
  }

  private void stubPassthroughTableLike(
      TableIdentifier identifier, PolarisEntity... resolvedLevels) {
    passthroughPathStubs.put(
        ResolvedPathKey.of(
            PolarisCatalogHelpers.tableIdentifierToList(identifier), PolarisEntityType.TABLE_LIKE),
        toResolvedChain(resolvedLevels));
  }

  protected static void assertSuccess(BaseResult result) {
    Assertions.assertThat(result.isSuccess()).isTrue();
  }

  @Test
  void testGrantPrivilegeOnNamespaceToRole() throws Exception {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("existing-ns");
    PolarisPrivilege privilege = PolarisPrivilege.NAMESPACE_FULL_METADATA;

    setupSuccessfulNamespaceResolution(catalogName, catalogRoleName, namespace);

    PrivilegeResult successResult = mock(PrivilegeResult.class);
    when(successResult.isSuccess()).thenReturn(true);
    when(grantManager.grantPrivilegeOnSecurableToRole(any(), any(), any(), any(), any()))
        .thenReturn(successResult);

    PrivilegeResult result =
        adminService.grantPrivilegeOnNamespaceToRole(
            catalogName, catalogRoleName, namespace, privilege);

    assertSuccess(result);
  }

  @Test
  void testGrantPrivilegeOnNamespaceToRole_ThrowsNamespaceNotFoundException() {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("non-existent-ns");
    PolarisPrivilege privilege = PolarisPrivilege.NAMESPACE_FULL_METADATA;

    stubCatalog(catalogName, false);
    stubCatalogRole(
        catalogRoleName, createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE, 2L));
    // Namespace deliberately left unresolved, so the combined resolve for
    // authorizeGrantOnNamespaceOperationOrThrow reports PATH_COULD_NOT_BE_FULLY_RESOLVED and the
    // authz-level check throws directly, before ever reaching the grant helper's own null check.

    assertThatThrownBy(
            () ->
                adminService.grantPrivilegeOnNamespaceToRole(
                    catalogName, catalogRoleName, namespace, privilege))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining(namespace.toString());
  }

  @Test
  void testGrantPrivilegeOnNamespaceToRole_IncompleteNamespaceThrowsNamespaceNotFoundException()
      throws Exception {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("complete-ns", "incomplete-ns");
    PolarisPrivilege privilege = PolarisPrivilege.NAMESPACE_FULL_METADATA;

    stubCatalog(catalogName, false);
    stubCatalogRole(
        catalogRoleName, createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE, 2L));
    // Only the first level resolves -- fewer entities than namespace.levels().length, so
    // isFullyResolvedNamespace() computes false for real.
    stubRequiredNamespace(namespace, createNamespaceEntity(Namespace.of("complete-ns"), 3L, 1L));

    assertThatThrownBy(
            () ->
                adminService.grantPrivilegeOnNamespaceToRole(
                    catalogName, catalogRoleName, namespace, privilege))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("Namespace " + namespace + " not found");
  }

  @Test
  void testRevokePrivilegeOnNamespaceFromRole() throws Exception {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("existing-ns");
    PolarisPrivilege privilege = PolarisPrivilege.NAMESPACE_FULL_METADATA;

    setupSuccessfulNamespaceResolution(catalogName, catalogRoleName, namespace);

    PrivilegeResult successResult = mock(PrivilegeResult.class);
    when(successResult.isSuccess()).thenReturn(true);
    when(grantManager.revokePrivilegeOnSecurableFromRole(any(), any(), any(), any(), any()))
        .thenReturn(successResult);

    PrivilegeResult result =
        adminService.revokePrivilegeOnNamespaceFromRole(
            catalogName, catalogRoleName, namespace, privilege);

    assertSuccess(result);
  }

  @Test
  void testRevokePrivilegeOnNamespaceFromRole_ThrowsNamespaceNotFoundException() {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("non-existent-ns");
    PolarisPrivilege privilege = PolarisPrivilege.NAMESPACE_FULL_METADATA;

    stubCatalog(catalogName, false);
    stubCatalogRole(
        catalogRoleName, createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE, 2L));

    assertThatThrownBy(
            () ->
                adminService.revokePrivilegeOnNamespaceFromRole(
                    catalogName, catalogRoleName, namespace, privilege))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining(namespace.toString());
  }

  @Test
  void testRevokePrivilegeOnNamespaceFromRole_IncompletelNamespaceThrowsNamespaceNotFoundException()
      throws Exception {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("incomplete-ns");
    PolarisPrivilege privilege = PolarisPrivilege.NAMESPACE_FULL_METADATA;

    stubCatalog("wrong-catalog", false);
    stubCatalogRole(
        catalogRoleName, createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE, 2L));
    // Resolves, but under a catalog name that won't match catalogName in isFullyResolvedNamespace.
    stubRequiredNamespace(namespace, createNamespaceEntity(namespace, 3L, 1L));

    assertThatThrownBy(
            () ->
                adminService.revokePrivilegeOnNamespaceFromRole(
                    catalogName, catalogRoleName, namespace, privilege))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("Namespace " + namespace + " not found");
  }

  @Test
  void testGrantPrivilegeOnNamespaceToRole_PassthroughFacade() throws Exception {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("org-ns", "team-ns", "project-ns");
    PolarisPrivilege privilege = PolarisPrivilege.NAMESPACE_FULL_METADATA;

    stubCatalog(catalogName, true);
    stubCatalogRole(
        catalogRoleName, createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE, 2L));

    PolarisEntity orgNsEntity = createNamespaceEntity(Namespace.of("org-ns"), 3L, 1L);
    // Only org-ns resolves initially (passthrough facade, partial resolve is expected).
    stubRequiredNamespace(namespace, orgNsEntity);

    // Mock creation of team-ns.
    GenerateEntityIdResult idResult = mock(GenerateEntityIdResult.class);
    when(idResult.getId()).thenReturn(4L);
    when(metaStoreManager.generateNewEntityId(any())).thenReturn(idResult);
    EntityResult teamNsCreateResult = mock(EntityResult.class);
    EntityResult projectNsCreateResult = mock(EntityResult.class);
    when(teamNsCreateResult.isSuccess()).thenReturn(true);
    when(projectNsCreateResult.isSuccess()).thenReturn(true);

    PolarisEntity teamNsEntity = createNamespaceEntity(Namespace.of("org-ns", "team-ns"), 4L, 3L);
    when(teamNsCreateResult.getEntity()).thenReturn(teamNsEntity);

    // Mock creation of project-ns.
    PolarisEntity projectNsEntity =
        createNamespaceEntity(Namespace.of("org-ns", "team-ns", "project-ns"), 5L, 4L);
    when(projectNsCreateResult.getEntity()).thenReturn(projectNsEntity);

    when(metaStoreManager.createEntityIfNotExists(any(), any(), any()))
        .thenReturn(teamNsCreateResult, projectNsCreateResult);

    // Fresh passthrough re-read after synthetic creation sees the fully resolved chain.
    stubPassthroughNamespace(namespace, orgNsEntity, teamNsEntity, projectNsEntity);

    PrivilegeResult successResult = mock(PrivilegeResult.class);
    when(successResult.isSuccess()).thenReturn(true);
    when(grantManager.grantPrivilegeOnSecurableToRole(any(), any(), any(), any(), any()))
        .thenReturn(successResult);

    PrivilegeResult result =
        adminService.grantPrivilegeOnNamespaceToRole(
            catalogName, catalogRoleName, namespace, privilege);
    assertThat(result.isSuccess()).isTrue();
  }

  @Test
  void testGrantPrivilegeOnNamespaceToRole_PassthroughFacade_FeatureDisabled() throws Exception {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("org-ns", "team-ns", "project-ns");
    PolarisPrivilege privilege = PolarisPrivilege.NAMESPACE_FULL_METADATA;

    // Disable the feature configuration
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS))
        .thenReturn(false);
    when(realmConfig.getConfig(
            eq(FeatureConfiguration.ENABLE_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS),
            (CatalogEntity) Mockito.any()))
        .thenReturn(false);

    stubCatalog(catalogName, true);
    stubCatalogRole(
        catalogRoleName, createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE, 2L));
    // Partially resolved, and since the feature is disabled the passthrough-facade fallback
    // never fires, so this must surface as not-found.
    stubRequiredNamespace(namespace, createNamespaceEntity(Namespace.of("org-ns"), 3L, 1L));

    // Should throw NotFoundException because feature is disabled and it's passthrough facade
    assertThatThrownBy(
            () ->
                adminService.grantPrivilegeOnNamespaceToRole(
                    catalogName, catalogRoleName, namespace, privilege))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("Namespace " + namespace + " not found");
  }

  @Test
  void testGrantPrivilegeOnNamespaceToRole_SyntheticEntityCreationFails() throws Exception {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("org-ns", "team-ns", "project-ns");
    PolarisPrivilege privilege = PolarisPrivilege.NAMESPACE_FULL_METADATA;

    stubCatalog(catalogName, true);
    stubCatalogRole(
        catalogRoleName, createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE, 2L));

    PolarisEntity orgNsEntity = createNamespaceEntity(Namespace.of("org-ns"), 3L, 1L);
    stubRequiredNamespace(namespace, orgNsEntity);

    // Mock generateNewEntityId for team-ns
    GenerateEntityIdResult idResult = mock(GenerateEntityIdResult.class);
    when(idResult.getId()).thenReturn(4L);
    when(metaStoreManager.generateNewEntityId(any())).thenReturn(idResult);

    // Mock createEntityIfNotExists to fail (not ENTITY_ALREADY_EXISTS, a hard failure), and never
    // populate a passthrough stub for the namespace, so the post-creation re-read is absent too.
    EntityResult failedResult = mock(EntityResult.class);
    when(failedResult.isSuccess()).thenReturn(false);
    when(failedResult.getReturnStatus())
        .thenReturn(BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED);
    when(metaStoreManager.createEntityIfNotExists(any(), any(), any())).thenReturn(failedResult);

    assertThatThrownBy(
            () ->
                adminService.grantPrivilegeOnNamespaceToRole(
                    catalogName, catalogRoleName, namespace, privilege))
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "Failed to create or find namespace entity 'team-ns' in federated catalog 'test-catalog'");
  }

  @Test
  void testGrantPrivilegeOnTableLikeToRole_PassthroughFacade() throws Exception {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("org-ns", "team-ns", "project-ns");
    TableIdentifier identifier = TableIdentifier.of(namespace, "test-table");
    PolarisPrivilege privilege = PolarisPrivilege.TABLE_WRITE_DATA;

    stubCatalog(catalogName, true);

    PolarisEntity catalogRoleEntity = createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE);
    stubCatalogRole(catalogRoleName, catalogRoleEntity);

    PolarisEntity orgNsEntity = createNamespaceEntity(Namespace.of("org-ns"), 3L, 1L);
    PolarisEntity teamNsEntity = createNamespaceEntity(Namespace.of("org-ns", "team-ns"), 4L, 3L);
    // Namespace and table-like both partially resolve to the same org-ns/team-ns prefix
    // (project-ns and the table itself don't exist yet); the mismatched leaf subtype
    // (NAMESPACE, not a table) drives the passthrough-facade synthetic-creation branch.
    stubRequiredNamespace(namespace, orgNsEntity, teamNsEntity);
    stubRequiredTableLike(identifier, orgNsEntity, teamNsEntity);

    GenerateEntityIdResult idResult = mock(GenerateEntityIdResult.class);
    when(idResult.getId()).thenReturn(5L);
    when(metaStoreManager.generateNewEntityId(any())).thenReturn(idResult);
    PolarisEntity projectNsEntity =
        createNamespaceEntity(Namespace.of("org-ns", "team-ns", "project-ns"), 5L, 4L);
    EntityResult projectNsCreateResult = mock(EntityResult.class);
    when(projectNsCreateResult.isSuccess()).thenReturn(true);
    when(projectNsCreateResult.getEntity()).thenReturn(projectNsEntity);

    stubPassthroughNamespace(namespace, orgNsEntity, teamNsEntity, projectNsEntity);

    PolarisEntity tableEntity = createTableEntity(identifier, ICEBERG_TABLE, 6L, 5L);
    EntityResult tableCreateResult = mock(EntityResult.class);
    when(tableCreateResult.isSuccess()).thenReturn(true);
    when(tableCreateResult.getEntity()).thenReturn(tableEntity);
    when(metaStoreManager.createEntityIfNotExists(any(), any(), any()))
        .thenReturn(projectNsCreateResult, tableCreateResult);

    stubPassthroughTableLike(identifier, tableEntity);

    PrivilegeResult successResult = mock(PrivilegeResult.class);
    when(successResult.isSuccess()).thenReturn(true);
    when(grantManager.grantPrivilegeOnSecurableToRole(any(), any(), any(), any(), any()))
        .thenReturn(successResult);

    PrivilegeResult result =
        adminService.grantPrivilegeOnTableToRole(
            catalogName, catalogRoleName, identifier, privilege);
    assertThat(result.isSuccess()).isTrue();
  }

  @Test
  void testGrantPrivilegeOnTableLikeToRole_PassthroughFacade_FeatureDisabled() throws Exception {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    Namespace namespace = Namespace.of("org-ns", "team-ns", "project-ns");
    TableIdentifier identifier = TableIdentifier.of(namespace, "test-table");
    PolarisPrivilege privilege = PolarisPrivilege.TABLE_WRITE_DATA;

    // Disable the feature configuration
    when(realmConfig.getConfig(FeatureConfiguration.ENABLE_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS))
        .thenReturn(false);
    when(realmConfig.getConfig(
            eq(FeatureConfiguration.ENABLE_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS),
            (CatalogEntity) Mockito.any()))
        .thenReturn(false);

    stubCatalog(catalogName, true);
    stubCatalogRole(
        catalogRoleName, createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE, 2L));
    // Namespace resolves fully, but the table-like leaf is a namespace entity (wrong subtype) --
    // with the feature disabled, authorizeGrantOnTableLikeOperationOrThrow's own subtype check
    // throws directly instead of falling through to the passthrough-facade synthetic-creation path.
    PolarisEntity orgNsEntity = createNamespaceEntity(Namespace.of("org-ns"), 3L, 1L);
    PolarisEntity teamNsEntity = createNamespaceEntity(Namespace.of("org-ns", "team-ns"), 4L, 3L);
    PolarisEntity projectNsEntity = createNamespaceEntity(namespace, 5L, 4L);
    stubRequiredNamespace(namespace, orgNsEntity, teamNsEntity, projectNsEntity);
    stubRequiredTableLike(identifier, orgNsEntity, teamNsEntity, projectNsEntity);

    // Should throw NoSuchTableException because feature is disabled
    assertThatThrownBy(
            () ->
                adminService.grantPrivilegeOnTableToRole(
                    catalogName, catalogRoleName, identifier, privilege))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("Table does not exist");
  }

  @Test
  void testGrantPrivilegeOnTableLikeToRole_SyntheticEntityCreationFails() throws Exception {
    String catalogName = "test-catalog";
    String catalogRoleName = "test-role";
    TableIdentifier identifier = TableIdentifier.of(Namespace.empty(), "test-table");
    PolarisPrivilege privilege = PolarisPrivilege.TABLE_WRITE_DATA;

    stubCatalog(catalogName, true);
    PolarisEntity catalogRoleEntity = createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE);
    stubCatalogRole(catalogRoleName, catalogRoleEntity);

    // Table-like path never resolves (namespace is empty, so createSyntheticTableLikeEntities
    // uses the catalog itself as existingPathWrapper's stand-in).
    stubRequiredTableLike(identifier, catalogEntityStub);

    GenerateEntityIdResult idResult = mock(GenerateEntityIdResult.class);
    when(idResult.getId()).thenReturn(3L);
    when(metaStoreManager.generateNewEntityId(any())).thenReturn(idResult);
    EntityResult tableCreateResult = mock(EntityResult.class);
    when(metaStoreManager.createEntityIfNotExists(any(), any(), any()))
        .thenReturn(tableCreateResult);
    when(tableCreateResult.isSuccess()).thenReturn(false);

    // No passthrough stub for the table-like key -- the final re-read comes back null, matching
    // the "failed to create or find" branch.

    assertThatThrownBy(
            () ->
                adminService.grantPrivilegeOnTableToRole(
                    catalogName, catalogRoleName, identifier, privilege))
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "Failed to create or find table entity 'test-table' in federated catalog 'test-catalog'");
  }

  private PolarisEntity createEntity(String name, PolarisEntityType type) {
    return new PolarisEntity.Builder()
        .setName(name)
        .setType(type)
        .setId(1L)
        .setCatalogId(1L)
        .setCreateTimestamp(System.currentTimeMillis())
        .build();
  }

  private PolarisEntity createEntity(String name, PolarisEntityType type, long id) {
    return new PolarisEntity.Builder()
        .setName(name)
        .setType(type)
        .setId(id)
        .setCatalogId(1L)
        .setCreateTimestamp(System.currentTimeMillis())
        .build();
  }

  private PolarisEntity createNamespaceEntity(Namespace namespace, long id, long parentId) {
    return new NamespaceEntity.Builder(namespace)
        .setId(id)
        .setCatalogId(1L)
        .setParentId(parentId)
        .setCreateTimestamp(System.currentTimeMillis())
        .build();
  }

  private PolarisEntity createTableEntity(
      TableIdentifier identifier, PolarisEntitySubType subType, long id, long parentId) {
    return new IcebergTableLikeEntity.Builder(subType, identifier, "")
        .setId(id)
        .setCatalogId(1L)
        .setParentId(parentId)
        .setCreateTimestamp(System.currentTimeMillis())
        .build();
  }

  private void setupSuccessfulNamespaceResolution(
      String catalogName, String catalogRoleName, Namespace namespace) throws Exception {
    stubCatalog(catalogName, false);
    PolarisEntity catalogRoleEntity =
        createEntity(catalogRoleName, PolarisEntityType.CATALOG_ROLE, 2L);
    stubCatalogRole(catalogRoleName, catalogRoleEntity);

    PolarisEntity namespaceEntity =
        createNamespaceEntity(Namespace.of(namespace.levels()[0]), 3L, 1L);
    stubRequiredNamespace(namespace, namespaceEntity);
  }
}
