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
package org.apache.polaris.extension.auth.rbac;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.auth.AuthorizationChain;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.auth.PathSegment;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisSecurable;
import org.apache.polaris.core.auth.RootPrivilegeGrantAuthorizationIntent;
import org.apache.polaris.core.auth.SingleTargetAuthorizationIntent;
import org.apache.polaris.core.auth.TargetlessAuthorizationIntent;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.spi.substrate.EntityResolver;
import org.apache.polaris.core.persistence.resolver.ResolutionRequest;
import org.apache.polaris.core.persistence.resolver.ResolutionResult;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.spi.substrate.PolarisAuthorizer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

/**
 * Parity oracle for the RBAC authorizer. Stage 3 (ADR-0008 Decision 4) made {@code authorize}
 * names-only: it composes the {@link EntityResolver} SPI to turn the request's securables into a
 * {@link ResolutionResult}, and {@link AuthorizationChain} composes the authorization chain
 * (reference catalog + root container prepend) that used to live in {@code PolarisResolution
 * manifest}. These tests hand the authorizer a pre-built {@code ResolutionResult} through a mocked
 * {@code EntityResolver} and assert the {@code decide} inputs, including the composed chain content
 * so the rooting behavior the old manifest performed internally is verified directly.
 */
public class RbacAuthorizerTest {

  private static final String ROOT_NAME = PolarisEntityConstants.getRootContainerName();

  @ParameterizedTest
  @EnumSource(PolarisPrivilege.class)
  void subsumingPrivilegesOf(PolarisPrivilege privilege) {
    Set<PolarisPrivilege> actual = RbacAuthorizer.subsumingPrivilegesOf(privilege);
    assertThat(actual).isNotEmpty().contains(privilege);
    Set<PolarisPrivilege> expected =
        RbacAuthorizer.SUPER_PRIVILEGES.containsKey(privilege)
            ? RbacAuthorizer.SUPER_PRIVILEGES.get(privilege)
            : EnumSet.of(privilege);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void authorizeResolvesRequestSecurablesThroughEntityResolver() {
    // authorize is names-only: it builds a ResolutionRequest from the intents' securables (plus the
    // root container as an optional top-level) and resolves it through the EntityResolver SPI.
    EntityResolver entityResolver = mock(EntityResolver.class);
    RbacAuthorizer authorizer = spy(new RbacAuthorizer(mock(RealmConfig.class), entityResolver));
    ResolvedPolarisEntity rootEntity = mock(ResolvedPolarisEntity.class);
    ResolvedPolarisEntity catalogEntity = mock(ResolvedPolarisEntity.class);
    ResolvedPolarisEntity nsEntity = mock(ResolvedPolarisEntity.class);
    when(entityResolver.resolve(any()))
        .thenReturn(
            successResult(
                catalogEntity,
                Map.of(
                    ResolvedPathKey.of(List.of("ns"), PolarisEntityType.NAMESPACE),
                    List.of(nsEntity)),
                Map.of(rootKey(), rootEntity)));
    doReturn(AuthorizationDecision.allow())
        .when(authorizer)
        .decide(any(), any(), any(), any(), any());
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));
    AuthorizationRequest request =
        new AuthorizationRequest(
            principal,
            List.of(
                new SingleTargetAuthorizationIntent(
                    PolarisAuthorizableOperation.LIST_NAMESPACES,
                    PolarisSecurable.of(
                        new PathSegment(PolarisEntityType.CATALOG, "catalog"),
                        new PathSegment(PolarisEntityType.NAMESPACE, "ns")))));

    authorizer.authorize(request);

    ArgumentCaptor<ResolutionRequest> resolutionRequest =
        ArgumentCaptor.forClass(ResolutionRequest.class);
    verify(entityResolver).resolve(resolutionRequest.capture());
    ResolutionRequest resolved = resolutionRequest.getValue();
    assertThat(resolved.principal()).isSameAs(principal);
    assertThat(resolved.referenceCatalogName()).isEqualTo("catalog");
    assertThat(resolved.paths())
        .anySatisfy(
            path ->
                assertThat(path.key())
                    .isEqualTo(ResolvedPathKey.of(List.of("ns"), PolarisEntityType.NAMESPACE)));
    // The root container is requested as an optional top-level so the chain can be rooted.
    assertThat(resolved.topLevelNames())
        .anySatisfy(
            name -> {
              assertThat(name.entityType()).isEqualTo(PolarisEntityType.ROOT);
              assertThat(name.entityName()).isEqualTo(ROOT_NAME);
            });
  }

  @Test
  void authorizeUsesRootTargetForRootGrantRequestWithoutPrimaryTarget() {
    // A root-grant request has no primary target: the root container is the target chain, and the
    // grantee principal role is the secondary chain (rooted).
    EntityResolver entityResolver = mock(EntityResolver.class);
    RbacAuthorizer authorizer = spy(new RbacAuthorizer(mock(RealmConfig.class), entityResolver));
    ResolvedPolarisEntity rootEntity = mock(ResolvedPolarisEntity.class);
    ResolvedPolarisEntity principalRoleEntity = mock(ResolvedPolarisEntity.class);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    when(entityResolver.resolve(any()))
        .thenReturn(
            successResult(
                null,
                Map.of(),
                Map.of(
                    rootKey(),
                    rootEntity,
                    new ResolutionResult.TopLevelKey(
                        PolarisEntityType.PRINCIPAL_ROLE, "analytics-admin"),
                    principalRoleEntity)));
    doReturn(AuthorizationDecision.allow())
        .when(authorizer)
        .decide(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            eq(PolarisAuthorizableOperation.ADD_ROOT_GRANT_TO_PRINCIPAL_ROLE),
            ArgumentMatchers.any(),
            ArgumentMatchers.any());

    AuthorizationRequest request =
        new AuthorizationRequest(
            principal,
            List.of(
                new RootPrivilegeGrantAuthorizationIntent(
                    PolarisAuthorizableOperation.ADD_ROOT_GRANT_TO_PRINCIPAL_ROLE,
                    PolarisSecurable.of(
                        new PathSegment(PolarisEntityType.PRINCIPAL_ROLE, "analytics-admin")))));

    AuthorizationDecision decision = authorizer.authorize(request);

    assertThat(decision.isAllowed()).isTrue();
    verify(authorizer)
        .decide(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.ADD_ROOT_GRANT_TO_PRINCIPAL_ROLE),
            chainOf(rootEntity),
            chainOf(rootEntity, principalRoleEntity));
  }

  @Test
  void authorizeUsesRootTargetForListCatalogsRequestWithoutPrimaryTarget() {
    // A targetless root-rooted op (LIST_CATALOGS) authorizes against the root container as target.
    EntityResolver entityResolver = mock(EntityResolver.class);
    RbacAuthorizer authorizer = spy(new RbacAuthorizer(mock(RealmConfig.class), entityResolver));
    ResolvedPolarisEntity rootEntity = mock(ResolvedPolarisEntity.class);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    when(entityResolver.resolve(any()))
        .thenReturn(successResult(null, Map.of(), Map.of(rootKey(), rootEntity)));
    doReturn(AuthorizationDecision.allow())
        .when(authorizer)
        .decide(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            eq(PolarisAuthorizableOperation.LIST_CATALOGS),
            ArgumentMatchers.any(),
            ArgumentMatchers.any());

    AuthorizationRequest request =
        new AuthorizationRequest(
            principal,
            List.of(new TargetlessAuthorizationIntent(PolarisAuthorizableOperation.LIST_CATALOGS)));

    AuthorizationDecision decision = authorizer.authorize(request);

    assertThat(decision.isAllowed()).isTrue();
    verify(authorizer)
        .decide(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.LIST_CATALOGS),
            chainOf(rootEntity),
            isNull());
  }

  @Test
  void authorizeResolvesNamespaceTargetUsingCatalog() {
    // A namespace target resolves within the reference catalog; the authz chain prepends the root
    // container and the reference catalog to the resolved path.
    EntityResolver entityResolver = mock(EntityResolver.class);
    RbacAuthorizer authorizer = spy(new RbacAuthorizer(mock(RealmConfig.class), entityResolver));
    ResolvedPolarisEntity rootEntity = mock(ResolvedPolarisEntity.class);
    ResolvedPolarisEntity catalogEntity = mock(ResolvedPolarisEntity.class);
    ResolvedPolarisEntity nsEntity = mock(ResolvedPolarisEntity.class);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    when(entityResolver.resolve(any()))
        .thenReturn(
            successResult(
                catalogEntity,
                Map.of(
                    ResolvedPathKey.of(List.of("ns"), PolarisEntityType.NAMESPACE),
                    List.of(nsEntity)),
                Map.of(rootKey(), rootEntity)));
    doReturn(AuthorizationDecision.allow())
        .when(authorizer)
        .decide(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            eq(PolarisAuthorizableOperation.LIST_NAMESPACES),
            ArgumentMatchers.any(),
            ArgumentMatchers.any());

    AuthorizationRequest request =
        new AuthorizationRequest(
            principal,
            List.of(
                new SingleTargetAuthorizationIntent(
                    PolarisAuthorizableOperation.LIST_NAMESPACES,
                    PolarisSecurable.of(
                        new PathSegment(PolarisEntityType.CATALOG, "catalog"),
                        new PathSegment(PolarisEntityType.NAMESPACE, "ns")))));

    AuthorizationDecision decision = authorizer.authorize(request);

    assertThat(decision.isAllowed()).isTrue();
    verify(authorizer)
        .decide(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.LIST_NAMESPACES),
            chainOf(rootEntity, catalogEntity, nsEntity),
            isNull());
  }

  @Test
  void authorizeSingleOperationMultiIntentRequestEvaluatesSequentially() {
    EntityResolver entityResolver = mock(EntityResolver.class);
    RbacAuthorizer authorizer = spy(new RbacAuthorizer(mock(RealmConfig.class), entityResolver));
    ResolvedPolarisEntity rootEntity = mock(ResolvedPolarisEntity.class);
    ResolvedPolarisEntity firstCatalogEntity = mock(ResolvedPolarisEntity.class);
    ResolvedPolarisEntity secondCatalogEntity = mock(ResolvedPolarisEntity.class);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    when(entityResolver.resolve(any()))
        .thenReturn(
            successResult(
                null,
                Map.of(),
                Map.of(
                    rootKey(),
                    rootEntity,
                    new ResolutionResult.TopLevelKey(PolarisEntityType.CATALOG, "catalog1"),
                    firstCatalogEntity,
                    new ResolutionResult.TopLevelKey(PolarisEntityType.CATALOG, "catalog2"),
                    secondCatalogEntity)));
    doReturn(AuthorizationDecision.allow())
        .when(authorizer)
        .decide(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            eq(PolarisAuthorizableOperation.GET_CATALOG),
            ArgumentMatchers.any(),
            ArgumentMatchers.any());

    AuthorizationDecision decision =
        authorizer.authorize(
            new AuthorizationRequest(
                principal,
                List.of(
                    new SingleTargetAuthorizationIntent(
                        PolarisAuthorizableOperation.GET_CATALOG,
                        PolarisSecurable.of(
                            new PathSegment(PolarisEntityType.CATALOG, "catalog1"))),
                    new SingleTargetAuthorizationIntent(
                        PolarisAuthorizableOperation.GET_CATALOG,
                        PolarisSecurable.of(
                            new PathSegment(PolarisEntityType.CATALOG, "catalog2"))))));

    assertThat(decision.isAllowed()).isTrue();
    verify(authorizer, times(1))
        .decide(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.GET_CATALOG),
            chainOf(rootEntity, firstCatalogEntity),
            isNull());
    verify(authorizer, times(1))
        .decide(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.GET_CATALOG),
            chainOf(rootEntity, secondCatalogEntity),
            isNull());
  }

  @Test
  void authorizeUpdateTableMultiIntentRequestEvaluatesSequentially() {
    EntityResolver entityResolver = mock(EntityResolver.class);
    RbacAuthorizer authorizer = spy(new RbacAuthorizer(mock(RealmConfig.class), entityResolver));
    ResolvedPolarisEntity rootEntity = mock(ResolvedPolarisEntity.class);
    ResolvedPolarisEntity catalogEntity = mock(ResolvedPolarisEntity.class);
    ResolvedPolarisEntity tableEntity = mock(ResolvedPolarisEntity.class);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    when(entityResolver.resolve(any()))
        .thenReturn(
            successResult(
                catalogEntity,
                Map.of(
                    ResolvedPathKey.of(List.of("ns", "table"), PolarisEntityType.TABLE_LIKE),
                    List.of(tableEntity)),
                Map.of(rootKey(), rootEntity)));
    doReturn(AuthorizationDecision.allow())
        .when(authorizer)
        .decide(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            any(PolarisAuthorizableOperation.class),
            ArgumentMatchers.any(),
            ArgumentMatchers.any());

    PolarisSecurable tableTarget =
        PolarisSecurable.of(
            new PathSegment(PolarisEntityType.CATALOG, "catalog"),
            new PathSegment(PolarisEntityType.NAMESPACE, "ns"),
            new PathSegment(PolarisEntityType.TABLE_LIKE, "table"));

    AuthorizationDecision decision =
        authorizer.authorize(
            new AuthorizationRequest(
                principal,
                List.of(
                    new SingleTargetAuthorizationIntent(
                        PolarisAuthorizableOperation.REMOVE_TABLE_PROPERTIES, tableTarget),
                    new SingleTargetAuthorizationIntent(
                        PolarisAuthorizableOperation.SET_TABLE_SNAPSHOT_REF, tableTarget))));

    assertThat(decision.isAllowed()).isTrue();
    verify(authorizer, times(1))
        .decide(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.REMOVE_TABLE_PROPERTIES),
            chainOf(rootEntity, catalogEntity, tableEntity),
            isNull());
    verify(authorizer, times(1))
        .decide(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.SET_TABLE_SNAPSHOT_REF),
            chainOf(rootEntity, catalogEntity, tableEntity),
            isNull());
  }

  @Test
  void authorizationRequestThrowsWhenIntentsAreEmpty() {
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> new AuthorizationRequest(principal, List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must contain at least one intent");
  }

  @Test
  void authorizeReturnsDenyDecision() {
    EntityResolver entityResolver = mock(EntityResolver.class);
    RbacAuthorizer authorizer = spy(new RbacAuthorizer(mock(RealmConfig.class), entityResolver));
    ResolvedPolarisEntity rootEntity = mock(ResolvedPolarisEntity.class);
    ResolvedPolarisEntity catalogEntity = mock(ResolvedPolarisEntity.class);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    when(entityResolver.resolve(any()))
        .thenReturn(
            successResult(
                null,
                Map.of(),
                Map.of(
                    rootKey(),
                    rootEntity,
                    new ResolutionResult.TopLevelKey(PolarisEntityType.CATALOG, "catalog"),
                    catalogEntity)));
    doReturn(AuthorizationDecision.deny("missing privilege"))
        .when(authorizer)
        .decide(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            eq(PolarisAuthorizableOperation.GET_CATALOG),
            ArgumentMatchers.any(),
            ArgumentMatchers.any());

    AuthorizationRequest request =
        new AuthorizationRequest(
            principal,
            List.of(
                new SingleTargetAuthorizationIntent(
                    PolarisAuthorizableOperation.GET_CATALOG,
                    PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, "catalog")))));

    AuthorizationDecision decision = authorizer.authorize(request);

    assertThat(decision.isAllowed()).isFalse();
    assertThat(decision.getMessage()).hasValue("missing privilege");
  }

  // --- ADR-0005 Decision 1 parity oracle -------------------------------------------------------
  // decide(...) is the single decision-native source of truth; authorizeOrThrow is a thin throwing
  // convenience over it. These pin the pre-check behavior that the veneer inversion lifted into the
  // decision path (credential-rotation, RESET_CREDENTIALS root-only) and the throw/decision parity.

  private static RealmConfig realmConfigWithRotationEnforcement(boolean enforce) {
    RealmConfig realmConfig = mock(RealmConfig.class);
    when(realmConfig.getConfig(
            FeatureConfiguration.ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING))
        .thenReturn(enforce);
    return realmConfig;
  }

  @Test
  void decideDeniesWhenCredentialRotationRequired() {
    RbacAuthorizer authorizer =
        new RbacAuthorizer(realmConfigWithRotationEnforcement(true), mock(EntityResolver.class));
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            "alice",
            Map.of(PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE, "true"),
            Set.of("role"));

    AuthorizationDecision decision =
        authorizer.decide(
            principal, Set.of(), PolarisAuthorizableOperation.LIST_CATALOGS, null, null);

    assertThat(decision.isAllowed()).isFalse();
    assertThat(decision.getMessage())
        .hasValueSatisfying(
            msg -> assertThat(msg).contains("PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE"));
  }

  @Test
  void decideDeniesResetCredentialsForNonRootPrincipal() {
    RbacAuthorizer authorizer =
        new RbacAuthorizer(realmConfigWithRotationEnforcement(false), mock(EntityResolver.class));
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    AuthorizationDecision decision =
        authorizer.decide(
            principal, Set.of(), PolarisAuthorizableOperation.RESET_CREDENTIALS, null, null);

    assertThat(decision.isAllowed()).isFalse();
    assertThat(decision.getMessage())
        .hasValue("Only Root principal(service-admin) can perform RESET_CREDENTIALS");
  }

  @Test
  void decideAllowsResetCredentialsForRootPrincipal() {
    RbacAuthorizer authorizer =
        new RbacAuthorizer(realmConfigWithRotationEnforcement(false), mock(EntityResolver.class));
    PolarisPrincipal root =
        PolarisPrincipal.of(
            PolarisEntityConstants.getRootPrincipalName(), Map.of(), Set.of("role"));

    AuthorizationDecision decision =
        authorizer.decide(
            root, Set.of(), PolarisAuthorizableOperation.RESET_CREDENTIALS, null, null);

    assertThat(decision.isAllowed()).isTrue();
  }

  @Test
  void authorizeOrThrowThrowsWithDecisionMessageWhenDenied() {
    RbacAuthorizer authorizer =
        spy(new RbacAuthorizer(mock(RealmConfig.class), mock(EntityResolver.class)));
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));
    doReturn(AuthorizationDecision.deny("nope"))
        .when(authorizer)
        .decide(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            eq(PolarisAuthorizableOperation.GET_CATALOG),
            ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any(),
            ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any());

    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                authorizer.authorizeOrThrow(
                    principal,
                    Set.of(),
                    PolarisAuthorizableOperation.GET_CATALOG,
                    (List<PolarisResolvedPathWrapper>) null,
                    (List<PolarisResolvedPathWrapper>) null))
        .isInstanceOf(ForbiddenException.class)
        .hasMessage("nope");
  }

  @Test
  void authorizeOrThrowDoesNotThrowWhenAllowed() {
    RbacAuthorizer authorizer =
        spy(new RbacAuthorizer(mock(RealmConfig.class), mock(EntityResolver.class)));
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));
    doReturn(AuthorizationDecision.allow())
        .when(authorizer)
        .decide(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            eq(PolarisAuthorizableOperation.GET_CATALOG),
            ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any(),
            ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any());

    org.assertj.core.api.Assertions.assertThatCode(
            () ->
                authorizer.authorizeOrThrow(
                    principal,
                    Set.of(),
                    PolarisAuthorizableOperation.GET_CATALOG,
                    (List<PolarisResolvedPathWrapper>) null,
                    (List<PolarisResolvedPathWrapper>) null))
        .doesNotThrowAnyException();
  }

  // --- ADR-0005 Decision 1 consequence: decision-native per-op form lets callers branch instead of
  // probing with try/catch (drives IcebergCatalogHandler.authorizeLoadTable, E3).
  // ------------------

  @Test
  void perOpAuthorizeDelegatesToDecideNatively() {
    RbacAuthorizer authorizer =
        spy(new RbacAuthorizer(mock(RealmConfig.class), mock(EntityResolver.class)));
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));
    PolarisResolvedPathWrapper target = mock(PolarisResolvedPathWrapper.class);
    doReturn(AuthorizationDecision.deny("no"))
        .when(authorizer)
        .decide(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            eq(PolarisAuthorizableOperation.GET_CATALOG),
            eq(List.of(target)),
            eq(null));

    AuthorizationDecision decision =
        authorizer.authorize(
            principal, Set.of(), PolarisAuthorizableOperation.GET_CATALOG, target, null);

    assertThat(decision.isAllowed()).isFalse();
    assertThat(decision.getMessage()).hasValue("no");
    verify(authorizer)
        .decide(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.GET_CATALOG),
            eq(List.of(target)),
            eq(null));
  }

  @Test
  void interfaceDefaultPerOpAuthorizeBridgesThrowToDecision() {
    // A throw-only authorizer (models OPA/Ranger, which do not override the decision-native per-op
    // form): the interface default maps its ForbiddenException to a deny and a clean return to
    // allow.
    PolarisAuthorizer throwOnly =
        new PolarisAuthorizer() {
          @Override
          public AuthorizationDecision authorize(AuthorizationRequest request) {
            return AuthorizationDecision.allow();
          }

          @Override
          public void authorizeOrThrow(
              PolarisPrincipal polarisPrincipal,
              Set<PolarisBaseEntity> activatedEntities,
              PolarisAuthorizableOperation authzOp,
              PolarisResolvedPathWrapper target,
              PolarisResolvedPathWrapper secondary) {
            if (authzOp == PolarisAuthorizableOperation.LIST_CATALOGS) {
              throw new ForbiddenException("denied");
            }
          }

          @Override
          public void authorizeOrThrow(
              PolarisPrincipal polarisPrincipal,
              Set<PolarisBaseEntity> activatedEntities,
              PolarisAuthorizableOperation authzOp,
              List<PolarisResolvedPathWrapper> targets,
              List<PolarisResolvedPathWrapper> secondaries) {}
        };
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    AuthorizationDecision allowed =
        throwOnly.authorize(
            principal, Set.of(), PolarisAuthorizableOperation.GET_CATALOG, null, null);
    AuthorizationDecision denied =
        throwOnly.authorize(
            principal, Set.of(), PolarisAuthorizableOperation.LIST_CATALOGS, null, null);

    assertThat(allowed.isAllowed()).isTrue();
    assertThat(denied.isAllowed()).isFalse();
    assertThat(denied.getMessage()).hasValue("denied");
  }

  // --- helpers ---------------------------------------------------------------------------------

  private static ResolutionResult.TopLevelKey rootKey() {
    return new ResolutionResult.TopLevelKey(PolarisEntityType.ROOT, ROOT_NAME);
  }

  private static ResolutionResult successResult(
      ResolvedPolarisEntity referenceCatalog,
      Map<ResolvedPathKey, List<ResolvedPolarisEntity>> paths,
      Map<ResolutionResult.TopLevelKey, ResolvedPolarisEntity> topLevel) {
    return new ResolutionResult(
        new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS),
        null,
        List.of(),
        referenceCatalog,
        null,
        paths,
        topLevel);
  }

  /**
   * Matches a single-element list of wrappers whose one wrapper's full resolved path is exactly the
   * given entities in order, verifying the authorization chain {@link AuthorizationChain} composed
   * (root container, reference catalog, then the resolved path).
   */
  private static List<PolarisResolvedPathWrapper> chainOf(ResolvedPolarisEntity... entities) {
    return argThat(
        (List<PolarisResolvedPathWrapper> wrappers) ->
            wrappers != null
                && wrappers.size() == 1
                && wrappers.get(0).getResolvedFullPath().equals(List.of(entities)));
  }
}
