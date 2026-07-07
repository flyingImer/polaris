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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentMatchers;

public class PolarisAuthorizerImplTest {

  @ParameterizedTest
  @EnumSource(PolarisPrivilege.class)
  void subsumingPrivilegesOf(PolarisPrivilege privilege) {
    Set<PolarisPrivilege> actual = PolarisAuthorizerImpl.subsumingPrivilegesOf(privilege);
    assertThat(actual).isNotEmpty().contains(privilege);
    Set<PolarisPrivilege> expected =
        PolarisAuthorizerImpl.SUPER_PRIVILEGES.containsKey(privilege)
            ? PolarisAuthorizerImpl.SUPER_PRIVILEGES.get(privilege)
            : EnumSet.of(privilege);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  void resolveAuthorizationInputsResolvesAll() {
    PolarisAuthorizerImpl authorizer = new PolarisAuthorizerImpl(mock(RealmConfig.class));
    AuthorizationState authzState = new AuthorizationState();
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));
    AuthorizationRequest request =
        new AuthorizationRequest(
            principal,
            List.of(
                new SingleTargetAuthorizationIntent(
                    PolarisAuthorizableOperation.GET_CATALOG,
                    PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, "catalog")))));

    authzState.setResolutionManifest(manifest);

    authorizer.resolveAuthorizationInputs(authzState, request);

    verify(manifest).resolveAll();
  }

  @Test
  void authorizeUsesRootTargetForRootGrantRequestWithoutPrimaryTarget() {
    // Verify that new authorize SPI call without primary target uses root_container
    // for resolution and authorization
    PolarisAuthorizerImpl authorizer = spy(new PolarisAuthorizerImpl(mock(RealmConfig.class)));
    AuthorizationState authzState = new AuthorizationState();
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper rootWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisResolvedPathWrapper principalRoleWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    authzState.setResolutionManifest(manifest);
    when(manifest.getResolvedRootContainerEntityAsPath()).thenReturn(rootWrapper);
    when(manifest.getResolvedTopLevelEntity("analytics-admin", PolarisEntityType.PRINCIPAL_ROLE))
        .thenReturn(principalRoleWrapper);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles()).thenReturn(Set.of());
    doReturn(AuthorizationDecision.allow())
        .when(authorizer)
        .decide(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            eq(PolarisAuthorizableOperation.ADD_ROOT_GRANT_TO_PRINCIPAL_ROLE),
            ArgumentMatchers.any(),
            ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any());

    AuthorizationRequest request =
        new AuthorizationRequest(
            principal,
            List.of(
                new RootPrivilegeGrantAuthorizationIntent(
                    PolarisAuthorizableOperation.ADD_ROOT_GRANT_TO_PRINCIPAL_ROLE,
                    PolarisSecurable.of(
                        new PathSegment(PolarisEntityType.PRINCIPAL_ROLE, "analytics-admin")))));

    AuthorizationDecision decision = authorizer.authorize(authzState, request);

    assertThat(decision.isAllowed()).isTrue();
    verify(authorizer)
        .decide(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.ADD_ROOT_GRANT_TO_PRINCIPAL_ROLE),
            eq(List.of(rootWrapper)),
            eq(List.of(principalRoleWrapper)));
  }

  @Test
  void authorizeUsesRootTargetForListCatalogsRequestWithoutPrimaryTarget() {
    // Verify that new authorize SPI call without primary target uses root_container
    // for resolution and authorization
    PolarisAuthorizerImpl authorizer = spy(new PolarisAuthorizerImpl(mock(RealmConfig.class)));
    AuthorizationState authzState = new AuthorizationState();
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper rootWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    authzState.setResolutionManifest(manifest);
    when(manifest.getResolvedRootContainerEntityAsPath()).thenReturn(rootWrapper);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles()).thenReturn(Set.of());
    doReturn(AuthorizationDecision.allow())
        .when(authorizer)
        .decide(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            eq(PolarisAuthorizableOperation.LIST_CATALOGS),
            ArgumentMatchers.any(),
            ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any());

    AuthorizationRequest request =
        new AuthorizationRequest(
            principal,
            List.of(new TargetlessAuthorizationIntent(PolarisAuthorizableOperation.LIST_CATALOGS)));

    AuthorizationDecision decision = authorizer.authorize(authzState, request);

    assertThat(decision.isAllowed()).isTrue();
    verify(authorizer)
        .decide(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.LIST_CATALOGS),
            eq(List.of(rootWrapper)),
            eq(null));
  }

  @Test
  void authorizeResolvesNamespaceTargetUsingCatalog() {
    // Verify authorize call that includes Catalog name in the PolarisSecurable
    // successfully resolves the correct namespace
    PolarisAuthorizerImpl authorizer = spy(new PolarisAuthorizerImpl(mock(RealmConfig.class)));
    AuthorizationState authzState = new AuthorizationState();
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper namespaceWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    authzState.setResolutionManifest(manifest);
    when(manifest.getResolvedPath(
            ResolvedPathKey.of(List.of("ns"), PolarisEntityType.NAMESPACE), true))
        .thenReturn(namespaceWrapper);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles()).thenReturn(Set.of());
    doReturn(AuthorizationDecision.allow())
        .when(authorizer)
        .decide(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            eq(PolarisAuthorizableOperation.LIST_NAMESPACES),
            ArgumentMatchers.any(),
            ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any());

    AuthorizationRequest request =
        new AuthorizationRequest(
            principal,
            List.of(
                new SingleTargetAuthorizationIntent(
                    PolarisAuthorizableOperation.LIST_NAMESPACES,
                    PolarisSecurable.of(
                        new PathSegment(PolarisEntityType.CATALOG, "catalog"),
                        new PathSegment(PolarisEntityType.NAMESPACE, "ns")))));

    AuthorizationDecision decision = authorizer.authorize(authzState, request);

    assertThat(decision.isAllowed()).isTrue();
    verify(manifest)
        .getResolvedPath(ResolvedPathKey.of(List.of("ns"), PolarisEntityType.NAMESPACE), true);
    verify(authorizer)
        .decide(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.LIST_NAMESPACES),
            eq(List.of(namespaceWrapper)),
            eq(null));
  }

  @Test
  void authorizeSingleOperationMultiIntentRequestEvaluatesSequentially() {
    PolarisAuthorizerImpl authorizer = spy(new PolarisAuthorizerImpl(mock(RealmConfig.class)));
    AuthorizationState authzState = new AuthorizationState();
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper firstCatalogWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisResolvedPathWrapper secondCatalogWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    authzState.setResolutionManifest(manifest);
    when(manifest.getResolvedTopLevelEntity("catalog1", PolarisEntityType.CATALOG))
        .thenReturn(firstCatalogWrapper);
    when(manifest.getResolvedTopLevelEntity("catalog2", PolarisEntityType.CATALOG))
        .thenReturn(secondCatalogWrapper);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles()).thenReturn(Set.of());
    doReturn(AuthorizationDecision.allow())
        .when(authorizer)
        .decide(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            eq(PolarisAuthorizableOperation.GET_CATALOG),
            ArgumentMatchers.any(),
            ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any());

    AuthorizationDecision decision =
        authorizer.authorize(
            authzState,
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
            eq(List.of(firstCatalogWrapper)),
            eq(null));
    verify(authorizer, times(1))
        .decide(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.GET_CATALOG),
            eq(List.of(secondCatalogWrapper)),
            eq(null));
  }

  @Test
  void authorizeUpdateTableMultiIntentRequestEvaluatesSequentially() {
    PolarisAuthorizerImpl authorizer = spy(new PolarisAuthorizerImpl(mock(RealmConfig.class)));
    AuthorizationState authzState = new AuthorizationState();
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper tableWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    authzState.setResolutionManifest(manifest);
    when(manifest.getResolvedPath(
            ResolvedPathKey.of(List.of("ns", "table"), PolarisEntityType.TABLE_LIKE), true))
        .thenReturn(tableWrapper);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles()).thenReturn(Set.of());
    doReturn(AuthorizationDecision.allow())
        .when(authorizer)
        .decide(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            any(PolarisAuthorizableOperation.class),
            ArgumentMatchers.any(),
            ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any());

    PolarisSecurable tableTarget =
        PolarisSecurable.of(
            new PathSegment(PolarisEntityType.CATALOG, "catalog"),
            new PathSegment(PolarisEntityType.NAMESPACE, "ns"),
            new PathSegment(PolarisEntityType.TABLE_LIKE, "table"));

    AuthorizationDecision decision =
        authorizer.authorize(
            authzState,
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
            eq(List.of(tableWrapper)),
            eq(null));
    verify(authorizer, times(1))
        .decide(
            eq(principal),
            eq(Set.of()),
            eq(PolarisAuthorizableOperation.SET_TABLE_SNAPSHOT_REF),
            eq(List.of(tableWrapper)),
            eq(null));
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
    PolarisAuthorizerImpl authorizer = spy(new PolarisAuthorizerImpl(mock(RealmConfig.class)));
    AuthorizationState authzState = new AuthorizationState();
    PolarisResolutionManifest manifest = mock(PolarisResolutionManifest.class);
    PolarisResolvedPathWrapper catalogWrapper = mock(PolarisResolvedPathWrapper.class);
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of("role"));

    authzState.setResolutionManifest(manifest);
    when(manifest.getResolvedTopLevelEntity("catalog", PolarisEntityType.CATALOG))
        .thenReturn(catalogWrapper);
    when(manifest.getAllActivatedCatalogRoleAndPrincipalRoles()).thenReturn(Set.of());
    doReturn(AuthorizationDecision.deny("missing privilege"))
        .when(authorizer)
        .decide(
            any(PolarisPrincipal.class),
            ArgumentMatchers.any(),
            eq(PolarisAuthorizableOperation.GET_CATALOG),
            ArgumentMatchers.any(),
            ArgumentMatchers.<List<PolarisResolvedPathWrapper>>any());

    AuthorizationRequest request =
        new AuthorizationRequest(
            principal,
            List.of(
                new SingleTargetAuthorizationIntent(
                    PolarisAuthorizableOperation.GET_CATALOG,
                    PolarisSecurable.of(new PathSegment(PolarisEntityType.CATALOG, "catalog")))));

    AuthorizationDecision decision = authorizer.authorize(authzState, request);

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
    PolarisAuthorizerImpl authorizer =
        new PolarisAuthorizerImpl(realmConfigWithRotationEnforcement(true));
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
    PolarisAuthorizerImpl authorizer =
        new PolarisAuthorizerImpl(realmConfigWithRotationEnforcement(false));
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
    PolarisAuthorizerImpl authorizer =
        new PolarisAuthorizerImpl(realmConfigWithRotationEnforcement(false));
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
    PolarisAuthorizerImpl authorizer = spy(new PolarisAuthorizerImpl(mock(RealmConfig.class)));
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
    PolarisAuthorizerImpl authorizer = spy(new PolarisAuthorizerImpl(mock(RealmConfig.class)));
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
    PolarisAuthorizerImpl authorizer = spy(new PolarisAuthorizerImpl(mock(RealmConfig.class)));
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
          public void resolveAuthorizationInputs(
              AuthorizationState authzState, AuthorizationRequest request) {}

          @Override
          public AuthorizationDecision authorize(
              AuthorizationState authzState, AuthorizationRequest request) {
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
}
