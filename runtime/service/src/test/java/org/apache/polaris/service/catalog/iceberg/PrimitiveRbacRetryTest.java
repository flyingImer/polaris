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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipalAttributes;
import org.apache.polaris.core.collection.ImmutableAttributeMap;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.ConfirmedTransactionConflictException;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactoryImpl;
import org.apache.polaris.core.persistence.resolver.Resolver;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.identity.provider.DefaultServiceIdentityProvider;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

/** Real RBAC, persisted grants and one warm cache across a confirmed-conflict retry. */
class PrimitiveRbacRetryTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void warmCacheRetryRechecksPersistedRevocation(boolean revoke) {
    String realmName = "rbac-" + UUID.randomUUID();
    RealmContext stableRealm = () -> realmName;
    var factory = new PrimitiveServiceTestFactory();
    factory.bootstrapRealms(List.of(realmName), RootCredentialsSet.EMPTY);
    var context = new PolarisCallContext(stableRealm, factory.getOrCreateSession(stableRealm));
    var manager = Mockito.spy(factory.getOrCreateMetaStoreManager(stableRealm));
    var cache = factory.getOrCreateEntityCache(stableRealm, context.getRealmConfig());
    var diagnostics = new PolarisDefaultDiagServiceImpl();
    ResolverFactory resolvers =
        (principal, catalog) ->
            new Resolver(diagnostics, context, manager, principal, cache, catalog);
    var manifests = new ResolutionManifestFactoryImpl(diagnostics, stableRealm, resolvers);
    var authorizer = Mockito.spy(new PolarisAuthorizerImpl(context.getRealmConfig()));
    var root = manager.findRootPrincipal(context).orElseThrow();
    var rootAdmin =
        new PolarisAdminService(
            context,
            manifests,
            manager,
            Mockito.mock(UserSecretsManager.class),
            new DefaultServiceIdentityProvider(),
            authenticated(root),
            authorizer,
            ReservedProperties.NONE);
    var user =
        manager
            .createPrincipal(
                context,
                new PrincipalEntity.Builder()
                    .setName("retry-user")
                    .setId(manager.generateNewEntityId(context).getId())
                    .setCreateTimestamp(System.currentTimeMillis())
                    .build())
            .getPrincipal();
    var role =
        rootAdmin.createPrincipalRole(new PrincipalRoleEntity.Builder().setName("creator").build());
    assertThat(rootAdmin.assignPrincipalRole(user.getName(), role.getName()).isSuccess()).isTrue();
    assertThat(
            rootAdmin
                .grantPrivilegeOnRootContainerToPrincipalRole(
                    role.getName(), PolarisPrivilege.PRINCIPAL_ROLE_CREATE)
                .isSuccess())
        .isTrue();
    var userAuthorizer = Mockito.spy(new PolarisAuthorizerImpl(context.getRealmConfig()));
    var userAdmin =
        new PolarisAdminService(
            context,
            manifests,
            manager,
            Mockito.mock(UserSecretsManager.class),
            new DefaultServiceIdentityProvider(),
            authenticated(user),
            userAuthorizer,
            ReservedProperties.NONE);

    // This succeeds through the real authorizer and primes the cache used on both attempts.
    userAdmin.createPrincipalRole(new PrincipalRoleEntity.Builder().setName("warmup").build());
    assertThat(
            cache
                .getOrLoadEntityById(
                    context, role.getCatalogId(), role.getId(), PolarisEntityType.PRINCIPAL_ROLE)
                .cacheHit())
        .isTrue();
    Mockito.clearInvocations(userAuthorizer);
    var firstAttempt = new AtomicBoolean(true);
    var submissions = new AtomicInteger();
    Mockito.doAnswer(
            call -> {
              PolarisBaseEntity entity = call.getArgument(2);
              if (!entity.getName().equals("after-conflict")) return call.callRealMethod();
              submissions.incrementAndGet();
              if (firstAttempt.compareAndSet(true, false)) {
                if (revoke) {
                  assertThat(
                          rootAdmin
                              .revokePrivilegeOnRootContainerFromPrincipalRole(
                                  role.getName(), PolarisPrivilege.PRINCIPAL_ROLE_CREATE)
                              .isSuccess())
                      .isTrue();
                }
                throw new ConfirmedTransactionConflictException(
                    new IllegalStateException("confirmed abort"));
              }
              return call.callRealMethod();
            })
        .when(manager)
        .createEntityIfNotExists(any(), any(), any());

    var request = new PrincipalRoleEntity.Builder().setName("after-conflict").build();
    if (revoke) {
      assertThatThrownBy(() -> userAdmin.createPrincipalRole(request))
          .isInstanceOf(ForbiddenException.class);
      assertThat(submissions).hasValue(1);
      assertThat(manager.findPrincipalRoleByName(context, "after-conflict")).isEmpty();
    } else {
      userAdmin.createPrincipalRole(request);
      assertThat(submissions).hasValue(2);
      assertThat(manager.findPrincipalRoleByName(context, "after-conflict")).isPresent();
    }
    Mockito.verify(userAuthorizer, Mockito.times(2)).authorize(any(), any());
  }

  private static PolarisPrincipal authenticated(PrincipalEntity entity) {
    return PolarisPrincipal.of(
        entity.getName(),
        ImmutableAttributeMap.builder()
            .put(PolarisPrincipalAttributes.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, entity)
            .put(PolarisPrincipalAttributes.PRINCIPAL_ROLE_ALL_ATTRIBUTE_KEY, true)
            .build(),
        Set.of());
  }
}
