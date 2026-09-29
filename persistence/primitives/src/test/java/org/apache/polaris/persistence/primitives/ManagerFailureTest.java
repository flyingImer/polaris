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
package org.apache.polaris.persistence.primitives;

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;
import static org.apache.polaris.persistence.primitives.api.StorageFailure.Outcome.REJECTED;
import static org.apache.polaris.persistence.primitives.api.StorageFailure.Outcome.UNKNOWN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.AbstractList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.exceptions.CommitConflictException;
import org.apache.polaris.core.persistence.CommitOutcomeUnknownException;
import org.apache.polaris.core.persistence.CommitRejectedException;
import org.apache.polaris.core.persistence.ConfirmedTransactionConflictException;
import org.apache.polaris.core.persistence.EntityAlreadyExistsException;
import org.apache.polaris.core.persistence.transactional.TransactionalMetaStoreManagerImpl;
import org.apache.polaris.persistence.primitives.api.DurablePrimitives;
import org.apache.polaris.persistence.primitives.api.StorageFailure;
import org.apache.polaris.persistence.primitives.domain.DomainRecords;
import org.apache.polaris.persistence.primitives.domain.RecordTransactionalPersistence;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Fault injection around native attempts, while executing the actual upstream Java Manager. */
class ManagerFailureTest {
  private Faults backend;
  private TransactionalMetaStoreManagerImpl manager;
  private PolarisCallContext context;
  private String prefix;

  @BeforeEach
  void setup() {
    backend = new Faults(NativeBackends.open(System.getProperty("poc.backend", "h2")));
    var diagnostics = new PolarisDefaultDiagServiceImpl();
    String realm = "failure-" + UUID.randomUUID();
    prefix = HexFormat.of().formatHex(realm.getBytes(StandardCharsets.UTF_8)) + "/";
    var persistence =
        new RecordTransactionalPersistence(
            diagnostics, new DomainRecords(backend, realm), RANDOM_SECRETS);
    manager = new TransactionalMetaStoreManagerImpl(Clock.systemUTC(), diagnostics);
    context = new PolarisCallContext(() -> realm, persistence);
    manager.bootstrapPolarisService(context);
  }

  @AfterEach
  void close() {
    if (backend != null) backend.close();
  }

  private PolarisBaseEntity catalog() {
    return new PolarisBaseEntity(
        PolarisEntityConstants.getNullId(),
        manager.generateNewEntityId(context).getId(),
        PolarisEntityType.CATALOG,
        PolarisEntitySubType.NULL_SUBTYPE,
        PolarisEntityConstants.getRootEntityId(),
        "catalog");
  }

  private Map<String, String> snapshot() {
    try (var read = backend.delegate.begin()) {
      Map<String, String> result = new TreeMap<>();
      for (var e : read.scan(prefix, prefix + "~", Integer.MAX_VALUE))
        result.put(e.key(), Base64.getEncoder().encodeToString(e.value()));
      read.commit(List.of());
      return result;
    }
  }

  @Test
  void catalogRejectPublishesNothing() {
    var catalog = catalog();
    var before = snapshot();
    backend.rejectCommit = true;
    assertThatThrownBy(() -> manager.createCatalog(context, catalog, List.of()))
        .isInstanceOf(CommitRejectedException.class);
    assertThat(snapshot()).isEqualTo(before);
  }

  @Test
  void lostCatalogResponseIsUnknownWithoutReplay() {
    var catalog = catalog();
    int before = backend.commits;
    backend.loseResponse = true;
    assertThatThrownBy(() -> manager.createCatalog(context, catalog, List.of()))
        .isInstanceOf(CommitOutcomeUnknownException.class);
    assertThat(backend.commits).isEqualTo(before + 1);
    assertThat(
            manager
                .loadEntity(
                    context, catalog.getCatalogId(), catalog.getId(), PolarisEntityType.CATALOG)
                .getEntity())
        .isNotNull();
  }

  @Test
  void revokeFailureRollsBackGrantAndEndpointVersions() {
    var created = manager.createCatalog(context, catalog(), List.of());
    var before = snapshot();
    backend.failAfterWrites = 2;
    assertThatThrownBy(
            () ->
                manager.revokePrivilegeOnSecurableFromRole(
                    context,
                    created.getCatalogAdminRole(),
                    List.of(created.getCatalog()),
                    created.getCatalog(),
                    PolarisPrivilege.CATALOG_MANAGE_METADATA))
        .isInstanceOf(CommitRejectedException.class);
    assertThat(snapshot()).isEqualTo(before);
  }

  @Test
  void dropFailureRollsBackCleanupAndVersions() {
    var catalog = manager.createCatalog(context, catalog(), List.of()).getCatalog();
    var role =
        new PolarisBaseEntity(
            catalog.getId(),
            manager.generateNewEntityId(context).getId(),
            PolarisEntityType.CATALOG_ROLE,
            PolarisEntitySubType.NULL_SUBTYPE,
            catalog.getId(),
            "disposable-role");
    manager.createEntityIfNotExists(context, List.of(catalog), role);
    manager.grantPrivilegeOnSecurableToRole(
        context, role, List.of(catalog), catalog, PolarisPrivilege.CATALOG_MANAGE_METADATA);
    var current =
        manager
            .loadEntity(context, role.getCatalogId(), role.getId(), PolarisEntityType.CATALOG_ROLE)
            .getEntity();
    var before = snapshot();
    backend.failAfterWrites = 3;
    assertThatThrownBy(
            () -> manager.dropEntityIfExists(context, List.of(catalog), current, null, false))
        .isInstanceOf(CommitRejectedException.class);
    assertThat(snapshot()).isEqualTo(before);
  }

  @Test
  void bulkCreateRejectsTwoIdsWithSameNameBeforeAnyWrites() {
    var first = catalog();
    var second = catalog();
    var before = snapshot();
    assertThatThrownBy(
            () -> context.getMetaStore().writeEntities(context, List.of(first, second), null))
        .isInstanceOf(EntityAlreadyExistsException.class);
    assertThat(snapshot()).isEqualTo(before);
  }

  @Test
  void managerNamespaceCreateDeleteRaceCannotPublishAnOrphan() throws Exception {
    String name = System.getProperty("poc.backend", "h2");
    Assumptions.assumeTrue(
        name.equals("fdb") || name.equals("jdbc"),
        "H2 does not prove range isolation; Spanner emulator cannot rendezvous concurrent reads");
    var catalog = manager.createCatalog(context, catalog(), List.of()).getCatalog();
    var namespace =
        new PolarisBaseEntity(
            catalog.getId(),
            manager.generateNewEntityId(context).getId(),
            PolarisEntityType.NAMESPACE,
            PolarisEntitySubType.NULL_SUBTYPE,
            catalog.getId(),
            "parent");
    namespace = manager.createEntityIfNotExists(context, List.of(catalog), namespace).getEntity();
    var parent = namespace;
    var child =
        new PolarisBaseEntity(
            catalog.getId(),
            manager.generateNewEntityId(context).getId(),
            PolarisEntityType.NAMESPACE,
            PolarisEntitySubType.NULL_SUBTYPE,
            parent.getId(),
            "child");
    backend.rendezvous = new CountDownLatch(2);
    try (var workers = Executors.newFixedThreadPool(2)) {
      var drop =
          workers.submit(
              () -> {
                try {
                  return manager
                      .dropEntityIfExists(context, List.of(catalog), parent, null, false)
                      .isSuccess();
                } catch (ConfirmedTransactionConflictException e) {
                  return false;
                }
              });
      var create =
          workers.submit(
              () -> {
                try {
                  return manager
                      .createEntityIfNotExists(context, List.of(catalog, parent), child)
                      .isSuccess();
                } catch (ConfirmedTransactionConflictException e) {
                  return false;
                }
              });
      assertThat(drop.get(30, TimeUnit.SECONDS) && create.get(30, TimeUnit.SECONDS)).isFalse();
    } finally {
      backend.rendezvous = null;
    }
    var childState =
        manager.loadEntity(
            context, child.getCatalogId(), child.getId(), PolarisEntityType.NAMESPACE);
    if (childState.isSuccess()) {
      assertThat(
              manager
                  .loadEntity(
                      context, parent.getCatalogId(), parent.getId(), PolarisEntityType.NAMESPACE)
                  .isSuccess())
          .isTrue();
    }
  }

  @Test
  void changedUnmodifiedAncestorRejectsStaleValidation() {
    var catalog = manager.createCatalog(context, catalog(), List.of()).getCatalog();
    var child =
        new PolarisBaseEntity(
            catalog.getId(),
            manager.generateNewEntityId(context).getId(),
            PolarisEntityType.NAMESPACE,
            PolarisEntitySubType.NULL_SUBTYPE,
            catalog.getId(),
            "stale-child");
    var changed =
        new PolarisBaseEntity.Builder(catalog)
            .propertiesAsMap(Map.of("validation-input", "new"))
            .build();
    assertThat(manager.updateEntityPropertiesIfNotChanged(context, null, changed).isSuccess())
        .isTrue();
    var before = snapshot();
    assertThatThrownBy(() -> manager.createEntityIfNotExists(context, List.of(catalog), child))
        .isInstanceOf(CommitConflictException.class);
    assertThat(snapshot()).isEqualTo(before);
  }

  private PolarisBaseEntity namespaceAt(PolarisBaseEntity catalog, String name, String location) {
    return new PolarisBaseEntity.Builder(
            new PolarisBaseEntity(
                catalog.getId(),
                manager.generateNewEntityId(context).getId(),
                PolarisEntityType.NAMESPACE,
                PolarisEntitySubType.NULL_SUBTYPE,
                catalog.getId(),
                name))
        .propertiesAsMap(Map.of(PolarisEntityConstants.ENTITY_BASE_LOCATION, location))
        .build();
  }

  @Test
  void batchLocationValidationRejectsItsOwnOverlappingCreates() {
    var catalog = manager.createCatalog(context, catalog(), List.of()).getCatalog();
    var one = namespaceAt(catalog, "one", "s3://bucket/shared/");
    var two = namespaceAt(catalog, "two", "s3://bucket/shared/child/");
    var before = snapshot();
    assertThatThrownBy(
            () -> manager.createEntitiesIfNotExist(context, List.of(catalog), List.of(one, two)))
        .isInstanceOf(ForbiddenException.class);
    assertThat(snapshot()).isEqualTo(before);
  }

  @Test
  void viewMetadataLocationProtectsOverlapWithoutBecomingStorageRoot() {
    var catalog = manager.createCatalog(context, catalog(), List.of()).getCatalog();
    var namespace =
        manager
            .createEntityIfNotExists(
                context, List.of(catalog), namespaceAt(catalog, "views", "s3://bucket/views/"))
            .getEntity();
    var one =
        new PolarisBaseEntity.Builder(
                new PolarisBaseEntity(
                    catalog.getId(),
                    manager.generateNewEntityId(context).getId(),
                    PolarisEntityType.TABLE_LIKE,
                    PolarisEntitySubType.ICEBERG_VIEW,
                    namespace.getId(),
                    "one"))
            .internalPropertiesAsMap(
                Map.of(IcebergTableLikeEntity.LOCATION, "s3://bucket/views/shared/"))
            .build();
    var created =
        manager.createEntityIfNotExists(context, List.of(catalog, namespace), one).getEntity();
    assertThat(created.getPropertiesAsMap())
        .doesNotContainKey(PolarisEntityConstants.ENTITY_BASE_LOCATION);
    var two =
        new PolarisBaseEntity.Builder(one)
            .id(manager.generateNewEntityId(context).getId())
            .name("two")
            .internalPropertiesAsMap(
                Map.of(IcebergTableLikeEntity.LOCATION, "s3://bucket/views/shared/child/"))
            .build();
    var before = snapshot();
    assertThatThrownBy(
            () -> manager.createEntityIfNotExists(context, List.of(catalog, namespace), two))
        .isInstanceOf(ForbiddenException.class);
    assertThat(snapshot()).isEqualTo(before);
  }

  @Test
  void locationValidationProtectsSiblingsAndIgnoresSelfOnUnrelatedUpdate() {
    var catalog = manager.createCatalog(context, catalog(), List.of()).getCatalog();
    var one =
        manager
            .createEntityIfNotExists(
                context, List.of(catalog), namespaceAt(catalog, "one", "s3://bucket/shared/"))
            .getEntity();
    var two = namespaceAt(catalog, "two", "s3://bucket/shared/child/");
    assertThatThrownBy(() -> manager.createEntityIfNotExists(context, List.of(catalog), two))
        .isInstanceOf(ForbiddenException.class);
    var update =
        new PolarisBaseEntity.Builder(one)
            .propertiesAsMap(
                Map.of(
                    PolarisEntityConstants.ENTITY_BASE_LOCATION,
                    "s3://bucket/shared/",
                    "unrelated",
                    "value"))
            .build();
    assertThat(
            manager
                .updateEntityPropertiesIfNotChanged(context, List.of(catalog), update)
                .isSuccess())
        .isTrue();
  }

  @Test
  void concurrentLocationChecksCannotBothPublishOverlappingSiblings() throws Exception {
    String name = System.getProperty("poc.backend", "h2");
    Assumptions.assumeTrue(
        name.equals("fdb") || name.equals("jdbc"),
        "H2 does not prove range isolation; Spanner emulator cannot rendezvous concurrent reads");
    var catalog = manager.createCatalog(context, catalog(), List.of()).getCatalog();
    var one = namespaceAt(catalog, "one", "s3://bucket/shared/");
    var two = namespaceAt(catalog, "two", "s3://bucket/shared/child/");
    backend.rendezvous = new CountDownLatch(2);
    try (var workers = Executors.newFixedThreadPool(2)) {
      var first =
          workers.submit(
              () -> {
                try {
                  return manager
                      .createEntityIfNotExists(context, List.of(catalog), one)
                      .isSuccess();
                } catch (ConfirmedTransactionConflictException e) {
                  return false;
                }
              });
      var second =
          workers.submit(
              () -> {
                try {
                  return manager
                      .createEntityIfNotExists(context, List.of(catalog), two)
                      .isSuccess();
                } catch (ConfirmedTransactionConflictException e) {
                  return false;
                }
              });
      assertThat(first.get(30, TimeUnit.SECONDS) && second.get(30, TimeUnit.SECONDS)).isFalse();
    } finally {
      backend.rendezvous = null;
    }
    boolean oneExists =
        manager
            .loadEntity(context, one.getCatalogId(), one.getId(), PolarisEntityType.NAMESPACE)
            .isSuccess();
    boolean twoExists =
        manager
            .loadEntity(context, two.getCatalogId(), two.getId(), PolarisEntityType.NAMESPACE)
            .isSuccess();
    assertThat(oneExists && twoExists).isFalse();
  }

  private PrincipalEntity principal(String name) {
    return manager
        .createPrincipal(
            context,
            new PrincipalEntity.Builder()
                .setName(name)
                .setId(manager.generateNewEntityId(context).getId())
                .setCreateTimestamp(System.currentTimeMillis())
                .build())
        .getPrincipal();
  }

  @Test
  void atomicResetRejectRetainsOldCredentialsAndClientId() {
    var principal = principal("reset-user");
    var before = snapshot();
    backend.rejectCommit = true;
    assertThatThrownBy(
            () ->
                manager.resetPrincipalCredentialsIfSupported(
                    context, principal, "replacement-client", "replacement-secret"))
        .isInstanceOf(CommitRejectedException.class);
    assertThat(snapshot()).isEqualTo(before);
  }

  @Test
  void atomicResetUpdatesPrincipalAndSecretsTogether() {
    var principal = principal("reset-user");
    var result =
        manager
            .resetPrincipalCredentialsIfSupported(
                context, principal, "replacement-client", "replacement-secret")
            .orElseThrow();
    assertThat(result.principal().getClientId()).isEqualTo("replacement-client");
    assertThat(result.secrets().getMainSecret()).isEqualTo("replacement-secret");
    assertThat(manager.loadPrincipalSecrets(context, principal.getClientId()).getPrincipalSecrets())
        .isNull();
    assertThat(
            manager
                .loadPrincipalSecrets(context, "replacement-client")
                .getPrincipalSecrets()
                .getPrincipalId())
        .isEqualTo(principal.getId());
    assertThat(manager.findPrincipalById(context, principal.getId()).orElseThrow().getClientId())
        .isEqualTo("replacement-client");
  }

  @Test
  void atomicResetCannotTakeAnotherPrincipalsClientId() {
    var principal = principal("reset-user");
    var other = principal("other-user");
    var before = snapshot();
    assertThatThrownBy(
            () ->
                manager.resetPrincipalCredentialsIfSupported(
                    context, principal, other.getClientId(), "replacement-secret"))
        .isInstanceOf(AlreadyExistsException.class);
    assertThat(snapshot()).isEqualTo(before);
  }

  private static final class Faults implements DurablePrimitives {
    private final DurablePrimitives delegate;
    private boolean rejectCommit;
    private boolean loseResponse;
    private int failAfterWrites;
    private int commits;
    private volatile CountDownLatch rendezvous;

    Faults(DurablePrimitives delegate) {
      this.delegate = delegate;
    }

    @Override
    public Attempt begin() {
      return wrap(delegate.begin());
    }

    @Override
    public ReadView readView() {
      return delegate.readView();
    }

    @Override
    public void close() {
      delegate.close();
    }

    private Attempt wrap(Attempt nativeAttempt) {
      return new Attempt() {
        @Override
        public byte[] get(String key) {
          return nativeAttempt.get(key);
        }

        @Override
        public List<Entry> scan(String lo, String hi, int limit) {
          return nativeAttempt.scan(lo, hi, limit);
        }

        @Override
        public List<byte[]> getMany(List<String> keys) {
          return nativeAttempt.getMany(keys);
        }

        @Override
        public void commit(List<Mutation> changes) {
          CountDownLatch barrier = rendezvous;
          if (barrier != null) {
            barrier.countDown();
            try {
              if (!barrier.await(15, TimeUnit.SECONDS))
                throw new IllegalStateException("Attempts did not rendezvous");
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException(e);
            }
          }
          if (rejectCommit) {
            rejectCommit = false;
            throw new StorageFailure(REJECTED, new IllegalStateException("Injected rejection"));
          }
          int failAt = failAfterWrites;
          failAfterWrites = 0;
          nativeAttempt.commit(
              failAt <= 0
                  ? changes
                  : new AbstractList<>() {
                    @Override
                    public Mutation get(int index) {
                      if (index >= failAt)
                        throw new StorageFailure(
                            REJECTED,
                            new IllegalStateException(
                                "Injected failure while consuming final mutations"));
                      return changes.get(index);
                    }

                    @Override
                    public int size() {
                      return changes.size();
                    }
                  });
          commits++;
          if (loseResponse) {
            loseResponse = false;
            throw new StorageFailure(UNKNOWN, new IllegalStateException("Injected lost response"));
          }
        }

        @Override
        public void close() {
          nativeAttempt.close();
        }
      };
    }
  }
}
