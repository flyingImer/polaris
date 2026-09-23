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
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.transactional.TransactionalMetaStoreManagerImpl;
import org.apache.polaris.persistence.primitives.api.DurablePrimitives;
import org.apache.polaris.persistence.primitives.api.StorageFailure;
import org.apache.polaris.persistence.primitives.domain.DomainRecords;
import org.apache.polaris.persistence.primitives.domain.RecordTransactionalPersistence;
import org.junit.jupiter.api.AfterEach;
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
        .isInstanceOfSatisfying(
            StorageFailure.class, e -> assertThat(e.outcome()).isEqualTo(REJECTED));
    assertThat(snapshot()).isEqualTo(before);
  }

  @Test
  void lostCatalogResponseIsUnknownWithoutReplay() {
    var catalog = catalog();
    int before = backend.commits;
    backend.loseResponse = true;
    assertThatThrownBy(() -> manager.createCatalog(context, catalog, List.of()))
        .isInstanceOfSatisfying(
            StorageFailure.class, e -> assertThat(e.outcome()).isEqualTo(UNKNOWN));
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
        .isInstanceOf(StorageFailure.class);
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
        .isInstanceOf(StorageFailure.class);
    assertThat(snapshot()).isEqualTo(before);
  }

  private static final class Faults implements DurablePrimitives {
    private final DurablePrimitives delegate;
    private boolean rejectCommit;
    private boolean loseResponse;
    private int failAfterWrites;
    private int commits;

    Faults(DurablePrimitives delegate) {
      this.delegate = delegate;
    }

    @Override
    public Attempt begin() {
      return wrap(delegate.begin());
    }

    @Override
    public LegacyAttempt beginLegacy() {
      return wrap(delegate.beginLegacy());
    }

    @Override
    public void close() {
      delegate.close();
    }

    private LegacyAttempt wrap(Attempt nativeAttempt) {
      return new LegacyAttempt() {
        @Override
        public byte[] get(String key) {
          return nativeAttempt.get(key);
        }

        @Override
        public List<Entry> scan(String lo, String hi, int limit) {
          return nativeAttempt.scan(lo, hi, limit);
        }

        @Override
        public void applyForLegacyReadYourWrites(List<Mutation> changes) {
          ((LegacyAttempt) nativeAttempt).applyForLegacyReadYourWrites(changes);
          if (failAfterWrites > 0 && --failAfterWrites == 0)
            throw new StorageFailure(
                REJECTED, new IllegalStateException("Injected pre-commit failure"));
        }

        @Override
        public void commit(List<Mutation> changes) {
          if (rejectCommit) {
            rejectCommit = false;
            throw new StorageFailure(REJECTED, new IllegalStateException("Injected rejection"));
          }
          nativeAttempt.commit(changes);
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
