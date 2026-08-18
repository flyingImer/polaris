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
package org.apache.polaris.extension.primitives.routing;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.extension.orchestration.DefaultDurableOrchestrator;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.JdbcDurableRecordStore;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.apache.polaris.persistence.treemap.TreeMapDurableRecordStore;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.OrchestrationResult;
import org.apache.polaris.spi.durable.RecordKind;
import org.apache.polaris.spi.durable.RecordRef;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The routing binding, end to end: the two-level {@code kind → store name → impl} mapping builds a
 * locator, the routing store fronts the two record stores — JDBC on H2 and the in-memory one —
 * behind ONE primitives handle, and both kinds serve through one orchestrator holding that handle;
 * remapping one kind's store name moves it with no other change. This is S11 exercised for real:
 * the integrator's whole act is one map entry, and nothing above the primitives SPI can tell.
 */
class RoutingAssembledTwoStoreTest {

  private static final PolarisDiagnostics DIAGNOSTICS = new PolarisDefaultDiagServiceImpl();
  private static final String REALM = "REALM";
  private static final int SCHEMA_VERSION = 4;
  private static final String MAIN = "main";
  private static final String AUTHZ = "authz";

  private JdbcDurableRecordStore jdbcStore;
  private TreeMapDurableRecordStore treeMapStore;

  /** Minimal configuration against the module's public interface; H2 stated explicitly. */
  private static final class H2Configuration implements RelationalJdbcConfiguration {
    @Override
    public Optional<Integer> maxRetries() {
      return Optional.empty();
    }

    @Override
    public Optional<Long> maxDurationInMs() {
      return Optional.empty();
    }

    @Override
    public Optional<Long> initialDelayInMs() {
      return Optional.empty();
    }

    @Override
    public Optional<String> databaseType() {
      return Optional.of("h2");
    }
  }

  @BeforeEach
  void setUp() throws SQLException {
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:factory_assembly_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1", "sa", "");
    DatasourceOperations datasourceOperations =
        new DatasourceOperations(dataSource, new H2Configuration());
    try (InputStream scriptStream = DatabaseType.H2.openInitScriptResource(SCHEMA_VERSION)) {
      datasourceOperations.executeScript(scriptStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    jdbcStore = new JdbcDurableRecordStore(datasourceOperations, REALM, SCHEMA_VERSION);
    treeMapStore = new TreeMapDurableRecordStore(DIAGNOSTICS);
  }

  private Map<String, DurableRecordStore> bothStores() {
    return Map.of(MAIN, jdbcStore, AUTHZ, treeMapStore);
  }

  /** The whole assembly for a mapping: locator, routing store, orchestrator over the one handle. */
  private DefaultDurableOrchestrator orchestratorFor(Map<RecordKind, String> mapping) {
    MappedDurableRecordStoreLocator locator =
        new MappedDurableRecordStoreLocator(mapping, bothStores());
    RoutingDurableRecordStore primitives =
        new RoutingDurableRecordStore(locator, List.of(jdbcStore, treeMapStore), jdbcStore);
    return new DefaultDurableOrchestrator(primitives);
  }

  private static PolarisBaseEntity entity(long id, String name) {
    return new PolarisBaseEntity.Builder()
        .catalogId(1L)
        .id(id)
        .typeCode(PolarisEntityType.NAMESPACE.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .parentId(1L)
        .name(name)
        .entityVersion(1)
        .propertiesAsMap(Map.of())
        .internalPropertiesAsMap(Map.of())
        .build();
  }

  private static RecordRef entityRef(long id) {
    return RecordRef.byIdentity(PolarisRecordKinds.ENTITY, List.of(id));
  }

  private static PolarisGrantRecord grant(long securableId, long granteeId, int privilegeCode) {
    return new PolarisGrantRecord(1L, securableId, 1L, granteeId, privilegeCode);
  }

  private static RecordRef grantRef(PolarisGrantRecord grant) {
    return RecordRef.byIdentity(
        PolarisRecordKinds.GRANT_RECORD,
        List.of(
            grant.getSecurableCatalogId(),
            grant.getSecurableId(),
            grant.getGranteeCatalogId(),
            grant.getGranteeId(),
            grant.getPrivilegeCode()));
  }

  private static Mutation createEntity(PolarisBaseEntity e) {
    return Mutation.of(PolarisRecordKinds.ENTITY, Mutation.Op.CREATE, entityRef(e.getId()), e);
  }

  private static Mutation createGrant(PolarisGrantRecord g) {
    return Mutation.of(PolarisRecordKinds.GRANT_RECORD, Mutation.Op.CREATE, grantRef(g), g);
  }

  @Test
  void kindsSplitAcrossTheTwoStoresBothServeThroughOneOrchestrator() {
    // Entities are mapped to the JDBC store, grant records to their own store name wired to the
    // in-memory store; the orchestrator holds one primitives handle and cannot tell.
    DefaultDurableOrchestrator orchestrator =
        orchestratorFor(
            Map.of(PolarisRecordKinds.ENTITY, MAIN, PolarisRecordKinds.GRANT_RECORD, AUTHZ));

    PolarisBaseEntity catalog = entity(10L, "catalog");
    PolarisGrantRecord g = grant(10L, 20L, 3);
    OrchestrationResult result =
        orchestrator.commit(List.of(createEntity(catalog), createGrant(g)));

    assertThat(result.isApplied()).isTrue();
    // Each kind landed in its mapped store and only there: both stores hold bindings for both
    // kinds, so absence is a routing fact, not a capability one.
    assertThat(jdbcStore.get(entityRef(10L), PolarisBaseEntity.class)).isPresent();
    assertThat(treeMapStore.get(entityRef(10L), PolarisBaseEntity.class)).isEmpty();
    assertThat(treeMapStore.get(grantRef(g), PolarisGrantRecord.class)).isPresent();
    assertThat(jdbcStore.get(grantRef(g), PolarisGrantRecord.class)).isEmpty();
  }

  @Test
  void remappingOneKindsStoreNameMovesItWithNoOtherChange() {
    // Under mapping A both kinds name the JDBC store: co-location, stated explicitly (there is no
    // default entry to ride).
    Map<RecordKind, String> mappingA = new HashMap<>();
    mappingA.put(PolarisRecordKinds.ENTITY, MAIN);
    mappingA.put(PolarisRecordKinds.GRANT_RECORD, MAIN);
    PolarisGrantRecord before = grant(10L, 20L, 3);
    assertThat(
            orchestratorFor(mappingA)
                .commit(List.of(createEntity(entity(10L, "catalog")), createGrant(before)))
                .isApplied())
        .isTrue();
    assertThat(jdbcStore.get(grantRef(before), PolarisGrantRecord.class)).isPresent();

    // The integrator's whole change: ONE map entry's value. Same stores, same construction path.
    Map<RecordKind, String> mappingB = new HashMap<>(mappingA);
    mappingB.put(PolarisRecordKinds.GRANT_RECORD, AUTHZ);
    assertThat(mappingB).hasSize(mappingA.size());

    PolarisGrantRecord after = grant(10L, 21L, 3);
    assertThat(
            orchestratorFor(mappingB)
                .commit(List.of(createEntity(entity(11L, "catalog2")), createGrant(after)))
                .isApplied())
        .isTrue();

    // The remapped kind moved; the entity kind did not.
    assertThat(treeMapStore.get(grantRef(after), PolarisGrantRecord.class)).isPresent();
    assertThat(jdbcStore.get(grantRef(after), PolarisGrantRecord.class)).isEmpty();
    assertThat(jdbcStore.get(entityRef(11L), PolarisBaseEntity.class)).isPresent();
  }
}
