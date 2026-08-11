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
package org.apache.polaris.extension.orchestration;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.persistence.treemap.TreeMapDurableRecordStore;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.OrchestrationResult;
import org.apache.polaris.spi.durable.RecordKind;
import org.apache.polaris.spi.durable.RecordRef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The ticket's demo: two hand-wired stores, a mutation list spanning both, and the guarantee
 * observed end-to-end against real implementations. Entities live in one in-memory store, grant
 * records in another; each store is its own atomicity domain by construction, so a list touching
 * both kinds exercises the cross-domain path with no scripting.
 *
 * <p>This is the deployment shape the compensation decision names: an integrator maps grant records
 * to their own store, a multi-kind operation partially fails, and the failing request's own error
 * path leaves nothing behind, so the retry simply succeeds.
 */
class TwoStoreDemoTest {

  private static final PolarisDiagnostics DIAGNOSTICS = new PolarisDefaultDiagServiceImpl();

  private TreeMapDurableRecordStore entityStore;
  private TreeMapDurableRecordStore grantStore;
  private DefaultDurableOrchestrator orchestrator;

  @BeforeEach
  void setUp() {
    entityStore = new TreeMapDurableRecordStore(DIAGNOSTICS);
    grantStore = new TreeMapDurableRecordStore(DIAGNOSTICS);
    Map<RecordKind, DurableRecordStore> wiring =
        Map.of(
            PolarisRecordKinds.ENTITY, entityStore,
            PolarisRecordKinds.GRANT_RECORD, grantStore);
    orchestrator = new DefaultDurableOrchestrator(wiring::get);
  }

  private static PolarisBaseEntity entity(long id, String name) {
    return new PolarisBaseEntity.Builder()
        .catalogId(1L)
        .id(id)
        .typeCode(2)
        .subTypeCode(0)
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
  void aListSpanningBothStoresCommitsBothDomains() {
    PolarisBaseEntity catalog = entity(10L, "catalog");
    PolarisGrantRecord g = grant(10L, 20L, 3);

    OrchestrationResult result =
        orchestrator.commit(List.of(createEntity(catalog), createGrant(g)));

    assertThat(result.isApplied()).isTrue();
    assertThat(entityStore.get(entityRef(10L), PolarisBaseEntity.class)).isPresent();
    assertThat(grantStore.get(grantRef(g), PolarisGrantRecord.class)).isPresent();
  }

  @Test
  void aFailureInTheSecondStoreLeavesNothingBehindAndARetrySucceeds() {
    // Seed the grant store so the orchestrated create collides there.
    PolarisGrantRecord existing = grant(10L, 20L, 3);
    assertThat(grantStore.commit(List.of(createGrant(existing))).isApplied()).isTrue();

    PolarisBaseEntity catalog = entity(10L, "catalog");
    OrchestrationResult failed =
        orchestrator.commit(List.of(createEntity(catalog), createGrant(existing)));

    // The grant store's domain failed; the entity store's commit was rolled back synchronously,
    // on this same call. Nothing from the request remains.
    assertThat(failed.outcome()).isEqualTo(OrchestrationResult.Outcome.ROLLED_BACK);
    assertThat(failed.groupFailure()).isPresent();
    assertThat(failed.groupFailure().get().failure())
        .contains(CommitResult.Failure.PRECONDITION_FAILED);
    assertThat(entityStore.get(entityRef(10L), PolarisBaseEntity.class)).isEmpty();

    // An ordinary failure leaves nothing behind, so the retry simply succeeds.
    PolarisGrantRecord fresh = grant(10L, 21L, 3);
    OrchestrationResult retried =
        orchestrator.commit(List.of(createEntity(catalog), createGrant(fresh)));

    assertThat(retried.isApplied()).isTrue();
    assertThat(entityStore.get(entityRef(10L), PolarisBaseEntity.class)).isPresent();
    assertThat(grantStore.get(grantRef(fresh), PolarisGrantRecord.class)).isPresent();
  }

  @Test
  void aRolledBackUpdateRestoresThePriorState() {
    PolarisBaseEntity original = entity(10L, "catalog");
    assertThat(entityStore.commit(List.of(createEntity(original))).isApplied()).isTrue();
    PolarisGrantRecord existing = grant(10L, 20L, 3);
    assertThat(grantStore.commit(List.of(createGrant(existing))).isApplied()).isTrue();

    // Rename the entity and add a colliding grant in one list: the grant domain fails, and the
    // compensating update puts the prior entity state back.
    PolarisBaseEntity renamed =
        new PolarisBaseEntity.Builder(original).name("renamed").entityVersion(2).build();
    OrchestrationResult result =
        orchestrator.commit(
            List.of(
                Mutation.of(PolarisRecordKinds.ENTITY, Mutation.Op.UPDATE, entityRef(10L), renamed),
                createGrant(existing)));

    assertThat(result.outcome()).isEqualTo(OrchestrationResult.Outcome.ROLLED_BACK);
    assertThat(entityStore.get(entityRef(10L), PolarisBaseEntity.class))
        .get()
        .extracting(PolarisBaseEntity::getName)
        .isEqualTo("catalog");
  }
}
