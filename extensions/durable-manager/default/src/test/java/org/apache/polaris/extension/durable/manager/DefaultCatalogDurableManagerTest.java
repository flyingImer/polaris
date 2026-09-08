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
package org.apache.polaris.extension.durable.manager;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.extension.orchestration.DefaultDurableOrchestrator;
import org.apache.polaris.extension.primitives.routing.MappedDurableRecordStoreLocator;
import org.apache.polaris.extension.primitives.routing.RoutingDurableRecordStore;
import org.apache.polaris.persistence.treemap.TreeMapDurableRecordStore;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Drives only the eight operations ticket 91 implements — {@code generateNewEntityId}, {@code
 * loadEntity}, {@code readEntityByName}, {@code listEntities}, {@code listFullEntities}, {@code
 * loadEntitiesChangeTracking}, {@code createEntityIfNotExists}, {@code createEntitiesIfNotExist} —
 * against {@link DefaultCatalogDurableManager} assembled the same way {@link
 * AbstractDefaultDurableManagerTest} assembles it: a fresh {@link TreeMapDurableRecordStore} behind
 * {@link RoutingDurableRecordStore} and {@link DefaultDurableOrchestrator}.
 *
 * <p><b>This is scaffolding, not the ticket's parity evidence.</b> {@link
 * org.apache.polaris.core.persistence.BaseDurableManagerTest} is the judge of parity. As of
 * increment 4, {@code createPrincipal}/{@code createCatalog}/grants/secrets exist and {@code
 * bootstrapPolarisService} runs, so most of that fixture now passes against {@link
 * TreeMapDefaultDurableManagerTest} — the remaining red tests call {@code renameEntity}/{@code
 * updateEntityPropertiesIfNotChanged}/{@code dropEntityIfExists}/the resolved-entity-cache reads,
 * none of which increment 4 touches. These tests mirror the fixture's own assertions for the eight
 * operations in THIS class's scope, rather than inventing looser ones.
 */
class DefaultCatalogDurableManagerTest {

  private PolarisCallContext callCtx;
  private DefaultCatalogDurableManager manager;
  private DefaultResolvedEntityReads reads;

  @BeforeEach
  void setup() {
    DurableRecordStore store = new TreeMapDurableRecordStore(new PolarisDefaultDiagServiceImpl());
    DurableRecordStore primitives =
        new RoutingDurableRecordStore(
            new MappedDurableRecordStoreLocator(
                Map.of(
                    PolarisRecordKinds.ENTITY, "main",
                    PolarisRecordKinds.GRANT_RECORD, "main",
                    PolarisRecordKinds.POLICY_MAPPING, "main",
                    PolarisRecordKinds.PRINCIPAL_SECRETS, "main",
                    PolarisRecordKinds.EVENT, "main"),
                Map.of("main", store)),
            List.of(store),
            store);
    var orchestrator = new DefaultDurableOrchestrator(primitives);
    manager =
        new DefaultCatalogDurableManager(
            Clock.systemUTC(), new PolarisDefaultDiagServiceImpl(), orchestrator, primitives);
    reads =
        new DefaultResolvedEntityReads(
            primitives,
            manager,
            new DefaultGrantDurableManager(
                new PolarisDefaultDiagServiceImpl(), orchestrator, primitives));
    RealmContext realmContext = () -> "testRealm";
    callCtx = new PolarisCallContext(realmContext, new NeverCallOldPrimitives());
  }

  private PolarisBaseEntity newCatalog(String name) {
    long id = manager.generateNewEntityId(callCtx).getId();
    return new PolarisBaseEntity.Builder()
        .catalogId(PolarisEntityConstants.getNullId())
        .id(id)
        .typeCode(PolarisEntityType.CATALOG.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .parentId(PolarisEntityConstants.getRootEntityId())
        .name(name)
        .propertiesAsMap(Map.of())
        .internalPropertiesAsMap(Map.of())
        .build();
  }

  private PolarisBaseEntity newNamespace(PolarisBaseEntity catalog, String name) {
    long id = manager.generateNewEntityId(callCtx).getId();
    return new PolarisBaseEntity.Builder()
        .catalogId(catalog.getId())
        .id(id)
        .typeCode(PolarisEntityType.NAMESPACE.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .parentId(catalog.getId())
        .name(name)
        .propertiesAsMap(Map.of())
        .internalPropertiesAsMap(Map.of())
        .build();
  }

  @Test
  void createThenReadBackByNameAndById() {
    PolarisBaseEntity catalog = newCatalog("c1");
    EntityResult created = manager.createEntityIfNotExists(callCtx, null, catalog);
    assertThat(created.isSuccess()).isTrue();
    assertThat(created.getEntity().getId()).isEqualTo(catalog.getId());

    EntityResult byId =
        manager.loadEntity(
            callCtx, catalog.getCatalogId(), catalog.getId(), PolarisEntityType.CATALOG);
    assertThat(byId.isSuccess()).isTrue();
    assertThat(byId.getEntity().getName()).isEqualTo("c1");

    EntityResult byName =
        manager.readEntityByName(
            callCtx, null, PolarisEntityType.CATALOG, PolarisEntitySubType.NULL_SUBTYPE, "c1");
    assertThat(byName.isSuccess()).isTrue();
    assertThat(byName.getEntity().getId()).isEqualTo(catalog.getId());
  }

  @Test
  void listUnderAParent() {
    PolarisBaseEntity catalog = newCatalog("c2");
    manager.createEntityIfNotExists(callCtx, null, catalog);
    PolarisBaseEntity ns1 = newNamespace(catalog, "n1");
    PolarisBaseEntity ns2 = newNamespace(catalog, "n2");
    manager.createEntitiesIfNotExist(callCtx, null, List.of(ns1, ns2));

    List<EntityNameLookupRecord> listed =
        manager
            .listEntities(
                callCtx,
                List.of(catalog),
                PolarisEntityType.NAMESPACE,
                PolarisEntitySubType.NULL_SUBTYPE,
                PageToken.readEverything())
            .getEntities();
    assertThat(listed)
        .extracting(EntityNameLookupRecord::getName)
        .containsExactlyInAnyOrder("n1", "n2");

    List<PolarisBaseEntity> full =
        manager
            .listFullEntities(
                callCtx,
                List.of(catalog),
                PolarisEntityType.NAMESPACE,
                PolarisEntitySubType.NULL_SUBTYPE,
                PageToken.readEverything())
            .items();
    assertThat(full).extracting(PolarisBaseEntity::getName).containsExactlyInAnyOrder("n1", "n2");
  }

  @Test
  void batchCreateSucceeds() {
    PolarisBaseEntity catalog = newCatalog("c3");
    manager.createEntityIfNotExists(callCtx, null, catalog);
    PolarisBaseEntity ns1 = newNamespace(catalog, "n1");
    PolarisBaseEntity ns2 = newNamespace(catalog, "n2");

    List<PolarisBaseEntity> createdEntities =
        manager.createEntitiesIfNotExist(callCtx, null, List.of(ns1, ns2)).getEntities();
    assertThat(createdEntities)
        .hasSize(2)
        .extracting(PolarisBaseEntity::getId)
        .containsExactlyInAnyOrder(ns1.getId(), ns2.getId());
  }

  @Test
  void resubmittingABatchWithAlreadyCreatedIdsReturnsTheFullList() {
    PolarisBaseEntity catalog = newCatalog("c4");
    manager.createEntityIfNotExists(callCtx, null, catalog);
    PolarisBaseEntity ns1 = newNamespace(catalog, "n1");
    PolarisBaseEntity ns2 = newNamespace(catalog, "n2");
    manager.createEntitiesIfNotExist(callCtx, null, List.of(ns1, ns2));

    // n1 and n2 already exist with the same identifiers; n3 is genuinely new. Mirrors
    // BaseDurableManagerTest#testCreateEntitiesAlreadyExisting: the full list of three comes back.
    PolarisBaseEntity ns3 = newNamespace(catalog, "n3");
    List<PolarisBaseEntity> createdEntities =
        manager.createEntitiesIfNotExist(callCtx, null, List.of(ns1, ns2, ns3)).getEntities();
    assertThat(createdEntities)
        .hasSize(3)
        .extracting(PolarisBaseEntity::getId)
        .containsExactly(ns1.getId(), ns2.getId(), ns3.getId());
  }

  @Test
  void sameNameDifferentIdCreateReturnsAlreadyExists() {
    PolarisBaseEntity catalog = newCatalog("c5");
    manager.createEntityIfNotExists(callCtx, null, catalog);
    PolarisBaseEntity ns1 = newNamespace(catalog, "clash");
    manager.createEntityIfNotExists(callCtx, null, ns1);

    PolarisBaseEntity impostor = newNamespace(catalog, "clash");
    EntityResult result = manager.createEntityIfNotExists(callCtx, null, impostor);
    assertThat(result.isSuccess()).isFalse();
    assertThat(result.getReturnStatus()).isEqualTo(BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS);
  }

  @Test
  void batchWithARealConflictFailsTheWholeBatch() {
    PolarisBaseEntity catalog = newCatalog("c6");
    manager.createEntityIfNotExists(callCtx, null, catalog);
    PolarisBaseEntity ns1 = newNamespace(catalog, "clash");
    manager.createEntityIfNotExists(callCtx, null, ns1);

    // Mirrors BaseDurableManagerTest#testCreateEntitiesWithConflict: one real conflict fails the
    // whole batch, including the entity that would otherwise have succeeded.
    PolarisBaseEntity impostor = newNamespace(catalog, "clash");
    PolarisBaseEntity fresh = newNamespace(catalog, "fresh");
    var result = manager.createEntitiesIfNotExist(callCtx, null, List.of(impostor, fresh));
    assertThat(result.getEntities()).isNull();
    assertThat(
            manager
                .readEntityByName(
                    callCtx,
                    List.of(catalog),
                    PolarisEntityType.NAMESPACE,
                    PolarisEntitySubType.NULL_SUBTYPE,
                    "fresh")
                .isSuccess())
        .isFalse();
  }

  @Test
  void versionsOfAnUpdateFreeCreate() {
    PolarisBaseEntity catalog = newCatalog("c7");
    manager.createEntityIfNotExists(callCtx, null, catalog);

    var tracking =
        reads
            .loadEntitiesChangeTracking(
                callCtx, List.of(new PolarisEntityId(catalog.getCatalogId(), catalog.getId())))
            .getChangeTrackingVersions();
    assertThat(tracking).hasSize(1);
    assertThat(tracking.get(0).entityVersion()).isEqualTo(1);
    assertThat(tracking.get(0).grantRecordsVersion()).isEqualTo(1);
  }

  /**
   * Finding 1 (independent review, 2026-08-18): {@code DefaultCatalogDurableManager}'s lost-race
   * branch — reached when {@code createEntityIfNotExists}/{@code createEntitiesIfNotExist}'s
   * pre-check read sees nothing but the commit's {@code notExists} precondition fails anyway — must
   * apply the SAME id-equality rule the pre-check branch applies, not report {@code
   * ENTITY_ALREADY_EXISTS} unconditionally. {@link DefaultCatalogDurableManager#isIdempotentRetry}
   * is the single place that rule now lives.
   *
   * <p><b>Why this asserts the helper directly rather than driving the branch end to end:</b> the
   * race window is entirely INSIDE one method call, between its own pre-check read and its own
   * commit. Nothing outside that call can land a write into that exact window without either (a)
   * real concurrent threads, whose OS-scheduled interleaving is not guaranteed to land the second
   * thread's pre-check before the first thread's commit rather than after it — sometimes it will
   * just see the first thread's already-committed row and take the (already-correct) pre-check
   * branch instead, making the test flaky rather than deterministic — or (b) a test-only hook
   * inside {@code createEntityIfNotExists} itself, which does not exist and would be a production
   * change beyond this fix's scope. A same-batch construction (two entities sharing one uniqueness
   * key, submitted together) does not reach it either: both mutations would be in the SAME commit,
   * so the second one's failed precondition rolls back the WHOLE commit including the first —
   * nothing survives to be "the winner," unlike a genuine lost race where the winner is a separate,
   * already-committed transaction. Concluded there is no deterministic way to drive the actual
   * control-flow branch from a test without one of those two changes; asserting the extracted rule
   * directly is the honest alternative the brief allows, not a replacement for wanting the fuller
   * coverage.
   */
  @Test
  void idempotentRetryRuleMatchesIdEqualityNotUnconditionalConflict() {
    PolarisBaseEntity catalog = newCatalog("c8");
    manager.createEntityIfNotExists(callCtx, null, catalog);
    PolarisBaseEntity existing = newNamespace(catalog, "n1");

    // Same id as `existing`: the low-level retry AtomicOperationMetaStoreManager#persistNewEntity
    // treats as idempotent success at its one collision point.
    assertThat(DefaultCatalogDurableManager.isIdempotentRetry(existing, existing.getId())).isTrue();

    // A different id squatting the same slot: a genuine conflict, not a retry.
    long otherId = manager.generateNewEntityId(callCtx).getId();
    assertThat(DefaultCatalogDurableManager.isIdempotentRetry(existing, otherId)).isFalse();
  }
}
