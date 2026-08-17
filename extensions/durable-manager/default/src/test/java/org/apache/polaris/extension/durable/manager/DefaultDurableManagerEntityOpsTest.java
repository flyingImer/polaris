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
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.extension.orchestration.DefaultDurableOrchestrator;
import org.apache.polaris.extension.primitives.factory.DefaultDurableRecordStoreFactory;
import org.apache.polaris.persistence.treemap.TreeMapDurableRecordStore;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Drives only the eight operations ticket 91 implements — {@code generateNewEntityId}, {@code
 * loadEntity}, {@code readEntityByName}, {@code listEntities}, {@code listFullEntities}, {@code
 * loadEntitiesChangeTracking}, {@code createEntityIfNotExists}, {@code createEntitiesIfNotExist} —
 * against {@link DefaultDurableManager} assembled the same way {@link
 * AbstractDefaultDurableManagerTest} assembles it: a fresh {@link TreeMapDurableRecordStore}
 * through {@link DefaultDurableRecordStoreFactory} and {@link DefaultDurableOrchestrator}.
 *
 * <p><b>This is scaffolding for this increment, not the ticket's parity evidence.</b> {@link
 * org.apache.polaris.core.persistence.BaseDurableManagerTest} is the judge of parity, and it cannot
 * run a single test yet: its {@code @BeforeEach} calls {@code bootstrapPolarisService}, which needs
 * {@code createPrincipal}/{@code createCatalog}/grants from later increments. These tests mirror
 * that fixture's own assertions for the eight operations already implemented, rather than inventing
 * looser ones, so the fixture flipping green at ticket 91's final increment is not a surprise.
 */
class DefaultDurableManagerEntityOpsTest {

  private PolarisCallContext callCtx;
  private DefaultDurableManager manager;

  @BeforeEach
  void setup() {
    DurableRecordStore store = new TreeMapDurableRecordStore(new PolarisDefaultDiagServiceImpl());
    var storeForKind =
        new DefaultDurableRecordStoreFactory().produce(Map.of(), "main", Map.of("main", store));
    var orchestrator = new DefaultDurableOrchestrator(storeForKind);
    manager =
        new DefaultDurableManager(
            Clock.systemUTC(), new PolarisDefaultDiagServiceImpl(), orchestrator, storeForKind);
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
        manager
            .loadEntitiesChangeTracking(
                callCtx, List.of(new PolarisEntityId(catalog.getCatalogId(), catalog.getId())))
            .getChangeTrackingVersions();
    assertThat(tracking).hasSize(1);
    assertThat(tracking.get(0).entityVersion()).isEqualTo(1);
    assertThat(tracking.get(0).grantRecordsVersion()).isEqualTo(1);
  }
}
