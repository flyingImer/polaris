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
import org.apache.polaris.core.durable.conformance.CommitPreemptingDurableRecordStore;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.extension.orchestration.DefaultDurableOrchestrator;
import org.apache.polaris.extension.primitives.routing.MappedDurableRecordStoreLocator;
import org.apache.polaris.extension.primitives.routing.RoutingDurableRecordStore;
import org.apache.polaris.persistence.treemap.TreeMapDurableRecordStore;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.RecordRef;
import org.junit.jupiter.api.Test;

/**
 * Proves the batch update's loud-conflict claim: when one entity in a multi-entity update changes
 * between the manager's read and its commit, the whole batch fails and nothing is applied.
 *
 * <p>The manager's own staleness pre-check cannot see this window — it reads each row, compares
 * versions, and only then builds the mutations, so a change landing after that read is caught by the
 * committed conditions rather than by the pre-check. {@link CommitPreemptingDurableRecordStore} is
 * the hook that lands one deterministically, the same way the create-race case uses it.
 *
 * <p>Assembled like {@link DefaultCatalogDurableManagerCreateRaceTest}: routing over one store, one
 * orchestrator, one manager. The two entities are created through a manager over the bare store, so
 * only the batch update meets the preempting decorator.
 */
class DefaultCatalogDurableManagerUpdateRaceTest {

  private static final PolarisCallContext CALL_CTX =
      new PolarisCallContext(() -> "testRealm", new NeverCallOldPrimitives());

  private static DefaultCatalogDurableManager managerOver(DurableRecordStore store) {
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
    return new DefaultCatalogDurableManager(
        Clock.systemUTC(),
        new PolarisDefaultDiagServiceImpl(),
        new DefaultDurableOrchestrator(primitives),
        primitives);
  }

  private static PolarisBaseEntity newCatalog(DefaultCatalogDurableManager manager, String name) {
    return new PolarisBaseEntity.Builder()
        .catalogId(PolarisEntityConstants.getNullId())
        .id(manager.generateNewEntityId(CALL_CTX).getId())
        .typeCode(PolarisEntityType.CATALOG.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .parentId(PolarisEntityConstants.getRootEntityId())
        .name(name)
        .propertiesAsMap(Map.of())
        .internalPropertiesAsMap(Map.of())
        .build();
  }

  private static PolarisBaseEntity created(
      DefaultCatalogDurableManager manager, PolarisBaseEntity entity) {
    EntityResult result = manager.createEntityIfNotExists(CALL_CTX, null, entity);
    assertThat(result.isSuccess()).isTrue();
    return result.getEntity();
  }

  private static PolarisBaseEntity withProperty(PolarisBaseEntity entity, String value) {
    return new PolarisBaseEntity.Builder(entity).propertiesAsMap(Map.of("batched", value)).build();
  }

  private static PolarisBaseEntity readBack(DurableRecordStore store, long id) {
    return store
        .get(RecordRef.byIdentity(PolarisRecordKinds.ENTITY, List.of(id)), PolarisBaseEntity.class)
        .orElseThrow();
  }

  @Test
  void aConcurrentChangeToOneEntityFailsTheWholeBatchAndAppliesNothing() {
    TreeMapDurableRecordStore base =
        new TreeMapDurableRecordStore(new PolarisDefaultDiagServiceImpl());
    DefaultCatalogDurableManager plain = managerOver(base);
    PolarisBaseEntity first = created(plain, newCatalog(plain, "update-race-a"));
    PolarisBaseEntity second = created(plain, newCatalog(plain, "update-race-b"));

    // The competitor bumps the first entity's grant-records version inside the batch's own commit,
    // after the manager has already read both rows and fixed its conditions.
    DurableRecordStore raced =
        new CommitPreemptingDurableRecordStore(
            base,
            intercepted -> {
              Mutation batched = intercepted.get(0);
              // Built from the STORED row, not from the manager's intended new state. The intended
              // state already carries entityVersion + 1, so competing with it would trip the
              // record-version condition and this case would prove nothing about the grant-records
              // one. Leaving entityVersion alone makes the grant-records condition the only thing
              // that can refuse the commit.
              PolarisBaseEntity competitor =
                  new PolarisBaseEntity.Builder(first)
                      .grantRecordsVersion(first.getGrantRecordsVersion() + 7)
                      .build();
              return List.of(
                  Mutation.of(batched.kind(), Mutation.Op.UPDATE, batched.target(), competitor));
            });

    EntitiesResult result =
        managerOver(raced)
            .updateEntitiesPropertiesIfNotChanged(
                CALL_CTX,
                List.of(
                    new EntityWithPath(List.of(), withProperty(first, "one")),
                    new EntityWithPath(List.of(), withProperty(second, "two"))));

    assertThat(result.isSuccess()).isFalse();
    assertThat(result.getReturnStatus())
        .isEqualTo(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED);

    // Nothing from the batch survives: the entity that did not lose the race is untouched too.
    assertThat(readBack(base, second.getId()).getEntityVersion())
        .isEqualTo(second.getEntityVersion());
  }
}
