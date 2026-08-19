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
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.extension.orchestration.DefaultDurableOrchestrator;
import org.apache.polaris.extension.primitives.routing.MappedDurableRecordStoreLocator;
import org.apache.polaris.extension.primitives.routing.RoutingDurableRecordStore;
import org.apache.polaris.persistence.treemap.TreeMapDurableRecordStore;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.RecordRef;
import org.junit.jupiter.api.Test;

/**
 * Drives {@code createEntityIfNotExists}'s lost-race branch deterministically — the branch {@code
 * mapFailedCreate} runs when a competing writer lands a row at the same uniqueness key between the
 * manager's pre-check read and its commit's {@code notExists} evaluation. {@code
 * isIdempotentRetry}'s own javadoc records why no test on the stack could reach this branch before:
 * real threads cannot be relied on to interleave into the window, and two same-key creates inside
 * one commit roll each other back leaving no winner. {@link CommitPreemptingDurableRecordStore} is
 * the missing hook: it plants the competing row inside the FIRST commit call, after the pre-check
 * has already read empty.
 *
 * <p>Assembled the same way {@link DefaultDurableManagerEntityOpsTest} assembles the manager, with
 * the preempting decorator wrapped around the base store, under routing.
 */
class DefaultDurableManagerCreateRaceTest {

  private static final PolarisCallContext CALL_CTX =
      new PolarisCallContext(() -> "testRealm", new NeverCallOldPrimitives());

  private static DefaultDurableManager managerOver(DurableRecordStore store) {
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
    return new DefaultDurableManager(
        Clock.systemUTC(),
        new PolarisDefaultDiagServiceImpl(),
        new DefaultDurableOrchestrator(primitives),
        primitives,
        PrincipalSecretsGenerator.RANDOM_SECRETS);
  }

  private static PolarisBaseEntity newCatalog(DefaultDurableManager manager, String name) {
    long id = manager.generateNewEntityId(CALL_CTX).getId();
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

  /** The single CREATE mutation createEntityIfNotExists commits, re-read from the interception. */
  private static Mutation racedCreate(List<Mutation> intercepted) {
    assertThat(intercepted).hasSize(1);
    return intercepted.get(0);
  }

  @Test
  void aLostRaceAgainstTheSameIdIsAnIdempotentRetry() {
    // The competing writer creates the IDENTICAL row (same reserved id, same uniqueness tuple),
    // unconditioned — the retried-request shape isIdempotentRetry exists for.
    DurableRecordStore raced =
        new CommitPreemptingDurableRecordStore(
            new TreeMapDurableRecordStore(new PolarisDefaultDiagServiceImpl()),
            intercepted -> {
              Mutation m = racedCreate(intercepted);
              return List.of(Mutation.of(m.kind(), Mutation.Op.CREATE, m.target(), m.record()));
            });
    DefaultDurableManager manager = managerOver(raced);
    PolarisBaseEntity catalog = newCatalog(manager, "raced-idempotent");

    EntityResult result = manager.createEntityIfNotExists(CALL_CTX, null, catalog);

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.getEntity().getId()).isEqualTo(catalog.getId());
  }

  @Test
  void aLostRaceAgainstADifferentIdIsEntityAlreadyExists() {
    // The competing writer takes the SAME uniqueness tuple under a DIFFERENT id — a genuine
    // conflict, not a retry.
    DurableRecordStore raced =
        new CommitPreemptingDurableRecordStore(
            new TreeMapDurableRecordStore(new PolarisDefaultDiagServiceImpl()),
            intercepted -> {
              Mutation m = racedCreate(intercepted);
              PolarisBaseEntity creating = (PolarisBaseEntity) m.record();
              PolarisBaseEntity winner =
                  new PolarisBaseEntity.Builder(creating).id(creating.getId() + 424242L).build();
              return List.of(
                  Mutation.of(
                      m.kind(),
                      Mutation.Op.CREATE,
                      RecordRef.byIdentity(m.kind(), List.of(winner.getId())),
                      winner));
            });
    DefaultDurableManager manager = managerOver(raced);
    PolarisBaseEntity catalog = newCatalog(manager, "raced-conflict");

    EntityResult result = manager.createEntityIfNotExists(CALL_CTX, null, catalog);

    assertThat(result.isSuccess()).isFalse();
    assertThat(result.getReturnStatus()).isEqualTo(BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS);
  }
}
