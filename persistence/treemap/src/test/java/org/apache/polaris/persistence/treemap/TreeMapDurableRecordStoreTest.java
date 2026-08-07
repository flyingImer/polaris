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
package org.apache.polaris.persistence.treemap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.Precondition;
import org.apache.polaris.spi.durable.RecordKind;
import org.apache.polaris.spi.durable.RecordRef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises the claims {@link TreeMapDurableRecordStore}'s javadoc makes, so "implemented" means
 * "ran" rather than "compiled".
 *
 * <p>This is not the conformance suite. That suite runs the same cases against every store and is
 * separate work; these are the cases whose failure would mean the class does not do what it says.
 */
class TreeMapDurableRecordStoreTest {

  private static final PolarisDiagnostics DIAGNOSTICS = new PolarisDefaultDiagServiceImpl();

  private TreeMapDurableRecordStore store;

  @BeforeEach
  void setUp() {
    store = new TreeMapDurableRecordStore(new TreeMapSlices(DIAGNOSTICS), DIAGNOSTICS);
  }

  private static PolarisBaseEntity entity(long id, long parentId, String name, int version) {
    return new PolarisBaseEntity.Builder()
        .catalogId(1L)
        .id(id)
        .typeCode(2)
        .subTypeCode(0)
        .parentId(parentId)
        .name(name)
        .entityVersion(version)
        .propertiesAsMap(Map.of())
        .internalPropertiesAsMap(Map.of())
        .build();
  }

  private static RecordRef entityRef(long id) {
    return RecordRef.byIdentity(PolarisRecordKinds.ENTITY, List.of(id));
  }

  @Test
  void commitAppliesEveryMutationOrNone() {
    CommitResult result =
        store.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.ENTITY,
                    Mutation.Op.CREATE,
                    entityRef(10L),
                    entity(10L, 1L, "catalog", 1)),
                Mutation.of(
                    PolarisRecordKinds.GRANT_RECORD,
                    Mutation.Op.CREATE,
                    RecordRef.byIdentity(
                        PolarisRecordKinds.GRANT_RECORD, List.of(1L, 10L, 1L, 20L, 3)),
                    new PolarisGrantRecord(1L, 10L, 1L, 20L, 3))));

    assertThat(result.isApplied()).isTrue();
    assertThat(store.get(entityRef(10L), PolarisBaseEntity.class)).isPresent();
    assertThat(
            store.get(
                RecordRef.byIdentity(PolarisRecordKinds.GRANT_RECORD, List.of(1L, 10L, 1L, 20L, 3)),
                PolarisGrantRecord.class))
        .isPresent();
  }

  @Test
  void aFailedPreconditionRollsBackEveryOtherMutationInTheCommit() {
    assertThat(store.commit(createOf(entity(10L, 1L, "catalog", 1))).isApplied()).isTrue();

    // Two mutations: the first would succeed on its own, the second names a stale version.
    CommitResult result =
        store.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.ENTITY,
                    Mutation.Op.CREATE,
                    entityRef(11L),
                    entity(11L, 1L, "namespace", 1)),
                Mutation.of(
                    PolarisRecordKinds.ENTITY,
                    Mutation.Op.UPDATE,
                    entityRef(10L),
                    entity(10L, 1L, "catalog", 2),
                    List.of(
                        Precondition.versionEquals(
                            entityRef(10L), Precondition.VersionAttribute.RECORD_VERSION, 99L)))));

    assertThat(result.isApplied()).isFalse();
    assertThat(result.failure()).contains(CommitResult.Failure.PRECONDITION_FAILED);
    // the sibling create must not have survived, and the stale update must not have applied
    assertThat(store.get(entityRef(11L), PolarisBaseEntity.class)).isEmpty();
    assertThat(store.get(entityRef(10L), PolarisBaseEntity.class))
        .get()
        .extracting(PolarisBaseEntity::getEntityVersion)
        .isEqualTo(1);
  }

  @Test
  void aVersionPreconditionThatHoldsLetsTheUpdateThrough() {
    assertThat(store.commit(createOf(entity(10L, 1L, "catalog", 1))).isApplied()).isTrue();

    CommitResult result =
        store.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.ENTITY,
                    Mutation.Op.UPDATE,
                    entityRef(10L),
                    entity(10L, 1L, "catalog", 2),
                    List.of(
                        Precondition.versionEquals(
                            entityRef(10L), Precondition.VersionAttribute.RECORD_VERSION, 1L)))));

    assertThat(result.isApplied()).isTrue();
    assertThat(store.get(entityRef(10L), PolarisBaseEntity.class))
        .get()
        .extracting(PolarisBaseEntity::getEntityVersion)
        .isEqualTo(2);
  }

  @Test
  void creatingTwiceFailsTheMustNotExistCondition() {
    assertThat(store.commit(createOf(entity(10L, 1L, "catalog", 1))).isApplied()).isTrue();
    CommitResult second = store.commit(createOf(entity(10L, 1L, "catalog", 1)));
    assertThat(second.isApplied()).isFalse();
    assertThat(second.failure()).contains(CommitResult.Failure.PRECONDITION_FAILED);
  }

  @Test
  void deleteRemovesTheRecord() {
    PolarisBaseEntity e = entity(10L, 1L, "catalog", 1);
    assertThat(store.commit(createOf(e)).isApplied()).isTrue();
    assertThat(
            store
                .commit(
                    List.of(
                        Mutation.of(
                            PolarisRecordKinds.ENTITY, Mutation.Op.DELETE, entityRef(10L), e)))
                .isApplied())
        .isTrue();
    assertThat(store.get(entityRef(10L), PolarisBaseEntity.class)).isEmpty();
  }

  @Test
  void aReferenceByUniquenessKeyResolvesTheSameRecordAsItsIdentity() {
    assertThat(store.commit(createOf(entity(10L, 1L, "catalog", 1))).isApplied()).isTrue();
    RecordRef byName =
        RecordRef.byUniquenessKey(PolarisRecordKinds.ENTITY, List.of(1L, 2, "catalog"));
    assertThat(store.get(byName, PolarisBaseEntity.class))
        .get()
        .extracting(PolarisBaseEntity::getId)
        .isEqualTo(10L);
  }

  @Test
  void overTheItemCapIsRejectedRatherThanSplit() {
    TreeMapDurableRecordStore small =
        new TreeMapDurableRecordStore(new TreeMapSlices(DIAGNOSTICS), DIAGNOSTICS, 1);
    CommitResult result =
        small.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.ENTITY,
                    Mutation.Op.CREATE,
                    entityRef(10L),
                    entity(10L, 1L, "a", 1)),
                Mutation.of(
                    PolarisRecordKinds.ENTITY,
                    Mutation.Op.CREATE,
                    entityRef(11L),
                    entity(11L, 1L, "b", 1))));
    assertThat(result.failure()).contains(CommitResult.Failure.TOO_MANY_ITEMS);
    assertThat(small.get(entityRef(10L), PolarisBaseEntity.class)).isEmpty();
  }

  @Test
  void anUnregisteredKindIsRejectedRatherThanGuessedAt() {
    RecordKind unknown = RecordKind.of("someone-elses.thing");
    assertThatThrownBy(() -> store.get(RecordRef.byIdentity(unknown, List.of(1L)), Object.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No mapper registered");
  }

  @Test
  void twoStoresAreTwoAtomicityDomains() {
    TreeMapDurableRecordStore other =
        new TreeMapDurableRecordStore(new TreeMapSlices(DIAGNOSTICS), DIAGNOSTICS);
    assertThat(store.domainOf(entityRef(10L))).isEqualTo(store.domainOf(entityRef(11L)));
    assertThat(store.domainOf(entityRef(10L))).isNotEqualTo(other.domainOf(entityRef(10L)));
  }

  @Test
  void versionsOfReadsVersionsAndRejectsKindsThatHaveNone() {
    assertThat(store.commit(createOf(entity(10L, 1L, "catalog", 7))).isApplied()).isTrue();
    assertThat(store.versionsOf(List.of(entityRef(10L), entityRef(99L))))
        .satisfies(
            versions -> {
              assertThat(versions).hasSize(2);
              assertThat(versions.get(0)).get().extracting("recordVersion").isEqualTo(7L);
              assertThat(versions.get(1)).isEmpty();
            });

    assertThatThrownBy(
            () ->
                store.versionsOf(
                    List.of(
                        RecordRef.byIdentity(
                            PolarisRecordKinds.GRANT_RECORD, List.of(1L, 10L, 1L, 20L, 3)))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("carries no version");
  }

  @Test
  void generateNewIdDoesNotRepeat() {
    assertThat(store.generateNewId()).isNotEqualTo(store.generateNewId());
  }

  private static List<Mutation> createOf(PolarisBaseEntity e) {
    return List.of(
        Mutation.of(PolarisRecordKinds.ENTITY, Mutation.Op.CREATE, entityRef(e.getId()), e));
  }
}
