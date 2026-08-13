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
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.LookupPath;
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
    store = new TreeMapDurableRecordStore(DIAGNOSTICS);
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
    // null payload: a DELETE is addressed by its target ref alone (contract, 2026-08-13)
    assertThat(
            store
                .commit(
                    List.of(
                        Mutation.of(
                            PolarisRecordKinds.ENTITY, Mutation.Op.DELETE, entityRef(10L), null)))
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
    TreeMapDurableRecordStore small = new TreeMapDurableRecordStore(DIAGNOSTICS, 1);
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
    TreeMapDurableRecordStore other = new TreeMapDurableRecordStore(DIAGNOSTICS);
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

  // ---------------------------------------------------------------- declared lookup paths

  private static final PageToken EVERYTHING = PageToken.readEverything();

  @Test
  void listByParentReturnsTheChildrenOfExactlyThatParent() {
    assertThat(store.commit(createOf(entity(10L, 1L, "a", 1))).isApplied()).isTrue();
    assertThat(store.commit(createOf(entity(11L, 1L, "b", 1))).isApplied()).isTrue();
    assertThat(store.commit(createOf(entity(12L, 2L, "c", 1))).isApplied()).isTrue();

    assertThat(
            store
                .list(
                    PolarisRecordKinds.ENTITY,
                    PolarisRecordKinds.ENTITY_BY_PARENT,
                    List.of(1L, 1L),
                    EVERYTHING,
                    PolarisBaseEntity.class)
                .items())
        .extracting(PolarisBaseEntity::getId)
        .containsExactlyInAnyOrder(10L, 11L);
  }

  @Test
  void theOptionalTrailingSubtypeAnchorNarrowsTheParentListing() {
    assertThat(store.commit(createOf(entity(10L, 1L, "plain", 1))).isApplied()).isTrue();
    PolarisBaseEntity subtyped =
        new PolarisBaseEntity.Builder(entity(11L, 1L, "subtyped", 1)).subTypeCode(42).build();
    assertThat(store.commit(createOf(subtyped)).isApplied()).isTrue();

    assertThat(
            store
                .list(
                    PolarisRecordKinds.ENTITY,
                    PolarisRecordKinds.ENTITY_BY_PARENT,
                    List.of(1L, 1L, 42),
                    EVERYTHING,
                    PolarisBaseEntity.class)
                .items())
        .extracting(PolarisBaseEntity::getId)
        .containsExactly(11L);
  }

  @Test
  void listByLocationPrefixHonoursBothTheCatalogAnchorAndThePrefix() {
    PolarisBaseEntity inWarehouse =
        new PolarisBaseEntity.Builder(entity(10L, 1L, "t1", 1))
            .propertiesAsMap(
                Map.of(PolarisEntityConstants.ENTITY_BASE_LOCATION, "s3://bucket/warehouse/t1"))
            .build();
    PolarisBaseEntity elsewhere =
        new PolarisBaseEntity.Builder(entity(11L, 1L, "t2", 1))
            .propertiesAsMap(
                Map.of(PolarisEntityConstants.ENTITY_BASE_LOCATION, "s3://bucket/elsewhere/t2"))
            .build();
    assertThat(store.commit(createOf(inWarehouse)).isApplied()).isTrue();
    assertThat(store.commit(createOf(elsewhere)).isApplied()).isTrue();

    assertThat(
            store
                .list(
                    PolarisRecordKinds.ENTITY,
                    PolarisRecordKinds.ENTITY_BY_LOCATION_PREFIX,
                    // the store strips the scheme, mirroring the shipped overlap query
                    List.of(1L, "s3://bucket/warehouse"),
                    EVERYTHING,
                    PolarisBaseEntity.class)
                .items())
        .extracting(PolarisBaseEntity::getId)
        .containsExactly(10L);

    // the catalog anchor is part of the declared signature, not decoration
    assertThat(
            store
                .list(
                    PolarisRecordKinds.ENTITY,
                    PolarisRecordKinds.ENTITY_BY_LOCATION_PREFIX,
                    List.of(9L, "s3://bucket/warehouse"),
                    EVERYTHING,
                    PolarisBaseEntity.class)
                .items())
        .isEmpty();
  }

  @Test
  void bySecurableAndByGranteeAreDistinctDirectionsNotOneSymmetricMatch() {
    PolarisGrantRecord grant = new PolarisGrantRecord(1L, 10L, 1L, 20L, 3);
    assertThat(
            store
                .commit(
                    List.of(
                        Mutation.of(
                            PolarisRecordKinds.GRANT_RECORD,
                            Mutation.Op.CREATE,
                            RecordRef.byIdentity(
                                PolarisRecordKinds.GRANT_RECORD, List.of(1L, 10L, 1L, 20L, 3)),
                            grant)))
                .isApplied())
        .isTrue();

    assertThat(
            store
                .list(
                    PolarisRecordKinds.GRANT_RECORD,
                    PolarisRecordKinds.GRANT_RECORD_BY_SECURABLE,
                    List.of(1L, 10L),
                    EVERYTHING,
                    PolarisGrantRecord.class)
                .items())
        .hasSize(1);
    assertThat(
            store
                .list(
                    PolarisRecordKinds.GRANT_RECORD,
                    PolarisRecordKinds.GRANT_RECORD_BY_GRANTEE,
                    List.of(1L, 20L),
                    EVERYTHING,
                    PolarisGrantRecord.class)
                .items())
        .hasSize(1);
    // the grantee's address on the securable path matches nothing: each direction is its own path
    assertThat(
            store
                .list(
                    PolarisRecordKinds.GRANT_RECORD,
                    PolarisRecordKinds.GRANT_RECORD_BY_SECURABLE,
                    List.of(1L, 20L),
                    EVERYTHING,
                    PolarisGrantRecord.class)
                .items())
        .isEmpty();
  }

  @Test
  void byTargetAndByPolicyServeBothPolicyMappingDirections() {
    PolarisPolicyMappingRecord mapping =
        new PolarisPolicyMappingRecord(1L, 10L, 1L, 30L, 5, (String) null);
    assertThat(
            store
                .commit(
                    List.of(
                        Mutation.of(
                            PolarisRecordKinds.POLICY_MAPPING,
                            Mutation.Op.CREATE,
                            RecordRef.byIdentity(
                                PolarisRecordKinds.POLICY_MAPPING, List.of(1L, 10L, 5, 1L, 30L)),
                            mapping)))
                .isApplied())
        .isTrue();

    assertThat(
            store
                .list(
                    PolarisRecordKinds.POLICY_MAPPING,
                    PolarisRecordKinds.POLICY_MAPPING_BY_TARGET,
                    List.of(1L, 10L),
                    EVERYTHING,
                    PolarisPolicyMappingRecord.class)
                .items())
        .hasSize(1);
    assertThat(
            store
                .list(
                    PolarisRecordKinds.POLICY_MAPPING,
                    PolarisRecordKinds.POLICY_MAPPING_BY_POLICY,
                    List.of(1L, 30L),
                    EVERYTHING,
                    PolarisPolicyMappingRecord.class)
                .items())
        .hasSize(1);
  }

  @Test
  void theKindLessFormUnionsOverTheKindsDeclaringThePath() {
    assertThat(store.commit(createOf(entity(10L, 1L, "child", 1))).isApplied()).isTrue();

    // only the entity kind declares by-parent today, so the union is that kind's result — but the
    // caller names no kind, which is what the children-existence check needs
    assertThat(
            store
                .list(
                    PolarisRecordKinds.ENTITY_BY_PARENT, List.of(1L, 1L), EVERYTHING, Object.class)
                .items())
        .hasSize(1);
    assertThat(
            store
                .list(
                    PolarisRecordKinds.ENTITY_BY_PARENT, List.of(1L, 99L), EVERYTHING, Object.class)
                .items())
        .isEmpty();
  }

  @Test
  void anUndeclaredPathIsRejectedRatherThanGuessedAt() {
    assertThatThrownBy(
            () ->
                store.list(
                    PolarisRecordKinds.PRINCIPAL_SECRETS,
                    PolarisRecordKinds.ENTITY_BY_PARENT,
                    List.of(1L, 1L),
                    EVERYTHING,
                    Object.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("declares no lookup path");

    assertThatThrownBy(
            () -> store.list(LookupPath.of("by-nothing"), List.of(1L), EVERYTHING, Object.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No registered kind declares");
  }

  @Test
  void theLocationPathAlsoMatchesAncestorsOfTheAnchorLikeTheShippedOverlapQuery() {
    // an overlap check matches in both directions: a stored parent location conflicts with a
    // deeper anchor exactly as a stored child conflicts with a shallower one
    PolarisBaseEntity parent =
        new PolarisBaseEntity.Builder(entity(10L, 1L, "ns1", 1))
            .propertiesAsMap(
                Map.of(PolarisEntityConstants.ENTITY_BASE_LOCATION, "s3://bucket/warehouse/ns1/"))
            .build();
    assertThat(store.commit(createOf(parent)).isApplied()).isTrue();

    assertThat(
            store
                .list(
                    PolarisRecordKinds.ENTITY,
                    PolarisRecordKinds.ENTITY_BY_LOCATION_PREFIX,
                    List.of(1L, "s3://bucket/warehouse/ns1/table1"),
                    EVERYTHING,
                    PolarisBaseEntity.class)
                .items())
        .extracting(PolarisBaseEntity::getId)
        .containsExactly(10L);
  }

  @Test
  void aWrongTypedAnchorAtTheRightArityIsRejectedNotClassCast() {
    assertThatThrownBy(
            () ->
                store.list(
                    PolarisRecordKinds.ENTITY,
                    PolarisRecordKinds.ENTITY_BY_PARENT,
                    List.of("1", 1L),
                    EVERYTHING,
                    PolarisBaseEntity.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("declares anchor 0 as Long");
  }

  @Test
  void anAnchorListNotMatchingTheDeclaredSignatureIsRejected() {
    assertThatThrownBy(
            () ->
                store.list(
                    PolarisRecordKinds.ENTITY,
                    PolarisRecordKinds.ENTITY_BY_PARENT,
                    List.of(1L),
                    EVERYTHING,
                    PolarisBaseEntity.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("declares 2 to 3 anchors");
  }

  private static List<Mutation> createOf(PolarisBaseEntity e) {
    return List.of(
        Mutation.of(PolarisRecordKinds.ENTITY, Mutation.Op.CREATE, entityRef(e.getId()), e));
  }
}
