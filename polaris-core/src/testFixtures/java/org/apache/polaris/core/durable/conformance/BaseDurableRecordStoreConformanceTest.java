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
package org.apache.polaris.core.durable.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.LookupPath;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.Precondition;
import org.apache.polaris.spi.durable.RecordKind;
import org.apache.polaris.spi.durable.RecordRef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/**
 * The Seam-1 conformance suite: the cases every {@link DurableRecordStore} implementation must
 * pass, run against an implementation by subclassing this base and supplying instances.
 *
 * <p>A third party building a backend runs exactly this suite; green means the implementation
 * honors the primitives contract as the shipped implementations do. Every case asserts
 * contract-observable behaviour only — {@link CommitResult} states, read-back through the four
 * reads, and rejection exceptions — never any implementation's mechanism, so two stores with
 * different rollback machinery pass the same cases (the JDBC store rolls back via its transaction
 * callback, the in-memory store by replaying an undo log; no case here can tell the difference).
 *
 * <p>This base compiles against the SPI and {@code polaris-core} declarations only. It must not
 * import any implementation: an assertion that only one mechanism can satisfy is a defect of the
 * case, not of a store (ADR-0010).
 *
 * <p>Record payloads here use Polaris's own kinds with real type/subtype codes, because a
 * conforming store may validate codes against the vocabulary when converting rows (the relational
 * store does; the in-memory store does not — both pass these cases either way).
 *
 * <p>The declared-lookup-path cases — enumerated per (kind × path) from the declarations rather
 * than written per kind — are the other half of this suite; see the path-case machinery in this
 * package.
 */
public abstract class BaseDurableRecordStoreConformanceTest {

  protected DurableRecordStore store;

  /** A fresh, empty store instance. Called before each case; instances are never reused. */
  protected abstract DurableRecordStore newStore();

  /**
   * A fresh, empty store instance declaring the given {@link DurableRecordStore#maxItemsPerCommit}
   * ceiling, for the over-cap case. The cap is a policy declaration, so every implementation can
   * honor a small one.
   */
  protected abstract DurableRecordStore newStore(int maxItemsPerCommit);

  @BeforeEach
  void freshStore() {
    store = newStore();
  }

  // ---------------------------------------------------------------- payloads

  protected static PolarisBaseEntity entity(long id, long parentId, String name, int version) {
    return new PolarisBaseEntity.Builder()
        .catalogId(1L)
        .id(id)
        .typeCode(PolarisEntityType.NAMESPACE.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .parentId(parentId)
        .name(name)
        .entityVersion(version)
        .propertiesAsMap(Map.of())
        .internalPropertiesAsMap(Map.of())
        .build();
  }

  protected static RecordRef entityRef(long id) {
    return RecordRef.byIdentity(PolarisRecordKinds.ENTITY, List.of(id));
  }

  protected static Mutation createEntity(PolarisBaseEntity e) {
    return Mutation.of(PolarisRecordKinds.ENTITY, Mutation.Op.CREATE, entityRef(e.getId()), e);
  }

  protected static PolarisGrantRecord grant(
      long securableCatalogId, long securableId, long granteeCatalogId, long granteeId, int priv) {
    return new PolarisGrantRecord(
        securableCatalogId, securableId, granteeCatalogId, granteeId, priv);
  }

  protected static RecordRef grantRef(PolarisGrantRecord g) {
    return RecordRef.byIdentity(
        PolarisRecordKinds.GRANT_RECORD,
        List.of(
            g.getSecurableCatalogId(),
            g.getSecurableId(),
            g.getGranteeCatalogId(),
            g.getGranteeId(),
            g.getPrivilegeCode()));
  }

  // ---------------------------------------------------------------- the write

  @Test
  protected void commitAppliesEveryMutationOrNoneAcrossKinds() {
    PolarisGrantRecord g = grant(1L, 10L, 1L, 20L, 3);
    CommitResult result =
        store.commit(
            List.of(
                createEntity(entity(10L, 1L, "catalog", 1)),
                Mutation.of(PolarisRecordKinds.GRANT_RECORD, Mutation.Op.CREATE, grantRef(g), g)));

    assertThat(result.isApplied()).isTrue();
    assertThat(store.get(entityRef(10L), PolarisBaseEntity.class)).isPresent();
    assertThat(store.get(grantRef(g), PolarisGrantRecord.class)).isPresent();
  }

  @Test
  protected void aFailedPreconditionAppliesNothing() {
    assertThat(store.commit(List.of(createEntity(entity(10L, 1L, "catalog", 1)))).isApplied())
        .isTrue();

    CommitResult result =
        store.commit(
            List.of(
                createEntity(entity(11L, 1L, "namespace", 1)),
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
    assertThat(store.get(entityRef(11L), PolarisBaseEntity.class)).isEmpty();
    assertThat(store.get(entityRef(10L), PolarisBaseEntity.class))
        .get()
        .extracting(PolarisBaseEntity::getEntityVersion)
        .isEqualTo(1);
  }

  @Test
  protected void aVersionPreconditionThatHoldsLetsTheUpdateThrough() {
    assertThat(store.commit(List.of(createEntity(entity(10L, 1L, "catalog", 1)))).isApplied())
        .isTrue();

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
  protected void aMustNotExistConditionOnTheUniquenessKeyFailsACollidingCreate() {
    assertThat(store.commit(List.of(createEntity(entity(10L, 1L, "catalog", 1)))).isApplied())
        .isTrue();

    // The colliding create names the same uniqueness tuple under a different identity — the
    // precondition decides the outcome, per the CREATE operator's contract.
    CommitResult second =
        store.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.ENTITY,
                    Mutation.Op.CREATE,
                    entityRef(11L),
                    entity(11L, 1L, "catalog", 1),
                    List.of(Precondition.notExists(entityUniquenessRef(1L, "catalog"))))));

    assertThat(second.isApplied()).isFalse();
    assertThat(second.failure()).contains(CommitResult.Failure.PRECONDITION_FAILED);
    assertThat(store.get(entityRef(11L), PolarisBaseEntity.class)).isEmpty();
  }

  /**
   * The declared logical uniqueness tuple for an entity, realm structural: {@code (parent, type,
   * name)}. The data model rules the relational store's extra {@code catalog_id} component a
   * locality choice, not part of the key — an implementation "may include such a component or omit
   * it", so no reference carries it.
   */
  protected static RecordRef entityUniquenessRef(long parentId, String name) {
    return RecordRef.byUniquenessKey(
        PolarisRecordKinds.ENTITY, List.of(parentId, PolarisEntityType.NAMESPACE.getCode(), name));
  }

  @Test
  protected void aCreateRacedByACompetingWriterLosesByPreconditionAtomically() {
    // The suite's first concurrency case, deterministic instead of thread-timed: the competing
    // writer's create lands in the real store on the FIRST commit call, strictly between this
    // commit's construction and its precondition evaluation — the interleaving real threads
    // cannot be relied on to produce, and which two same-key creates in ONE commit cannot model
    // because they roll each other back leaving no winner.
    DurableRecordStore raced =
        new CommitPreemptingDurableRecordStore(
            newStore(), racedMutations -> List.of(createEntity(entity(10L, 1L, "catalog", 1))));

    CommitResult result =
        raced.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.ENTITY,
                    Mutation.Op.CREATE,
                    entityRef(11L),
                    entity(11L, 1L, "catalog", 1),
                    List.of(Precondition.notExists(entityUniquenessRef(1L, "catalog")))),
                createEntity(entity(12L, 1L, "bystander", 1))));

    assertThat(result.isApplied()).isFalse();
    assertThat(result.failure()).contains(CommitResult.Failure.PRECONDITION_FAILED);
    // The winner survives; NOTHING of the losing commit landed, its innocent second mutation
    // included — losing a race must not shred the commit's atomicity.
    assertThat(raced.get(entityRef(10L), PolarisBaseEntity.class)).isPresent();
    assertThat(raced.get(entityRef(11L), PolarisBaseEntity.class)).isEmpty();
    assertThat(raced.get(entityRef(12L), PolarisBaseEntity.class)).isEmpty();
  }

  @Test
  protected void aConditionedDeleteAppliesWhenItsConditionHolds() {
    PolarisBaseEntity e = entity(10L, 1L, "catalog", 1);
    assertThat(store.commit(List.of(createEntity(e))).isApplied()).isTrue();

    CommitResult result =
        store.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.ENTITY,
                    Mutation.Op.DELETE,
                    entityRef(10L),
                    null,
                    List.of(
                        Precondition.versionEquals(
                            entityRef(10L), Precondition.VersionAttribute.RECORD_VERSION, 1L)))));

    assertThat(result.isApplied()).isTrue();
    assertThat(store.get(entityRef(10L), PolarisBaseEntity.class)).isEmpty();
  }

  @Test
  protected void aConditionedDeleteWhoseConditionFailsAppliesNothing() {
    PolarisBaseEntity e = entity(10L, 1L, "catalog", 1);
    assertThat(store.commit(List.of(createEntity(e))).isApplied()).isTrue();

    CommitResult result =
        store.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.ENTITY,
                    Mutation.Op.DELETE,
                    entityRef(10L),
                    null,
                    List.of(
                        Precondition.versionEquals(
                            entityRef(10L), Precondition.VersionAttribute.RECORD_VERSION, 99L)))));

    assertThat(result.isApplied()).isFalse();
    assertThat(result.failure()).contains(CommitResult.Failure.PRECONDITION_FAILED);
    assertThat(store.get(entityRef(10L), PolarisBaseEntity.class)).isPresent();
  }

  @Test
  protected void aDeleteAddressesItsRecordByTargetRefAloneWithANullPayload() {
    PolarisBaseEntity e = entity(10L, 1L, "catalog", 1);
    assertThat(store.commit(List.of(createEntity(e))).isApplied()).isTrue();
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
  protected void aDeleteCarryingAPayloadIsRejected() {
    PolarisBaseEntity e = entity(10L, 1L, "catalog", 1);
    assertThat(store.commit(List.of(createEntity(e))).isApplied()).isTrue();
    // a payload on DELETE would be an implicit condition channel; the contract pins rejection
    assertThatThrownBy(
            () ->
                store.commit(
                    List.of(
                        Mutation.of(
                            PolarisRecordKinds.ENTITY, Mutation.Op.DELETE, entityRef(10L), e))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(store.get(entityRef(10L), PolarisBaseEntity.class)).isPresent();
  }

  @Test
  protected void overTheItemCapIsRejectedRatherThanSplit() {
    DurableRecordStore small = newStore(1);
    CommitResult result =
        small.commit(
            List.of(createEntity(entity(10L, 1L, "a", 1)), createEntity(entity(11L, 1L, "b", 1))));

    assertThat(result.isApplied()).isFalse();
    assertThat(result.failure()).contains(CommitResult.Failure.TOO_MANY_ITEMS);
    assertThat(small.get(entityRef(10L), PolarisBaseEntity.class)).isEmpty();
    assertThat(small.get(entityRef(11L), PolarisBaseEntity.class)).isEmpty();
  }

  // ---------------------------------------------------------------- declarations

  @Test
  protected void equalDomainValuesCommitTogether() {
    assertThat(store.domainOf(entityRef(10L)))
        .isEqualTo(
            store.domainOf(
                RecordRef.byIdentity(
                    PolarisRecordKinds.GRANT_RECORD, List.of(1L, 10L, 1L, 20L, 3))));

    PolarisGrantRecord g = grant(1L, 10L, 1L, 20L, 3);
    assertThat(
            store
                .commit(
                    List.of(
                        createEntity(entity(10L, 1L, "catalog", 1)),
                        Mutation.of(
                            PolarisRecordKinds.GRANT_RECORD, Mutation.Op.CREATE, grantRef(g), g)))
                .isApplied())
        .isTrue();
  }

  @Test
  protected void twoStoreInstancesAreTwoAtomicityDomains() {
    DurableRecordStore other = newStore();
    assertThat(store.domainOf(entityRef(10L))).isEqualTo(store.domainOf(entityRef(11L)));
    assertThat(store.domainOf(entityRef(10L))).isNotEqualTo(other.domainOf(entityRef(10L)));
  }

  // ---------------------------------------------------------------- reads

  @Test
  protected void aReferenceByUniquenessKeyResolvesTheSameRecordAsItsIdentity() {
    assertThat(store.commit(List.of(createEntity(entity(10L, 1L, "catalog", 1)))).isApplied())
        .isTrue();
    assertThat(store.get(entityUniquenessRef(1L, "catalog"), PolarisBaseEntity.class))
        .get()
        .extracting(PolarisBaseEntity::getId)
        .isEqualTo(10L);
  }

  @Test
  protected void getManyPreservesRequestOrderWithEmptiesForMisses() {
    assertThat(store.commit(List.of(createEntity(entity(10L, 1L, "a", 1)))).isApplied()).isTrue();
    assertThat(store.commit(List.of(createEntity(entity(12L, 1L, "b", 1)))).isApplied()).isTrue();

    List<Optional<PolarisBaseEntity>> results =
        store.getMany(
            List.of(entityRef(12L), entityRef(99L), entityRef(10L)), PolarisBaseEntity.class);

    assertThat(results).hasSize(3);
    assertThat(results.get(0)).get().extracting(PolarisBaseEntity::getId).isEqualTo(12L);
    assertThat(results.get(1)).isEmpty();
    assertThat(results.get(2)).get().extracting(PolarisBaseEntity::getId).isEqualTo(10L);
  }

  @Test
  protected void versionsOfReadsVersionsAndRejectsVersionlessKinds() {
    assertThat(store.commit(List.of(createEntity(entity(10L, 1L, "catalog", 7)))).isApplied())
        .isTrue();

    assertThat(store.versionsOf(List.of(entityRef(10L), entityRef(99L))))
        .satisfies(
            versions -> {
              assertThat(versions).hasSize(2);
              assertThat(versions.get(0)).get().extracting("recordVersion").isEqualTo(7L);
              assertThat(versions.get(1)).isEmpty();
            });

    PolarisGrantRecord g = grant(1L, 10L, 1L, 20L, 3);
    assertThatThrownBy(() -> store.versionsOf(List.of(grantRef(g))))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  protected void generateNewIdDoesNotRepeat() {
    assertThat(store.generateNewId()).isNotEqualTo(store.generateNewId());
  }

  // ---------------------------------------------------------------- rejection

  @Test
  protected void anUnregisteredKindIsRejectedOnEveryOperation() {
    RecordKind unknown = RecordKind.of("someone-elses.thing");
    RecordRef ref = RecordRef.byIdentity(unknown, List.of(1L));

    assertThatThrownBy(() -> store.get(ref, Object.class))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                store.commit(List.of(Mutation.of(unknown, Mutation.Op.CREATE, ref, new Object()))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                store.list(
                    unknown,
                    PolarisRecordKinds.ENTITY_BY_PARENT,
                    List.of(1L, 1L),
                    PageToken.readEverything(),
                    Object.class))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  protected void anUndeclaredKindPathPairIsRejected() {
    // principal secrets declare no list paths — a documented gap, so the pair is undeclared
    assertThatThrownBy(
            () ->
                store.list(
                    PolarisRecordKinds.PRINCIPAL_SECRETS,
                    PolarisRecordKinds.ENTITY_BY_PARENT,
                    List.of(1L, 1L),
                    PageToken.readEverything(),
                    Object.class))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  protected void aPathNoRegisteredKindDeclaresIsRejectedOnTheUnionForm() {
    assertThatThrownBy(
            () ->
                store.list(
                    LookupPath.of("by-nothing"),
                    List.of(1L),
                    PageToken.readEverything(),
                    Object.class))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------- declared lookup paths
  //
  // Generated per (kind × path) from the declarations: the case code below iterates PathCase
  // entries and never names a concrete kind or path, so a new declared path gets coverage by
  // adding an entry, never by adding case code.

  /** The declared (kind × path) entries the generated cases run over. */
  protected List<PathCase> pathCases() {
    return ConformanceDeclarations.polarisPathCases();
  }

  @TestFactory
  protected Stream<DynamicNode> declaredPathCasesGeneratedFromTheDeclarations() {
    return pathCases().stream().map(this::casesFor);
  }

  private DynamicNode casesFor(PathCase pc) {
    List<DynamicTest> cases = new ArrayList<>();
    cases.add(
        DynamicTest.dynamicTest(
            "recordsUnderOneAnchorDoNotLeakIntoAnother",
            () -> {
              DurableRecordStore s = newStore();
              List<Object> a = pc.anchors().apply(0);
              List<Object> b = pc.anchors().apply(1);
              Object a0 = pc.mint().mint(a, 0, null);
              Object a1 = pc.mint().mint(a, 1, null);
              Object b0 = pc.mint().mint(b, 0, null);
              create(s, pc, a0);
              create(s, pc, a1);
              create(s, pc, b0);

              assertThat(refsListedUnder(s, pc, a))
                  .containsExactlyInAnyOrder(ref(pc, a0), ref(pc, a1));
              assertThat(refsListedUnder(s, pc, b)).containsExactly(ref(pc, b0));
            }));
    // One case per anchor position: two tuples differing at EXACTLY that position must not see
    // each other's records. The two-seed tuples above differ at every position at once, so a store
    // ignoring one component entirely would still pass them; these variants isolate each component.
    for (int p = 0; p < pc.anchorTypes().size(); p++) {
      final int pos = p;
      cases.add(
          DynamicTest.dynamicTest(
              "recordsDoNotLeakWhenOnlyAnchorPosition" + pos + "Differs",
              () -> {
                DurableRecordStore s = newStore();
                List<Object> base = pc.anchors().apply(0);
                List<Object> variant = new ArrayList<>(base);
                variant.set(pos, pc.anchors().apply(1).get(pos));
                Object under = pc.mint().mint(base, 0, null);
                Object other = pc.mint().mint(variant, 1, null);
                create(s, pc, under);
                create(s, pc, other);

                assertThat(refsListedUnder(s, pc, base)).containsExactly(ref(pc, under));
                assertThat(refsListedUnder(s, pc, variant)).containsExactly(ref(pc, other));
              }));
    }
    cases.add(
        DynamicTest.dynamicTest(
            "anEmptyDeclaredScopeIsAnEmptyPageNotAnError",
            () -> {
              DurableRecordStore s = newStore();
              assertThat(refsListedUnder(s, pc, pc.anchors().apply(0))).isEmpty();
            }));
    cases.add(
        DynamicTest.dynamicTest(
            "aPagingWalkYieldsTheFullSetExactlyOnce",
            () -> {
              DurableRecordStore s = newStore();
              List<Object> a = pc.anchors().apply(0);
              List<RecordRef> expected = new ArrayList<>();
              for (int i = 0; i < 3; i++) {
                Object r = pc.mint().mint(a, i, null);
                create(s, pc, r);
                expected.add(ref(pc, r));
              }

              // The contract-observable minimum: the walk terminates and yields the complete set
              // exactly once. A store answering with one complete page and no token conforms;
              // page-size enforcement is not asserted (a recorded gap, not a conformance clause).
              List<RecordRef> collected = new ArrayList<>();
              PageToken token = PageToken.fromLimit(2);
              boolean terminated = false;
              for (int hop = 0; hop < 10; hop++) {
                Page<Object> page = s.list(pc.kind(), pc.path(), a, token, Object.class);
                page.items().forEach(r -> collected.add(ref(pc, r)));
                String next = page.encodedResponseToken();
                if (next == null) {
                  terminated = true;
                  break;
                }
                token = PageToken.build(next, null, () -> true);
              }
              assertThat(terminated).as("token walk terminates").isTrue();
              assertThat(collected).containsExactlyInAnyOrderElementsOf(expected);
            }));
    cases.add(
        DynamicTest.dynamicTest(
            "anchorArityBelowTheDeclaredSignatureIsRejected",
            () -> {
              DurableRecordStore s = newStore();
              List<Object> a = pc.anchors().apply(0);
              List<Object> tooFew = a.subList(0, a.size() - 1);
              assertThatThrownBy(
                      () ->
                          s.list(
                              pc.kind(),
                              pc.path(),
                              tooFew,
                              PageToken.readEverything(),
                              Object.class))
                  .isInstanceOf(IllegalArgumentException.class);
            }));
    cases.add(
        DynamicTest.dynamicTest(
            "aWrongTypedAnchorIsRejected",
            () -> {
              DurableRecordStore s = newStore();
              List<Object> a = new ArrayList<>(pc.anchors().apply(0));
              a.set(0, pc.anchorTypes().get(0) == Long.class ? "not-a-long" : 1L);
              assertThatThrownBy(
                      () ->
                          s.list(pc.kind(), pc.path(), a, PageToken.readEverything(), Object.class))
                  .isInstanceOf(IllegalArgumentException.class);
            }));
    if (pc.trailingType() != null) {
      cases.add(
          DynamicTest.dynamicTest(
              "theOptionalTrailingAnchorNarrowsTheListing",
              () -> {
                DurableRecordStore s = newStore();
                List<Object> a = pc.anchors().apply(0);
                Object plain = pc.mint().mint(a, 0, null);
                Object selected = pc.mint().mint(a, 1, pc.trailingValue());
                create(s, pc, plain);
                create(s, pc, selected);

                List<Object> withTrailing = new ArrayList<>(a);
                withTrailing.add(pc.trailingValue());
                assertThat(refsListedUnder(s, pc, withTrailing)).containsExactly(ref(pc, selected));
              }));
    }
    if (pc.ancestorMint() != null) {
      cases.add(
          DynamicTest.dynamicTest(
              "aRecordAtAnAncestorOfTheAnchorIsAlsoServed",
              () -> {
                DurableRecordStore s = newStore();
                List<Object> a = pc.anchors().apply(0);
                Object shallow = pc.ancestorMint().mint(a, 0, null);
                create(s, pc, shallow);
                // The declared overlap runs in BOTH directions: records deeper than the anchor
                // (every other case's mints) and a record AT one of the anchor's slash-terminated
                // ancestor segments. Generation could never reach this direction before, because
                // the standard minter only mints deeper.
                assertThat(refsListedUnder(s, pc, a)).contains(ref(pc, shallow));
              }));
    }
    cases.add(
        DynamicTest.dynamicTest(
            "theKindLessUnionFormServesThePath",
            () -> {
              DurableRecordStore s = newStore();
              List<Object> a = pc.anchors().apply(0);
              List<Object> b = pc.anchors().apply(1);
              Object a0 = pc.mint().mint(a, 0, null);
              Object b0 = pc.mint().mint(b, 0, null);
              create(s, pc, a0);
              create(s, pc, b0);

              List<RecordRef> union =
                  s.list(pc.path(), a, PageToken.readEverything(), Object.class).items().stream()
                      .map(r -> ref(pc, r))
                      .toList();
              assertThat(union).contains(ref(pc, a0));
              // The union serves the path's SCOPE, not the kind's whole slice: a record under a
              // different anchor tuple must be absent, or a union ignoring the anchors would pass.
              assertThat(union).doesNotContain(ref(pc, b0));
            }));
    return DynamicContainer.dynamicContainer(pc.kind().id() + " " + pc.path().name(), cases);
  }

  /** The cross-path entries the generated cases run over. */
  protected List<CrossPathCase> crossPathCases() {
    return ConformanceDeclarations.crossPathCases();
  }

  @TestFactory
  protected Stream<DynamicNode> crossPathCasesGeneratedFromTheDeclarations() {
    return crossPathCases().stream().map(this::crossPathCaseFor);
  }

  private DynamicNode crossPathCaseFor(CrossPathCase cc) {
    return DynamicTest.dynamicTest(
        cc.kind().id() + " aSingleCommitIsServedByEveryDeclaredPathOfItsKind",
        () -> {
          DurableRecordStore s = newStore();
          RecordRef committed = cc.identityRef().apply(cc.record());
          CommitResult result =
              s.commit(List.of(Mutation.of(cc.kind(), Mutation.Op.CREATE, committed, cc.record())));
          assertThat(result.isApplied()).as("fixture create applies").isTrue();

          cc.anchorsPerPath()
              .forEach(
                  (path, anchors) ->
                      assertThat(crossRefsListedUnder(s, cc, path, anchors))
                          .as("path %s serves the one committed record", path.name())
                          .contains(committed));

          // The negative direction: under ANOTHER path's anchors (where the tuple types are
          // compatible and the values differ), the record must be absent — two paths accidentally
          // wired to the same column would serve it symmetrically and pass the positive half.
          // Scope, stated so the skip is visible: this direction runs only for path pairs sharing
          // anchor types (GRANT_RECORD and POLICY_MAPPING today). A kind whose paths declare
          // different types (ENTITY: by-parent [Long,Long] vs by-location-prefix [Long,String])
          // gets zero negative assertions here; each such path's own anchor filtering is covered
          // by the per-position isolation cases instead.
          cc.anchorsPerPath()
              .forEach(
                  (path, anchors) ->
                      cc.anchorsPerPath()
                          .forEach(
                              (otherPath, otherAnchors) -> {
                                if (path.equals(otherPath)
                                    || anchors.equals(otherAnchors)
                                    || !anchorClassesOf(anchors)
                                        .equals(anchorClassesOf(otherAnchors))) {
                                  return;
                                }
                                assertThat(crossRefsListedUnder(s, cc, path, otherAnchors))
                                    .as(
                                        "path %s must not serve the record under %s's anchors",
                                        path.name(), otherPath.name())
                                    .doesNotContain(committed);
                              }));
        });
  }

  private static List<Class<?>> anchorClassesOf(List<Object> anchors) {
    return anchors.stream().<Class<?>>map(Object::getClass).toList();
  }

  private static List<RecordRef> crossRefsListedUnder(
      DurableRecordStore s, CrossPathCase cc, LookupPath path, List<Object> anchors) {
    return s
        .list(cc.kind(), path, anchors, PageToken.readEverything(), Object.class)
        .items()
        .stream()
        .map(r -> cc.identityRef().apply(r))
        .toList();
  }

  private static void create(DurableRecordStore s, PathCase pc, Object record) {
    CommitResult result =
        s.commit(List.of(Mutation.of(pc.kind(), Mutation.Op.CREATE, ref(pc, record), record)));
    assertThat(result.isApplied()).as("fixture create applies").isTrue();
  }

  private static RecordRef ref(PathCase pc, Object record) {
    return pc.identityRef().apply(record);
  }

  private static List<RecordRef> refsListedUnder(
      DurableRecordStore s, PathCase pc, List<Object> anchors) {
    return s
        .list(pc.kind(), pc.path(), anchors, PageToken.readEverything(), Object.class)
        .items()
        .stream()
        .map(r -> ref(pc, r))
        .toList();
  }
}
