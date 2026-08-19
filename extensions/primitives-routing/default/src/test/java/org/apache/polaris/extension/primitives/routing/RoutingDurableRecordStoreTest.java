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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.extension.orchestration.DefaultDurableOrchestrator;
import org.apache.polaris.persistence.treemap.TreeMapDurableRecordStore;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.LookupPath;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.OrchestrationResult;
import org.apache.polaris.spi.durable.RecordKind;
import org.apache.polaris.spi.durable.RecordRef;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The routing store's own mechanics over two in-memory backends: the single-backend commit rule,
 * untouched {@code domainOf} forwarding, designated id generation, order-preserving scatter-gather
 * reads, and the concatenating union read. The conformance proof — the full Seam-1 suite over a
 * two-store routing assembly — lives in {@link RoutingDurableRecordStoreConformanceTest}.
 */
class RoutingDurableRecordStoreTest {

  private static final PolarisDiagnostics DIAGNOSTICS = new PolarisDefaultDiagServiceImpl();
  private static final String MAIN = "main";
  private static final String AUTHZ = "authz";

  private TreeMapDurableRecordStore mainStore;
  private TreeMapDurableRecordStore authzStore;
  private RoutingDurableRecordStore routing;

  @BeforeEach
  void setUp() {
    mainStore = new TreeMapDurableRecordStore(DIAGNOSTICS);
    authzStore = new TreeMapDurableRecordStore(DIAGNOSTICS);
    routing =
        new RoutingDurableRecordStore(
            new MappedDurableRecordStoreLocator(
                Map.of(
                    PolarisRecordKinds.ENTITY, MAIN,
                    PolarisRecordKinds.POLICY_MAPPING, MAIN,
                    PolarisRecordKinds.GRANT_RECORD, AUTHZ),
                Map.of(MAIN, mainStore, AUTHZ, authzStore)),
            List.of(mainStore, authzStore),
            mainStore);
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

  private static Mutation createEntity(PolarisBaseEntity e) {
    return Mutation.of(PolarisRecordKinds.ENTITY, Mutation.Op.CREATE, entityRef(e.getId()), e);
  }

  private static PolarisGrantRecord grant(long securableId, long granteeId) {
    return new PolarisGrantRecord(1L, securableId, 1L, granteeId, 3);
  }

  private static RecordRef grantRef(PolarisGrantRecord g) {
    return RecordRef.byIdentity(
        PolarisRecordKinds.GRANT_RECORD,
        List.of(
            g.getSecurableCatalogId(),
            g.getSecurableId(),
            g.getGranteeCatalogId(),
            g.getGranteeId(),
            g.getPrivilegeCode()));
  }

  private static Mutation createGrant(PolarisGrantRecord g) {
    return Mutation.of(PolarisRecordKinds.GRANT_RECORD, Mutation.Op.CREATE, grantRef(g), g);
  }

  // ---------------------------------------------------------------- the commit rule

  @Test
  void aCommitSpanningTwoBackendsIsRefusedAndAppliesNothing() {
    PolarisBaseEntity e = entity(10L, "catalog");
    PolarisGrantRecord g = grant(10L, 20L);

    CommitResult result = routing.commit(List.of(createEntity(e), createGrant(g)));

    assertThat(result.isApplied()).isFalse();
    assertThat(result.failure()).contains(CommitResult.Failure.DOMAIN_MISMATCH);
    assertThat(mainStore.get(entityRef(10L), PolarisBaseEntity.class)).isEmpty();
    assertThat(authzStore.get(grantRef(g), PolarisGrantRecord.class)).isEmpty();
  }

  @Test
  void aSingleBackendMultiKindCommitForwardsAsOneCommit() {
    // ENTITY and POLICY_MAPPING are co-located on main: one commit, both applied, nothing on authz.
    PolarisBaseEntity e = entity(10L, "catalog");
    PolarisPolicyMappingRecord p = new PolarisPolicyMappingRecord(1L, 10L, 1L, 30L, 5, "{}");
    Mutation policyCreate =
        Mutation.of(
            PolarisRecordKinds.POLICY_MAPPING,
            Mutation.Op.CREATE,
            RecordRef.byIdentity(PolarisRecordKinds.POLICY_MAPPING, List.of(1L, 10L, 5, 1L, 30L)),
            p);

    CommitResult result = routing.commit(List.of(createEntity(e), policyCreate));

    assertThat(result.isApplied()).isTrue();
    assertThat(mainStore.get(entityRef(10L), PolarisBaseEntity.class)).isPresent();
  }

  @Test
  void aCommitOfAnUnmappedKindIsRejectedNamingTheKind() {
    RecordKind unknown = RecordKind.of("someone-elses.thing");
    assertThatThrownBy(
            () ->
                routing.commit(
                    List.of(
                        Mutation.of(
                            unknown,
                            Mutation.Op.CREATE,
                            RecordRef.byIdentity(unknown, List.of(1L)),
                            new Object()))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("someone-elses.thing");
  }

  // ---------------------------------------------------------------- declarations

  @Test
  void domainOfForwardsTheHoldingBackendsAnswerUntouched() {
    // Same value object, not a wrapped copy: co-located kinds stay mergeable upstream.
    assertThat(routing.domainOf(entityRef(10L))).isSameAs(mainStore.domainOf(entityRef(10L)));
    PolarisGrantRecord g = grant(10L, 20L);
    assertThat(routing.domainOf(grantRef(g))).isSameAs(authzStore.domainOf(grantRef(g)));
    assertThat(routing.domainOf(entityRef(10L))).isNotEqualTo(routing.domainOf(grantRef(g)));
  }

  @Test
  void maxItemsPerCommitDeclaresTheMinimumAcrossBackends() {
    TreeMapDurableRecordStore roomy = new TreeMapDurableRecordStore(DIAGNOSTICS, 5);
    TreeMapDurableRecordStore tight = new TreeMapDurableRecordStore(DIAGNOSTICS, 3);
    RoutingDurableRecordStore capped =
        new RoutingDurableRecordStore(
            new MappedDurableRecordStoreLocator(
                Map.of(PolarisRecordKinds.ENTITY, MAIN, PolarisRecordKinds.GRANT_RECORD, AUTHZ),
                Map.of(MAIN, roomy, AUTHZ, tight)),
            List.of(roomy, tight),
            roomy);
    assertThat(capped.maxItemsPerCommit()).isEqualTo(3);
  }

  @Test
  void aBatchAboveTheDeclaredMinimumButWithinTheHoldingBackendsCapSucceedsToday() {
    // The declaration's meaning, pinned over an asymmetric assembly rather than resting on javadoc
    // alone: maxItemsPerCommit declares "the one cap every routed commit can honor", and
    // ENFORCEMENT stays with whichever single backend a commit routes to — the class javadoc's
    // declared wart (ticket 111's recorded non-goal). A batch larger than the declared minimum but
    // within the holding backend's own cap therefore SUCCEEDS today; any future decision to
    // enforce the declared minimum at the routing layer turns this case red instead of changing
    // the semantics silently.
    TreeMapDurableRecordStore roomy = new TreeMapDurableRecordStore(DIAGNOSTICS, 5);
    TreeMapDurableRecordStore tight = new TreeMapDurableRecordStore(DIAGNOSTICS, 3);
    RoutingDurableRecordStore asymmetric =
        new RoutingDurableRecordStore(
            new MappedDurableRecordStoreLocator(
                Map.of(PolarisRecordKinds.ENTITY, MAIN, PolarisRecordKinds.GRANT_RECORD, AUTHZ),
                Map.of(MAIN, roomy, AUTHZ, tight)),
            List.of(roomy, tight),
            roomy);
    assertThat(asymmetric.maxItemsPerCommit()).isEqualTo(3);

    CommitResult result =
        asymmetric.commit(
            List.of(
                createEntity(entity(1L, "n1")),
                createEntity(entity(2L, "n2")),
                createEntity(entity(3L, "n3")),
                createEntity(entity(4L, "n4"))));

    assertThat(result.isApplied()).isTrue();
    assertThat(asymmetric.get(entityRef(4L), PolarisBaseEntity.class)).isPresent();
  }

  @Test
  void generateNewIdDelegatesToTheDesignatedOwner() {
    TreeMapDurableRecordStore marked =
        new TreeMapDurableRecordStore(DIAGNOSTICS) {
          @Override
          public long generateNewId() {
            return 424_242L;
          }
        };
    RoutingDurableRecordStore owned =
        new RoutingDurableRecordStore(
            new MappedDurableRecordStoreLocator(
                Map.of(PolarisRecordKinds.ENTITY, MAIN), Map.of(MAIN, marked)),
            List.of(marked),
            marked);
    assertThat(owned.generateNewId()).isEqualTo(424_242L);
  }

  @Test
  void theIdGenerationOwnerMustBeOneOfTheBackends() {
    TreeMapDurableRecordStore outsider = new TreeMapDurableRecordStore(DIAGNOSTICS);
    assertThatThrownBy(
            () ->
                new RoutingDurableRecordStore(
                    new MappedDurableRecordStoreLocator(
                        Map.of(PolarisRecordKinds.ENTITY, MAIN), Map.of(MAIN, mainStore)),
                    List.of(mainStore),
                    outsider))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------- reads

  @Test
  void getManyAcrossBackendsPreservesRequestOrderWithEmptiesForMisses() {
    PolarisBaseEntity e = entity(10L, "catalog");
    PolarisGrantRecord g = grant(10L, 20L);
    assertThat(routing.commit(List.of(createEntity(e))).isApplied()).isTrue();
    assertThat(routing.commit(List.of(createGrant(g))).isApplied()).isTrue();

    List<Optional<Object>> results =
        routing.getMany(List.of(grantRef(g), entityRef(99L), entityRef(10L)), Object.class);

    assertThat(results).hasSize(3);
    assertThat(results.get(0)).containsInstanceOf(PolarisGrantRecord.class);
    assertThat(results.get(1)).isEmpty();
    assertThat(results.get(2)).containsInstanceOf(PolarisBaseEntity.class);
  }

  @Test
  void versionsOfForwardsToTheHoldingBackend() {
    PolarisBaseEntity e = entity(10L, "catalog");
    assertThat(routing.commit(List.of(createEntity(e))).isApplied()).isTrue();

    assertThat(routing.versionsOf(List.of(entityRef(10L), entityRef(99L))))
        .satisfies(
            versions -> {
              assertThat(versions).hasSize(2);
              assertThat(versions.get(0)).isPresent();
              assertThat(versions.get(1)).isEmpty();
            });
  }

  // ---------------------------------------------------------------- the union read

  @Test
  void theUnionReadConcatenatesAcrossBackendsAndAnEmptyContributionIsNotAnError() {
    // ENTITY lives on main; authz structurally serves the path too and contributes an empty page.
    PolarisBaseEntity e = entity(10L, "catalog");
    assertThat(routing.commit(List.of(createEntity(e))).isApplied()).isTrue();

    var union =
        routing.list(
            PolarisRecordKinds.ENTITY_BY_PARENT,
            List.of(1L, 1L),
            PageToken.readEverything(),
            Object.class);

    assertThat(union.items()).hasSize(1);
    assertThat(union.encodedResponseToken()).isNull();
  }

  @Test
  void collidingDomainValuesAcrossBackendsFailLoudlyRatherThanCrossCommitting() {
    // The shipped stores declare identity-distinct domain objects, so this cannot happen with
    // them. A third-party pair declaring colliding VALUES makes the orchestrator (which compares
    // the forwarded values alone) merge the two kinds into one group; the routing commit then
    // refuses the merged group because its kinds resolve to two backends. Loud, never a silent
    // cross-store commit. This pins the disclosed degradation left by collapsing the old
    // (store, value) domain pair to the bare value (decided 2026-08-18).
    TreeMapDurableRecordStore collidingMain =
        new TreeMapDurableRecordStore(DIAGNOSTICS) {
          @Override
          public @NonNull Object domainOf(@NonNull RecordRef target) {
            return "d";
          }
        };
    TreeMapDurableRecordStore collidingAuthz =
        new TreeMapDurableRecordStore(DIAGNOSTICS) {
          @Override
          public @NonNull Object domainOf(@NonNull RecordRef target) {
            return "d";
          }
        };
    RoutingDurableRecordStore colliding =
        new RoutingDurableRecordStore(
            new MappedDurableRecordStoreLocator(
                Map.of(PolarisRecordKinds.ENTITY, MAIN, PolarisRecordKinds.GRANT_RECORD, AUTHZ),
                Map.of(MAIN, collidingMain, AUTHZ, collidingAuthz)),
            List.of(collidingMain, collidingAuthz),
            collidingMain);

    PolarisBaseEntity e = entity(10L, "catalog");
    PolarisGrantRecord g = grant(10L, 20L);
    OrchestrationResult result =
        new DefaultDurableOrchestrator(colliding).commit(List.of(createEntity(e), createGrant(g)));

    assertThat(result.outcome()).isEqualTo(OrchestrationResult.Outcome.ROLLED_BACK);
    assertThat(result.groupFailure())
        .get()
        .extracting(CommitResult::failure)
        .extracting(Optional::get)
        .isEqualTo(CommitResult.Failure.DOMAIN_MISMATCH);
    assertThat(collidingMain.get(entityRef(10L), PolarisBaseEntity.class)).isEmpty();
    assertThat(collidingAuthz.get(grantRef(g), PolarisGrantRecord.class)).isEmpty();
  }

  @Test
  void aPathNoBackendServesIsRejectedOnTheUnionRead() {
    assertThatThrownBy(
            () ->
                routing.list(
                    LookupPath.of("by-nothing"),
                    List.of(1L),
                    PageToken.readEverything(),
                    Object.class))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
