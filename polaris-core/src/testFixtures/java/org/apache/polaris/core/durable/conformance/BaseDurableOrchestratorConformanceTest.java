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
import java.util.function.Function;
import org.apache.polaris.core.durable.conformance.CommitRecordingDurableRecordStore.RecordedCommit;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableOrchestrator;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.OrchestrationResult;
import org.apache.polaris.spi.durable.Precondition;
import org.apache.polaris.spi.durable.RecordKind;
import org.apache.polaris.spi.durable.RecordRef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The Seam-2 conformance suite: the cases every {@link DurableOrchestrator} implementation must
 * pass over a multi-store assembly, run by subclassing this base and supplying the assembly and the
 * orchestrator.
 *
 * <p>The cases assert the orchestration contract's observables only: how many {@code commit} calls
 * reach which store in what order (through {@link CommitRecordingDurableRecordStore} wrappers at
 * the seam), the {@link OrchestrationResult} outcome, and final state read back through the
 * assembly's stores. Nothing here can tell a local orchestrator from a remote one — which is what
 * lets the remote implementation run the identical cases.
 *
 * <p>The assembly under test routes {@code GRANT_RECORD} to the {@code authz} store and every other
 * kind to {@code main} — a two-store configuration, so the same list can hold same-domain adjacent
 * runs, semantic separations, and cross-domain sequences. Both stores are real; nothing is
 * scripted, so a group fails only the way a store can really fail: a precondition that does not
 * hold.
 */
public abstract class BaseDurableOrchestratorConformanceTest {

  /** A fresh, empty store instance for the {@code main} half of the assembly. */
  protected abstract DurableRecordStore newMainStore();

  /** A fresh, empty store instance for the {@code authz} half of the assembly. */
  protected abstract DurableRecordStore newAuthzStore();

  /**
   * The realm-scoped assembly over the two given stores, routing {@code GRANT_RECORD} to {@code
   * authz} and every other kind (the default) to {@code main}. The local runner produces this
   * through the real factory; a remote runner wires its transport however it likes, as long as the
   * routing holds and every request passes through the given instances.
   */
  protected abstract Function<RecordKind, DurableRecordStore> assemble(
      DurableRecordStore main, DurableRecordStore authz);

  /** The orchestrator under test, constructed over the given assembly. */
  protected abstract DurableOrchestrator orchestratorOver(
      Function<RecordKind, DurableRecordStore> assembly);

  protected List<RecordedCommit> log;
  protected CommitRecordingDurableRecordStore main;
  protected CommitRecordingDurableRecordStore authz;
  protected DurableOrchestrator orchestrator;

  @BeforeEach
  void freshAssembly() {
    log = new ArrayList<>();
    main = new CommitRecordingDurableRecordStore("main", newMainStore(), log);
    authz = new CommitRecordingDurableRecordStore("authz", newAuthzStore(), log);
    orchestrator = orchestratorOver(assemble(main, authz));
  }

  // ---------------------------------------------------------------- payloads

  private static PolarisBaseEntity entity(long id, String name, int version) {
    return new PolarisBaseEntity.Builder()
        .catalogId(1L)
        .id(id)
        .typeCode(PolarisEntityType.NAMESPACE.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .parentId(1L)
        .name(name)
        .entityVersion(version)
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

  /** A grant create gated on a grant that does not exist — the only way a real store fails. */
  private static Mutation failingGrant() {
    PolarisGrantRecord g = grant(9_999L, 9_999L);
    return Mutation.of(
        PolarisRecordKinds.GRANT_RECORD,
        Mutation.Op.CREATE,
        grantRef(g),
        g,
        List.of(Precondition.exists(grantRef(grant(8_888L, 8_888L)))));
  }

  private record Call(String store, Mutation.Op op, RecordRef target) {}

  private List<Call> calls() {
    return log.stream()
        .flatMap(rc -> rc.mutations().stream().map(m -> new Call(rc.store(), m.op(), m.target())))
        .toList();
  }

  // ---------------------------------------------------------------- grouping

  @Test
  protected void anAdjacentSameDomainMultiKindListMergesIntoExactlyOneCommit() {
    PolarisBaseEntity e = entity(10L, "catalog", 1);
    // policy-mapping routes to main like entity does: two kinds, one store, adjacent
    // parameters as an empty JSON object rather than null: a payload a relational store's
    // NOT NULL column accepts and the in-memory store is indifferent to
    PolarisPolicyMappingRecord p = new PolarisPolicyMappingRecord(1L, 10L, 1L, 30L, 5, "{}");
    Mutation policyCreate =
        Mutation.of(
            PolarisRecordKinds.POLICY_MAPPING,
            Mutation.Op.CREATE,
            RecordRef.byIdentity(PolarisRecordKinds.POLICY_MAPPING, List.of(1L, 10L, 5, 1L, 30L)),
            p);

    OrchestrationResult result = orchestrator.commit(List.of(createEntity(e), policyCreate));

    assertThat(result.isApplied()).isTrue();
    assertThat(log).hasSize(1);
    assertThat(log.get(0).store()).isEqualTo("main");
    assertThat(log.get(0).mutations()).hasSize(2);
    assertThat(main.get(entityRef(10L), PolarisBaseEntity.class)).isPresent();
  }

  @Test
  protected void sameDomainMutationsSeparatedByAnotherDomainStaySeparateCommitsInListOrder() {
    PolarisBaseEntity e1 = entity(10L, "a", 1);
    PolarisGrantRecord g1 = grant(10L, 20L);
    PolarisBaseEntity e2 = entity(11L, "b", 1);

    OrchestrationResult result =
        orchestrator.commit(List.of(createEntity(e1), createGrant(g1), createEntity(e2)));

    assertThat(result.isApplied()).isTrue();
    // separation is semantic: three commits, strictly in list order, never hoisted into two
    assertThat(log).extracting(RecordedCommit::store).containsExactly("main", "authz", "main");
    assertThat(calls())
        .containsExactly(
            new Call("main", Mutation.Op.CREATE, entityRef(10L)),
            new Call("authz", Mutation.Op.CREATE, grantRef(g1)),
            new Call("main", Mutation.Op.CREATE, entityRef(11L)));
  }

  // ---------------------------------------------------------------- compensation

  @Test
  protected void aLaterGroupsFailureRollsBackInReverseCommitOrderAcrossASharedDomain() {
    PolarisBaseEntity e1 = entity(10L, "a", 1);
    PolarisGrantRecord g1 = grant(10L, 20L);
    PolarisBaseEntity e2 = entity(11L, "b", 1);

    OrchestrationResult result =
        orchestrator.commit(
            List.of(createEntity(e1), createGrant(g1), createEntity(e2), failingGrant()));

    assertThat(result.outcome()).isEqualTo(OrchestrationResult.Outcome.ROLLED_BACK);
    assertThat(result.groupFailure())
        .get()
        .extracting(CommitResult::failure)
        .extracting(java.util.Optional::get)
        .isEqualTo(CommitResult.Failure.PRECONDITION_FAILED);

    // forward commits in list order, the failed attempt, then reverse-commit-order rollback —
    // the main store rolled back twice, in reverse, which is the shared-domain criterion
    assertThat(calls())
        .containsExactly(
            new Call("main", Mutation.Op.CREATE, entityRef(10L)),
            new Call("authz", Mutation.Op.CREATE, grantRef(g1)),
            new Call("main", Mutation.Op.CREATE, entityRef(11L)),
            new Call("authz", Mutation.Op.CREATE, grantRef(grant(9_999L, 9_999L))),
            new Call("main", Mutation.Op.DELETE, entityRef(11L)),
            new Call("authz", Mutation.Op.DELETE, grantRef(g1)),
            new Call("main", Mutation.Op.DELETE, entityRef(10L)));

    assertThat(main.get(entityRef(10L), PolarisBaseEntity.class)).isEmpty();
    assertThat(main.get(entityRef(11L), PolarisBaseEntity.class)).isEmpty();
    assertThat(authz.get(grantRef(g1), PolarisGrantRecord.class)).isEmpty();
  }

  @Test
  protected void anOrdinaryFailureLeavesNothingAndARetrySimplySucceeds() {
    PolarisBaseEntity e1 = entity(10L, "a", 1);
    PolarisGrantRecord g1 = grant(10L, 20L);

    OrchestrationResult failed = orchestrator.commit(List.of(createEntity(e1), failingGrant()));
    assertThat(failed.outcome()).isEqualTo(OrchestrationResult.Outcome.ROLLED_BACK);
    assertThat(main.get(entityRef(10L), PolarisBaseEntity.class)).isEmpty();

    OrchestrationResult retried = orchestrator.commit(List.of(createEntity(e1), createGrant(g1)));
    assertThat(retried.isApplied()).isTrue();
    assertThat(main.get(entityRef(10L), PolarisBaseEntity.class)).isPresent();
    assertThat(authz.get(grantRef(g1), PolarisGrantRecord.class)).isPresent();
  }

  // ---------------------------------------------------------------- rejection

  @Test
  protected void aKindTheAssemblyHoldsNoStoreForIsRejectedBeforeAnythingCommits() {
    RecordKind unknown = RecordKind.of("someone-elses.thing");
    assertThatThrownBy(
            () ->
                orchestrator.commit(
                    List.of(
                        createEntity(entity(10L, "a", 1)),
                        Mutation.of(
                            unknown,
                            Mutation.Op.CREATE,
                            RecordRef.byIdentity(unknown, List.of(1L)),
                            new Object()))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(log).isEmpty();
    assertThat(main.get(entityRef(10L), PolarisBaseEntity.class)).isEmpty();
  }

  @Test
  protected void aSingleMutationWhoseUnionSpansDomainsIsRejectedBeforeAnythingCommits() {
    // an entity write (main) gated on a grant (authz): no store can evaluate both atomically
    PolarisBaseEntity e = entity(10L, "a", 1);
    Mutation spanning =
        Mutation.of(
            PolarisRecordKinds.ENTITY,
            Mutation.Op.CREATE,
            entityRef(10L),
            e,
            List.of(Precondition.exists(grantRef(grant(10L, 20L)))));

    assertThatThrownBy(() -> orchestrator.commit(List.of(spanning)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(log).isEmpty();
    assertThat(main.get(entityRef(10L), PolarisBaseEntity.class)).isEmpty();
  }

  // ---------------------------------------------------------------- two real writers

  @Test
  protected void aStaleBaselineCommitSurfacesAsALoudConflictEndToEnd() {
    // writer 1 and writer 2 both read version 1; nothing here is mocked — the conflict is
    // produced by a real competing commit through the same orchestrator and stores
    assertThat(orchestrator.commit(List.of(createEntity(entity(10L, "catalog", 1)))).isApplied())
        .isTrue();
    long baselineOfBothWriters =
        main.get(entityRef(10L), PolarisBaseEntity.class).orElseThrow().getEntityVersion();

    // writer 2 commits first: version 1 → 2
    OrchestrationResult writer2 =
        orchestrator.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.ENTITY,
                    Mutation.Op.UPDATE,
                    entityRef(10L),
                    entity(10L, "catalog", 2),
                    List.of(
                        Precondition.versionEquals(
                            entityRef(10L),
                            Precondition.VersionAttribute.RECORD_VERSION,
                            baselineOfBothWriters)))));
    assertThat(writer2.isApplied()).isTrue();

    // writer 1's commit still carries the stale baseline: a loud conflict, never a lost update
    OrchestrationResult writer1 =
        orchestrator.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.ENTITY,
                    Mutation.Op.UPDATE,
                    entityRef(10L),
                    entity(10L, "overwritten", 2),
                    List.of(
                        Precondition.versionEquals(
                            entityRef(10L),
                            Precondition.VersionAttribute.RECORD_VERSION,
                            baselineOfBothWriters)))));

    assertThat(writer1.outcome()).isEqualTo(OrchestrationResult.Outcome.ROLLED_BACK);
    assertThat(writer1.groupFailure())
        .get()
        .extracting(CommitResult::failure)
        .extracting(java.util.Optional::get)
        .isEqualTo(CommitResult.Failure.PRECONDITION_FAILED);

    // writer 2's write is intact: the conflict was loud, the state is writer 2's
    assertThat(main.get(entityRef(10L), PolarisBaseEntity.class))
        .get()
        .satisfies(
            e -> {
              assertThat(e.getEntityVersion()).isEqualTo(2);
              assertThat(e.getName()).isEqualTo("catalog");
            });
  }
}
