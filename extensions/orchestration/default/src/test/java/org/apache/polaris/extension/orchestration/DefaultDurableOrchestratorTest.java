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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.LookupPath;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.OrchestrationResult;
import org.apache.polaris.spi.durable.Precondition;
import org.apache.polaris.spi.durable.RecordKind;
import org.apache.polaris.spi.durable.RecordRef;
import org.apache.polaris.spi.durable.RecordVersions;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Behaviour of the orchestration seam, asserted against the contract only: grouping, ordering,
 * compensation. The stores here are scripted doubles behind {@link KindRoutedTestStore}, the
 * contract-shaped stand-in for the routing primitives handle, so the cases observe what the
 * orchestrator does, never how any real store achieves atomicity; the end-to-end multi-store path
 * against real stores is {@link TwoStoreDemoTest}, and the orchestrator runs over the REAL routing
 * implementation in that module's Seam-2 conformance runner and two-store tests.
 *
 * <p>The orchestrator holds ONE primitives handle and groups by the bare {@code domainOf} value it
 * answers (the (store, value) pair collapsed by decision 2026-08-18); each double therefore
 * declares its own distinct default value, mirroring the identity-distinct declarations of the
 * shipped stores. The colliding-values case — two backends declaring equal values behind one
 * routing handle — is pinned where its subject lives, in the routing store's own tests: it fails
 * loudly as {@code DOMAIN_MISMATCH}, never as a silent cross-store commit.
 */
class DefaultDurableOrchestratorTest {

  private static final RecordKind ALPHA = RecordKind.of("test.alpha");
  private static final RecordKind BETA = RecordKind.of("test.beta");
  private static final RecordKind GAMMA = RecordKind.of("test.gamma");

  private List<String> commitSequence;
  private TestStore storeA;
  private TestStore storeB;
  private DefaultDurableOrchestrator orchestrator;

  @BeforeEach
  void setUp() {
    commitSequence = new ArrayList<>();
    storeA = new TestStore("A", commitSequence);
    storeB = new TestStore("B", commitSequence);
    orchestrator =
        new DefaultDurableOrchestrator(
            new KindRoutedTestStore(Map.of(ALPHA, storeA, BETA, storeA, GAMMA, storeB)));
  }

  private static RecordRef ref(RecordKind kind, long id) {
    return RecordRef.byIdentity(kind, List.of(id));
  }

  private static Mutation create(RecordKind kind, long id) {
    return Mutation.of(kind, Mutation.Op.CREATE, ref(kind, id), "record-" + id);
  }

  // ---------------------------------------------------------------- grouping

  @Test
  void oneDomainListProducesExactlyOneCommit() {
    OrchestrationResult result =
        orchestrator.commit(List.of(create(ALPHA, 1), create(BETA, 2), create(ALPHA, 3)));

    assertThat(result.isApplied()).isTrue();
    assertThat(storeA.commits).hasSize(1);
    assertThat(storeA.commits.get(0)).hasSize(3);
    assertThat(storeB.commits).isEmpty();
  }

  @Test
  void twoDomainListProducesTwoCommitsInListOrder() {
    OrchestrationResult result = orchestrator.commit(List.of(create(GAMMA, 1), create(ALPHA, 2)));

    assertThat(result.isApplied()).isTrue();
    assertThat(storeA.commits).hasSize(1);
    assertThat(storeB.commits).hasSize(1);
    // The caller's list order is the write order across domains: GAMMA (store B) appears first.
    assertThat(commitSequence).containsExactly("B", "A");
  }

  @Test
  void conditionTargetsPullAMutationIntoTheWritersGroup() {
    // M2 is adjacent to M1 and conditions on the record M1 writes: both ride in one commit, so
    // the condition is evaluated atomically with the write it depends on. The condition target's
    // domain participates in M2's domain (the union rule); the cross-store rejection case below
    // proves the participation is real.
    Mutation m1 = create(ALPHA, 1);
    Mutation m2 =
        Mutation.of(
            BETA,
            Mutation.Op.CREATE,
            ref(BETA, 2),
            "record-2",
            List.of(Precondition.exists(ref(ALPHA, 1))));
    Mutation m3 = create(GAMMA, 3);

    OrchestrationResult result = orchestrator.commit(List.of(m1, m2, m3));

    assertThat(result.isApplied()).isTrue();
    assertThat(storeA.commits).hasSize(1);
    assertThat(storeA.commits.get(0)).containsExactly(m1, m2);
    assertThat(storeB.commits).hasSize(1);
    assertThat(storeB.commits.get(0)).containsExactly(m3);
  }

  @Test
  void sameDomainMutationsSeparatedInTheListStaySeparate() {
    // Separation is semantic: the caller ordered another domain's write between two same-domain
    // writes, so the two stay separate commits in list order — never hoisted together. On a later
    // failure, the two same-domain groups also roll back as separate commits, in reverse order.
    storeA.responder =
        mutations ->
            mutations.get(0).target().key().get(0).equals(3L)
                ? CommitResult.preconditionFailed(List.of(Precondition.none()))
                : CommitResult.applied();

    Mutation first = create(ALPHA, 1);
    OrchestrationResult result =
        orchestrator.commit(List.of(first, create(GAMMA, 2), create(ALPHA, 3)));

    assertThat(result.outcome()).isEqualTo(OrchestrationResult.Outcome.ROLLED_BACK);
    // Forward: A[1], B[2], A[3] fails. Rollback: B undo, then A undo — strict reverse.
    assertThat(commitSequence).containsExactly("A", "B", "A", "B", "A");
    assertThat(storeA.commits).hasSize(3);
    assertThat(storeA.commits.get(0)).containsExactly(first);
    assertThat(storeA.commits.get(2).get(0).op()).isEqualTo(Mutation.Op.DELETE);
    assertThat(storeA.commits.get(2).get(0).target()).isEqualTo(ref(ALPHA, 1));
    assertThat(storeB.commits).hasSize(2);
    assertThat(storeB.commits.get(1).get(0).op()).isEqualTo(Mutation.Op.DELETE);
  }

  @Test
  void mutationWhoseConditionLivesInAnotherStoreIsRejectedBeforeAnythingCommits() {
    // The write lands in store B, the condition references a record in store A: no backend can
    // evaluate the condition atomically with the write, so the whole request is refused up front.
    Mutation spanning =
        Mutation.of(
            GAMMA,
            Mutation.Op.CREATE,
            ref(GAMMA, 2),
            "record-2",
            List.of(Precondition.exists(ref(ALPHA, 1))));

    assertThatThrownBy(() -> orchestrator.commit(List.of(create(ALPHA, 1), spanning)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("share one atomicity domain");
    assertThat(storeA.commits).isEmpty();
    assertThat(storeB.commits).isEmpty();
  }

  @Test
  void unknownKindIsRejectedWhetherWrittenOrConditionedOn() {
    RecordKind unknown = RecordKind.of("test.unknown");

    assertThatThrownBy(() -> orchestrator.commit(List.of(create(unknown, 1))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("test.unknown");

    Mutation conditioned =
        Mutation.of(
            ALPHA,
            Mutation.Op.CREATE,
            ref(ALPHA, 1),
            "record-1",
            List.of(Precondition.exists(ref(unknown, 9))));
    assertThatThrownBy(() -> orchestrator.commit(List.of(conditioned)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("test.unknown");
    assertThat(storeA.commits).isEmpty();
    assertThat(storeB.commits).isEmpty();
  }

  @Test
  void emptyListAppliesVacuously() {
    assertThat(orchestrator.commit(List.of()).isApplied()).isTrue();
    assertThat(commitSequence).isEmpty();
  }

  // ---------------------------------------------------------------- compensation

  @Test
  void failureInTheSecondGroupRollsTheFirstBack() {
    storeB.responder = mutations -> CommitResult.preconditionFailed(List.of(Precondition.none()));

    OrchestrationResult result = orchestrator.commit(List.of(create(ALPHA, 1), create(GAMMA, 2)));

    assertThat(result.outcome()).isEqualTo(OrchestrationResult.Outcome.ROLLED_BACK);
    assertThat(result.groupFailure()).isPresent();
    assertThat(result.groupFailure().get().failure())
        .contains(CommitResult.Failure.PRECONDITION_FAILED);
    // Store A saw its original commit, then the compensating one: the created record deleted by
    // its target ref alone, no payload (the DELETE addressing contract, decided 2026-08-13).
    assertThat(storeA.commits).hasSize(2);
    Mutation undo = storeA.commits.get(1).get(0);
    assertThat(undo.op()).isEqualTo(Mutation.Op.DELETE);
    assertThat(undo.target()).isEqualTo(ref(ALPHA, 1));
    assertThat(undo.record()).isNull();
  }

  @Test
  void rollbackRunsInReverseCommitOrderAcrossThreeGroups() {
    // Three domains inside store A via a per-target domain declaration, plus the scripted
    // failure on the last: the two committed groups roll back last-committed-first.
    List<String> order = new ArrayList<>();
    TestStore partitioned = new TestStore("P", order);
    partitioned.domainFn = target -> target.key().get(0); // domain = the id itself
    DefaultDurableOrchestrator local =
        new DefaultDurableOrchestrator(new KindRoutedTestStore(Map.of(ALPHA, partitioned)));

    partitioned.responder =
        mutations ->
            mutations.get(0).target().key().get(0).equals(3L)
                ? CommitResult.preconditionFailed(List.of(Precondition.none()))
                : CommitResult.applied();

    OrchestrationResult result =
        local.commit(List.of(create(ALPHA, 1), create(ALPHA, 2), create(ALPHA, 3)));

    assertThat(result.outcome()).isEqualTo(OrchestrationResult.Outcome.ROLLED_BACK);
    // Commits: group(1), group(2), group(3) fails, then undo(2), undo(1) — reverse write order.
    assertThat(partitioned.commits).hasSize(5);
    assertThat(partitioned.commits.get(3).get(0).target()).isEqualTo(ref(ALPHA, 2));
    assertThat(partitioned.commits.get(4).get(0).target()).isEqualTo(ref(ALPHA, 1));
  }

  @Test
  void updateAndDeleteInvertToPriorStateReadBeforeTheCommit() {
    storeA.records.put(ref(ALPHA, 1), "prior-1");
    storeA.records.put(ref(BETA, 2), "prior-2");
    storeB.responder = mutations -> CommitResult.tooManyItems();

    Mutation update = Mutation.of(ALPHA, Mutation.Op.UPDATE, ref(ALPHA, 1), "new-1");
    Mutation delete = Mutation.of(BETA, Mutation.Op.DELETE, ref(BETA, 2), null);
    OrchestrationResult result = orchestrator.commit(List.of(update, delete, create(GAMMA, 3)));

    assertThat(result.outcome()).isEqualTo(OrchestrationResult.Outcome.ROLLED_BACK);
    assertThat(result.groupFailure().get().failure()).contains(CommitResult.Failure.TOO_MANY_ITEMS);
    assertThat(storeA.commits).hasSize(2);
    List<Mutation> undo = storeA.commits.get(1);
    assertThat(undo).hasSize(2);
    assertThat(undo.get(0).op()).isEqualTo(Mutation.Op.UPDATE);
    assertThat(undo.get(0).record()).isEqualTo("prior-1");
    assertThat(undo.get(1).op()).isEqualTo(Mutation.Op.CREATE);
    assertThat(undo.get(1).record()).isEqualTo("prior-2");
  }

  @Test
  void compensationFailureReportsRollbackIncompleteWithWhatRemains() {
    Mutation first = create(ALPHA, 1);
    storeA.responder =
        new Function<>() {
          int calls;

          @Override
          public CommitResult apply(List<Mutation> mutations) {
            // First call is the original commit; the second is the compensating one.
            return ++calls == 1
                ? CommitResult.applied()
                : CommitResult.preconditionFailed(List.of(Precondition.none()));
          }
        };
    storeB.responder = mutations -> CommitResult.preconditionFailed(List.of(Precondition.none()));

    OrchestrationResult result = orchestrator.commit(List.of(first, create(GAMMA, 2)));

    assertThat(result.outcome()).isEqualTo(OrchestrationResult.Outcome.ROLLBACK_INCOMPLETE);
    assertThat(result.uncompensated()).containsExactly(first);
  }

  @Test
  void overCapGroupFailurePassesThroughWithoutSplitting() {
    storeA.responder = mutations -> CommitResult.tooManyItems();

    OrchestrationResult result = orchestrator.commit(List.of(create(ALPHA, 1), create(ALPHA, 2)));

    assertThat(result.outcome()).isEqualTo(OrchestrationResult.Outcome.ROLLED_BACK);
    assertThat(result.groupFailure().get().failure()).contains(CommitResult.Failure.TOO_MANY_ITEMS);
    // Exactly one attempt: an over-cap group is the caller's to shrink, never silently split.
    assertThat(storeA.commits).hasSize(1);
  }

  @Test
  void storeThrowTriggersCompensationThenPropagates() {
    RuntimeException boom = new RuntimeException("boom");
    storeB.responder =
        mutations -> {
          throw boom;
        };

    assertThatThrownBy(() -> orchestrator.commit(List.of(create(ALPHA, 1), create(GAMMA, 2))))
        .isSameAs(boom);

    // Compensation ran before the exception propagated, and completed: no suppressed disclosure.
    assertThat(storeA.commits).hasSize(2);
    assertThat(storeA.commits.get(1).get(0).op()).isEqualTo(Mutation.Op.DELETE);
    assertThat(boom.getSuppressed()).isEmpty();
  }

  @Test
  void storeThrowWithFailedCompensationCarriesTheDisclosureAsSuppressed() {
    RuntimeException boom = new RuntimeException("boom");
    storeA.responder =
        new Function<>() {
          int calls;

          @Override
          public CommitResult apply(List<Mutation> mutations) {
            if (++calls == 1) {
              return CommitResult.applied();
            }
            throw new RuntimeException("undo also failed");
          }
        };
    storeB.responder =
        mutations -> {
          throw boom;
        };

    assertThatThrownBy(() -> orchestrator.commit(List.of(create(ALPHA, 1), create(GAMMA, 2))))
        .isSameAs(boom);
    assertThat(boom.getSuppressed()).hasSize(1);
    assertThat(boom.getSuppressed()[0]).hasMessageContaining("rollback incomplete");
  }

  @Test
  void theLastGroupCapturesNoUndoState() {
    // Undo state serves a later group's failure; the last group of any N has none, N=1 included.
    // Observable as: a single-domain commit performs no prior-state reads at all.
    storeA.records.put(ref(ALPHA, 1), "prior-1");

    orchestrator.commit(List.of(Mutation.of(ALPHA, Mutation.Op.UPDATE, ref(ALPHA, 1), "new-1")));

    assertThat(storeA.reads).isEmpty();
  }

  // ---------------------------------------------------------------- the double

  /**
   * A scripted store: records every commit and read, answers get() from a primed map, declares
   * domains via a configurable function (a per-instance constant by default, mirroring the shipped
   * stores' identity-distinct declarations), and fails on script. Reads the orchestrator never
   * needs are unsupported.
   */
  private static final class TestStore implements DurableRecordStore {
    final List<List<Mutation>> commits = new ArrayList<>();
    final List<RecordRef> reads = new ArrayList<>();
    final Map<RecordRef, Object> records = new HashMap<>();
    final String name;
    final List<String> sequence;
    Function<List<Mutation>, CommitResult> responder = mutations -> CommitResult.applied();
    Function<RecordRef, Object> domainFn;

    TestStore(String name, List<String> sequence) {
      this.name = name;
      this.sequence = sequence;
      this.domainFn = target -> "domain-of-" + name;
    }

    @Override
    public @NonNull CommitResult commit(@NonNull List<Mutation> mutations) {
      sequence.add(name);
      commits.add(List.copyOf(mutations));
      return responder.apply(mutations);
    }

    @Override
    public long generateNewId() {
      throw new UnsupportedOperationException();
    }

    @Override
    public @NonNull <T> Optional<T> get(@NonNull RecordRef ref, @NonNull Class<T> type) {
      reads.add(ref);
      return Optional.ofNullable(records.get(ref)).map(type::cast);
    }

    @Override
    public @NonNull <T> List<Optional<T>> getMany(
        @NonNull List<RecordRef> refs, @NonNull Class<T> type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public @NonNull <T> Page<T> list(
        @NonNull RecordKind kind,
        @NonNull LookupPath path,
        @NonNull List<Object> anchors,
        @NonNull PageToken pageToken,
        @NonNull Class<T> type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public @NonNull <T> Page<T> list(
        @NonNull LookupPath path,
        @NonNull List<Object> anchors,
        @NonNull PageToken pageToken,
        @NonNull Class<T> type) {
      throw new UnsupportedOperationException();
    }

    @Override
    public @NonNull List<Optional<RecordVersions>> versionsOf(@NonNull List<RecordRef> refs) {
      throw new UnsupportedOperationException();
    }

    @Override
    public @NonNull Object domainOf(@NonNull RecordRef target) {
      return domainFn.apply(target);
    }

    @Override
    public int maxItemsPerCommit() {
      return 1000;
    }
  }
}
