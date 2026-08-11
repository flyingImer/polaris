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
 * compensation. The store here is a scripted double so the cases observe what the orchestrator
 * does, never how any real store achieves atomicity; the end-to-end multi-store path against real
 * stores is {@link TwoStoreDemoTest}.
 *
 * <p>Both doubles deliberately declare the same constant domain VALUE: domain values are opaque and
 * comparable only within the store that declared them, so equal values from two stores must still
 * be two domains. The grouping cases would break if the implementation keyed on the value alone.
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
    Map<RecordKind, DurableRecordStore> wiring = Map.of(ALPHA, storeA, BETA, storeA, GAMMA, storeB);
    orchestrator = new DefaultDurableOrchestrator(wiring::get);
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
  void equalDomainValuesFromTwoStoresAreStillTwoDomains() {
    // Both doubles declare the same constant "d"; keying on the value alone would merge them
    // into one group and reach one store with the other store's mutations.
    OrchestrationResult result = orchestrator.commit(List.of(create(ALPHA, 1), create(GAMMA, 2)));

    assertThat(result.isApplied()).isTrue();
    assertThat(storeA.commits).hasSize(1);
    assertThat(storeA.commits.get(0)).extracting(Mutation::kind).containsExactly(ALPHA);
    assertThat(storeB.commits).hasSize(1);
    assertThat(storeB.commits.get(0)).extracting(Mutation::kind).containsExactly(GAMMA);
  }

  @Test
  void conditionTargetsPullAMutationIntoTheWritersGroup() {
    // M2 conditions on the record M1 writes. Despite the store-B mutation sitting between them
    // in the list, M2 lands in the same single commit as the writer of its condition target —
    // the condition is evaluated atomically with the write it depends on.
    Mutation m1 = create(ALPHA, 1);
    Mutation m3 = create(GAMMA, 3);
    Mutation m2 =
        Mutation.of(
            BETA,
            Mutation.Op.CREATE,
            ref(BETA, 2),
            "record-2",
            List.of(Precondition.exists(ref(ALPHA, 1))));

    OrchestrationResult result = orchestrator.commit(List.of(m1, m3, m2));

    assertThat(result.isApplied()).isTrue();
    assertThat(storeA.commits).hasSize(1);
    assertThat(storeA.commits.get(0)).containsExactly(m1, m2);
    assertThat(storeB.commits).hasSize(1);
    assertThat(storeB.commits.get(0)).containsExactly(m3);
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
    // Store A saw its original commit, then the compensating one: the created record deleted,
    // with the payload carried (one shipped store derives its key from the payload).
    assertThat(storeA.commits).hasSize(2);
    Mutation undo = storeA.commits.get(1).get(0);
    assertThat(undo.op()).isEqualTo(Mutation.Op.DELETE);
    assertThat(undo.target()).isEqualTo(ref(ALPHA, 1));
    assertThat(undo.record()).isEqualTo("record-1");
  }

  @Test
  void rollbackRunsInReverseCommitOrderAcrossThreeGroups() {
    // Three domains inside store A via a per-target domain declaration, plus the scripted
    // failure on the last: the two committed groups roll back last-committed-first.
    List<String> order = new ArrayList<>();
    TestStore partitioned = new TestStore("P", order);
    partitioned.domainFn = target -> target.key().get(0); // domain = the id itself
    Map<RecordKind, DurableRecordStore> wiring = Map.of(ALPHA, partitioned);
    DefaultDurableOrchestrator local = new DefaultDurableOrchestrator(wiring::get);

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
   * domains via a configurable function (constant by default, deliberately the same constant in
   * every instance), and fails on script. Reads the orchestrator never needs are unsupported.
   */
  private static final class TestStore implements DurableRecordStore {
    final List<List<Mutation>> commits = new ArrayList<>();
    final List<RecordRef> reads = new ArrayList<>();
    final Map<RecordRef, Object> records = new HashMap<>();
    final String name;
    final List<String> sequence;
    Function<List<Mutation>, CommitResult> responder = mutations -> CommitResult.applied();
    Function<RecordRef, Object> domainFn = target -> "d";

    TestStore(String name, List<String> sequence) {
      this.name = name;
      this.sequence = sequence;
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
