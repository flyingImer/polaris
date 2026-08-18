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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableOrchestrator;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.OrchestrationResult;
import org.apache.polaris.spi.durable.Precondition;
import org.apache.polaris.spi.durable.RecordRef;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The default implementation of {@link DurableOrchestrator}, and the in-process one: groups
 * in-process, commits each group through the one primitives handle, compensates synchronously on
 * this thread.
 *
 * <p><b>Construction takes ONE primitives handle and nothing else about storage.</b> This class
 * holds a single {@link DurableRecordStore} — in a multi-store deployment that handle is the
 * routing implementation, whose kind-to-store mapping hides BEHIND the primitives SPI (decided
 * 2026-08-18, dissolving the factory whose resolved kind-keyed function used to be handed here).
 * Orchestration therefore never learns that stores exist: it asks the handle {@code domainOf} per
 * target and groups by the answers. A kind the handle cannot resolve is rejected by the handle
 * itself, naming the kind, before anything commits.
 *
 * <p><b>A domain is the opaque {@code domainOf} value alone.</b> The handle is the single declaring
 * store, so its values are comparable among themselves by the SPI's own rule (opaque, comparable
 * within the declaring store). The routing implementation forwards each backend's identity-distinct
 * declaration untouched, which is what keeps two co-located kinds mergeable; a hypothetical backend
 * pair declaring colliding values degrades LOUDLY — the merged group is refused by the routing
 * {@code commit} as {@code DOMAIN_MISMATCH} — never silently.
 *
 * <p><b>One code path for any number of domains.</b> The list is grouped, the groups commit in
 * order, and a failure compensates the groups already committed. A single-domain list produces one
 * group and runs the same loop; nothing here asks how many domains exist. The only
 * position-sensitive step is undo capture, below.
 *
 * <p><b>Undo state is captured for every group except the last, and that is a property of position,
 * not of domain count.</b> Undo information for a group exists to reverse it when a LATER group
 * fails; the failing group itself applied nothing, because a primitives commit is atomic. The last
 * group of any N therefore captures nothing, N=1 included. The alternative, capturing uniformly for
 * every group, was considered and rejected: it would tax every single-domain commit, the production
 * hot path, with prior-state reads that can never be used. This choice is the implementer's,
 * disclosed here; the records fix the compensation semantics (synchronous, reverse write order) but
 * not the capture mechanics.
 *
 * <p><b>How a committed group is reversed.</b> At capture time, each mutation yields its inverse: a
 * CREATE's inverse is a DELETE of the created record; an UPDATE's inverse restores the prior state
 * read just before the group committed; a DELETE's inverse re-creates the prior state. The inverses
 * are unconditional, because compensation runs on an error path where refusing to compensate over a
 * lost race would convert an ordinary failure into lingering partial state. A compensating DELETE
 * carries no payload: the contract pins DELETE to target-ref addressing with a null payload
 * (decided 2026-08-13, closing the divergence the conformance suite surfaced — one store used to
 * derive its delete key from the payload and now resolves the ref instead).
 *
 * <p><b>Two windows this class does not close, disclosed rather than hidden.</b> Between a prior
 * state's capture and the compensating write, a concurrent writer's change can be overwritten by
 * the unconditional restore; and a crash between a group's commit and the rollback's completion
 * leaves partial state with no compensation having run. Both are the recorded cost of compensation
 * without cross-domain atomicity; the partial states are inert and reclaimable through normal admin
 * paths, and cross-domain atomicity via commit-logging is a post-GA enhancement.
 */
public class DefaultDurableOrchestrator implements DurableOrchestrator {

  private final DurableRecordStore primitives;

  /**
   * @param primitives the one primitives handle every read, commit, and declaration goes through —
   *     the routing implementation in a multi-store deployment, a single backend otherwise
   */
  public DefaultDurableOrchestrator(@NonNull DurableRecordStore primitives) {
    this.primitives = primitives;
  }

  /** One adjacent run of same-domain mutations: what the loop below hands to one commit. */
  private record Group(Object domain, List<Mutation> mutations) {
    private Group {
      Objects.requireNonNull(domain);
    }
  }

  /**
   * A committed group remembered for possible rollback: what was applied, and how to reverse it.
   */
  private record CommittedGroup(List<Mutation> mutations, List<Mutation> undo) {}

  @Override
  public @NonNull OrchestrationResult commit(@NonNull List<Mutation> mutations) {
    List<Group> groups = groupAdjacent(mutations);

    List<CommittedGroup> committed = new ArrayList<>();
    for (int index = 0; index < groups.size(); index++) {
      List<Mutation> group = groups.get(index).mutations();
      boolean hasSuccessor = index + 1 < groups.size();

      List<Mutation> undo = hasSuccessor ? captureUndo(group) : List.of();

      CommitResult result;
      try {
        result = primitives.commit(group);
      } catch (RuntimeException e) {
        List<Mutation> uncompensated = compensate(committed);
        if (!uncompensated.isEmpty()) {
          e.addSuppressed(
              new IllegalStateException(
                  "rollback incomplete: "
                      + uncompensated.size()
                      + " mutation(s) remain applied and await reclamation: "
                      + uncompensated));
        }
        throw e;
      }

      if (!result.isApplied()) {
        List<Mutation> uncompensated = compensate(committed);
        return uncompensated.isEmpty()
            ? OrchestrationResult.rolledBack(result)
            : OrchestrationResult.rollbackIncomplete(result, uncompensated);
      }

      committed.add(new CommittedGroup(group, undo));
    }
    return OrchestrationResult.applied();
  }

  /**
   * Merges ADJACENT mutations with equal domains into groups, strictly preserving list order.
   *
   * <p>Separation in the list is semantic, never optimized away: two same-domain mutations with
   * another domain's mutation between them stay in separate commits, in list order. The caller's
   * order is the program — it is what makes every crash window a prefix of the caller's intended
   * writes, which is the reasoning Option A's inert-partial-state story rests on — and a caller who
   * wants two mutations atomic together says so by placing them adjacent. Hoisting a later mutation
   * forward would change the program silently, the same class of silent semantic change as the
   * split S12 forbids. (EJ, 2026-08-11, amending the earlier by-equality wording.)
   *
   * <p>A mutation's own domain is the union over its write target and every record its
   * preconditions reference. The union must be one domain: a condition split from its write across
   * domains cannot be evaluated atomically with it by any backend, and compensation operates across
   * groups, never inside one mutation, so such a mutation is a caller error caught here before
   * anything commits.
   */
  private List<Group> groupAdjacent(List<Mutation> mutations) {
    List<Group> groups = new ArrayList<>();
    Group current = null;
    for (Mutation mutation : mutations) {
      Object domain = domainOf(mutation.target());
      for (Precondition precondition : mutation.preconditions()) {
        Optional<RecordRef> conditionRef = precondition.ref();
        if (conditionRef.isEmpty()) {
          continue;
        }
        Object conditionDomain = domainOf(conditionRef.get());
        if (!domain.equals(conditionDomain)) {
          throw new IllegalArgumentException(
              "A mutation's write target and condition targets must share one atomicity domain;"
                  + " a condition split from its write cannot be evaluated atomically with it."
                  + " Write target "
                  + mutation.target()
                  + " and condition target "
                  + conditionRef.get()
                  + " resolve to different domains. Restructure the operation: consult the other"
                  + " record with a read and accept its staleness, or co-locate the kinds.");
        }
      }
      if (current == null || !current.domain().equals(domain)) {
        current = new Group(domain, new ArrayList<>());
        groups.add(current);
      }
      current.mutations().add(mutation);
    }
    return groups;
  }

  private Object domainOf(RecordRef ref) {
    return primitives.domainOf(ref);
  }

  /**
   * Builds the group's inverse just before it commits: prior state is read now, as close to the
   * commit as the no-interactive-transactions rule allows, so the restore is as fresh as it can be.
   * An UPDATE whose target is concurrently absent inverts to a DELETE of what the update is about
   * to write; a DELETE whose target is already absent needs no inverse at all.
   */
  private List<Mutation> captureUndo(List<Mutation> group) {
    List<Mutation> undo = new ArrayList<>(group.size());
    for (Mutation mutation : group) {
      Mutation inverse = inverseOf(mutation);
      if (inverse != null) {
        undo.add(inverse);
      }
    }
    return undo;
  }

  private @Nullable Mutation inverseOf(Mutation mutation) {
    return switch (mutation.op()) {
      case CREATE -> Mutation.of(mutation.kind(), Mutation.Op.DELETE, mutation.target(), null);
      case UPDATE -> {
        Optional<Object> prior = primitives.get(mutation.target(), Object.class);
        yield prior
            .map(p -> Mutation.of(mutation.kind(), Mutation.Op.UPDATE, mutation.target(), p))
            .orElseGet(
                () -> Mutation.of(mutation.kind(), Mutation.Op.DELETE, mutation.target(), null));
      }
      case DELETE -> {
        Optional<Object> prior = primitives.get(mutation.target(), Object.class);
        yield prior
            .map(p -> Mutation.of(mutation.kind(), Mutation.Op.CREATE, mutation.target(), p))
            .orElse(null);
      }
    };
  }

  /**
   * Rolls the committed groups back in reverse commit order. A compensating commit that fails or
   * throws does not stop the earlier groups' rollback: reversing as much as possible minimizes the
   * partial state left for reclamation. Returns the original mutations whose effects remain.
   */
  private List<Mutation> compensate(List<CommittedGroup> committed) {
    List<Mutation> uncompensated = new ArrayList<>();
    for (int i = committed.size() - 1; i >= 0; i--) {
      CommittedGroup group = committed.get(i);
      if (group.undo().isEmpty()) {
        // Nothing to reverse: a group whose every mutation deleted an already-absent record left
        // no effect behind, so its rollback is trivially complete without a store round trip.
        continue;
      }
      try {
        CommitResult result = primitives.commit(group.undo());
        if (!result.isApplied()) {
          uncompensated.addAll(group.mutations());
        }
      } catch (RuntimeException e) {
        uncompensated.addAll(group.mutations());
      }
    }
    return uncompensated;
  }
}
