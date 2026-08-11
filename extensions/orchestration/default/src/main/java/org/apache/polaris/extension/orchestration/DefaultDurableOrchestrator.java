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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableOrchestrator;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.OrchestrationResult;
import org.apache.polaris.spi.durable.Precondition;
import org.apache.polaris.spi.durable.RecordKind;
import org.apache.polaris.spi.durable.RecordRef;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The default implementation of {@link DurableOrchestrator}, and the in-process one: groups
 * in-process, commits each group on its own primitives implementation, compensates synchronously on
 * this thread.
 *
 * <p><b>Construction is the hand-wired stand-in for the factory's realm-scoped assembly.</b> The
 * kind-to-store-name-to-implementation mapping machinery is the factory's business; this class
 * takes the already-resolved form of it, a function from record kind to the store holding that
 * kind, and asks nothing else about topology. A kind the function cannot resolve is a configuration
 * error and is rejected, never guessed at.
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
 * carries the created record as its payload rather than the null the carrier permits, because one
 * shipped store derives the slice key from the payload; the divergence is a conformance question
 * tracked outside this ticket, and carrying the payload is correct under both readings.
 *
 * <p><b>Two windows this class does not close, disclosed rather than hidden.</b> Between a prior
 * state's capture and the compensating write, a concurrent writer's change can be overwritten by
 * the unconditional restore; and a crash between a group's commit and the rollback's completion
 * leaves partial state with no compensation having run. Both are the recorded cost of compensation
 * without cross-domain atomicity; the partial states are inert and reclaimable through normal admin
 * paths, and cross-domain atomicity via commit-logging is a post-GA enhancement.
 */
public class DefaultDurableOrchestrator implements DurableOrchestrator {

  private final Function<RecordKind, DurableRecordStore> storeForKind;

  /**
   * @param storeForKind resolves the store holding each record kind — the already-resolved form of
   *     the factory's realm-scoped assembly. Returning null marks the kind unknown to this
   *     assembly.
   */
  public DefaultDurableOrchestrator(
      @NonNull Function<RecordKind, DurableRecordStore> storeForKind) {
    this.storeForKind = storeForKind;
  }

  /**
   * A group's atomicity domain: the declaring store and its opaque domain value. The store is part
   * of the key because domain values are opaque and comparable only within the store that declared
   * them; two distinct stores may both declare equal-looking constants without sharing a domain.
   * Store identity, not store equality: two instances are two stores.
   */
  private record Domain(DurableRecordStore store, Object value) {
    private Domain {
      Objects.requireNonNull(store);
      Objects.requireNonNull(value);
    }
  }

  /** A committed group remembered for possible rollback: where, what, and how to reverse it. */
  private record CommittedGroup(
      DurableRecordStore store, List<Mutation> mutations, List<Mutation> undo) {}

  @Override
  public @NonNull OrchestrationResult commit(@NonNull List<Mutation> mutations) {
    Map<Domain, List<Mutation>> groups = groupByDomain(mutations);

    List<CommittedGroup> committed = new ArrayList<>();
    int index = 0;
    for (Map.Entry<Domain, List<Mutation>> entry : groups.entrySet()) {
      DurableRecordStore store = entry.getKey().store();
      List<Mutation> group = entry.getValue();
      boolean hasSuccessor = ++index < groups.size();

      List<Mutation> undo = hasSuccessor ? captureUndo(store, group) : List.of();

      CommitResult result;
      try {
        result = store.commit(group);
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

      committed.add(new CommittedGroup(store, group, undo));
    }
    return OrchestrationResult.applied();
  }

  /**
   * Groups the list by atomicity domain, preserving the order in which each domain first appears —
   * the caller's list order is the write order across domains.
   *
   * <p>A mutation's domain is the union over its write target and every record its preconditions
   * reference. The union must be one domain: a condition split from its write across domains cannot
   * be evaluated atomically with it by any backend, and compensation operates across groups, never
   * inside one mutation, so such a mutation is a caller error caught here before anything commits.
   */
  private Map<Domain, List<Mutation>> groupByDomain(List<Mutation> mutations) {
    Map<Domain, List<Mutation>> groups = new LinkedHashMap<>();
    for (Mutation mutation : mutations) {
      Domain domain = domainOf(mutation.target());
      for (Precondition precondition : mutation.preconditions()) {
        Optional<RecordRef> conditionRef = precondition.ref();
        if (conditionRef.isEmpty()) {
          continue;
        }
        Domain conditionDomain = domainOf(conditionRef.get());
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
      groups.computeIfAbsent(domain, key -> new ArrayList<>()).add(mutation);
    }
    return groups;
  }

  private Domain domainOf(RecordRef ref) {
    DurableRecordStore store = storeForKind.apply(ref.kind());
    if (store == null) {
      throw new IllegalArgumentException(
          "No store holds record kind '" + ref.kind().id() + "' in this assembly");
    }
    return new Domain(store, store.domainOf(ref));
  }

  /**
   * Builds the group's inverse just before it commits: prior state is read now, as close to the
   * commit as the no-interactive-transactions rule allows, so the restore is as fresh as it can be.
   * An UPDATE whose target is concurrently absent inverts to a DELETE of what the update is about
   * to write; a DELETE whose target is already absent needs no inverse at all.
   */
  private List<Mutation> captureUndo(DurableRecordStore store, List<Mutation> group) {
    List<Mutation> undo = new ArrayList<>(group.size());
    for (Mutation mutation : group) {
      Mutation inverse = inverseOf(store, mutation);
      if (inverse != null) {
        undo.add(inverse);
      }
    }
    return undo;
  }

  private @Nullable Mutation inverseOf(DurableRecordStore store, Mutation mutation) {
    return switch (mutation.op()) {
      case CREATE ->
          Mutation.of(mutation.kind(), Mutation.Op.DELETE, mutation.target(), mutation.record());
      case UPDATE -> {
        Optional<Object> prior = store.get(mutation.target(), Object.class);
        yield prior
            .map(p -> Mutation.of(mutation.kind(), Mutation.Op.UPDATE, mutation.target(), p))
            .orElseGet(
                () ->
                    Mutation.of(
                        mutation.kind(), Mutation.Op.DELETE, mutation.target(), mutation.record()));
      }
      case DELETE -> {
        Optional<Object> prior = store.get(mutation.target(), Object.class);
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
        CommitResult result = group.store().commit(group.undo());
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
