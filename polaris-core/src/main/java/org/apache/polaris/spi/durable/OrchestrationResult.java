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
package org.apache.polaris.spi.durable;

import java.util.List;
import java.util.Optional;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The outcome of a {@link DurableOrchestrator#commit} call.
 *
 * <p>{@link CommitResult} answers for one atomic commit; this type answers for an orchestrated
 * sequence of them, and it exists because the sequence has one outcome a single commit cannot have:
 * <b>the rollback itself may fail to complete.</b> A caller must be able to tell that state from an
 * ordinary failure, because it is the one case where partial state survives and something — in the
 * PoC, the catalog admin — has to reclaim it. Folding that into {@link CommitResult} would make
 * every single-commit caller carry a state it can never observe.
 *
 * <p>Three outcomes, in decreasing order of how happy the caller should be:
 *
 * <ul>
 *   <li>{@link Outcome#APPLIED} — every group committed. The whole list is in storage.
 *   <li>{@link Outcome#ROLLED_BACK} — a group failed, and every group committed before it was
 *       rolled back in reverse commit order. Nothing from this call remains; a retry after
 *       re-reading simply succeeds. {@link #groupFailure()} says why the failing group failed, with
 *       {@link CommitResult}'s own distinguishable reasons.
 *   <li>{@link Outcome#ROLLBACK_INCOMPLETE} — a group failed and at least one compensating commit
 *       also failed. {@link #uncompensated()} lists the mutations whose effects remain in storage.
 *       Every such residue is inert and reclaimable through normal admin paths; this result is the
 *       disclosure that makes it findable.
 * </ul>
 */
public final class OrchestrationResult {

  /** How the orchestrated commit ended. */
  public enum Outcome {
    /** Every group committed. */
    APPLIED,
    /** A group failed; everything previously committed was rolled back. Nothing remains. */
    ROLLED_BACK,
    /**
     * A group failed and the rollback did not complete. The mutations in {@link #uncompensated()}
     * remain applied and await reclamation.
     */
    ROLLBACK_INCOMPLETE,
  }

  private static final OrchestrationResult APPLIED =
      new OrchestrationResult(Outcome.APPLIED, null, List.of());

  private final Outcome outcome;
  private final @Nullable CommitResult groupFailure;
  private final List<Mutation> uncompensated;

  private OrchestrationResult(
      Outcome outcome, @Nullable CommitResult groupFailure, List<Mutation> uncompensated) {
    this.outcome = outcome;
    this.groupFailure = groupFailure;
    this.uncompensated = List.copyOf(uncompensated);
  }

  /** Every group committed. */
  public static @NonNull OrchestrationResult applied() {
    return APPLIED;
  }

  /**
   * A group failed and every previously committed group was rolled back.
   *
   * @param groupFailure the failing group's own result, saying why it did not apply
   */
  public static @NonNull OrchestrationResult rolledBack(@NonNull CommitResult groupFailure) {
    return new OrchestrationResult(Outcome.ROLLED_BACK, groupFailure, List.of());
  }

  /**
   * A group failed and the rollback did not complete.
   *
   * @param groupFailure the failing group's own result, saying why it did not apply
   * @param uncompensated the mutations whose effects remain in storage
   */
  public static @NonNull OrchestrationResult rollbackIncomplete(
      @NonNull CommitResult groupFailure, @NonNull List<Mutation> uncompensated) {
    return new OrchestrationResult(Outcome.ROLLBACK_INCOMPLETE, groupFailure, uncompensated);
  }

  public @NonNull Outcome outcome() {
    return outcome;
  }

  /** True only for {@link Outcome#APPLIED}. */
  public boolean isApplied() {
    return outcome == Outcome.APPLIED;
  }

  /**
   * Why the failing group did not apply, with {@link CommitResult}'s distinguishable reasons. Empty
   * only for {@link Outcome#APPLIED}.
   */
  public @NonNull Optional<CommitResult> groupFailure() {
    return Optional.ofNullable(groupFailure);
  }

  /**
   * The mutations whose effects remain in storage because their compensation failed. Non-empty only
   * for {@link Outcome#ROLLBACK_INCOMPLETE}.
   */
  public @NonNull List<Mutation> uncompensated() {
    return uncompensated;
  }

  @Override
  public String toString() {
    return switch (outcome) {
      case APPLIED -> "OrchestrationResult{applied}";
      case ROLLED_BACK -> "OrchestrationResult{ROLLED_BACK " + groupFailure + "}";
      case ROLLBACK_INCOMPLETE ->
          "OrchestrationResult{ROLLBACK_INCOMPLETE "
              + groupFailure
              + " uncompensated="
              + uncompensated.size()
              + "}";
    };
  }
}
