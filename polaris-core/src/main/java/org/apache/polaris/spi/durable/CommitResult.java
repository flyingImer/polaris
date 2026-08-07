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
 * The outcome of a {@link DurableRecordStore#commit} call.
 *
 * <p>A commit either applied everything or nothing. When it applied nothing, exactly one thing
 * explains why, and this type names it — rather than reporting a generic failure the caller has to
 * guess at.
 *
 * <p><b>The failure must be distinguishable.</b> A caller has to be able to tell a lost race
 * (retry, after re-reading) from a request that was too large (split it) from a request that was
 * malformed (do not retry). Collapsing those into one error is what forces callers to retry blindly
 * or not at all.
 */
public final class CommitResult {

  /** Why a commit did not apply. */
  public enum Failure {
    /** A precondition was false. The caller should re-read and rebuild before retrying. */
    PRECONDITION_FAILED,
    /**
     * The mutation list exceeded {@link DurableRecordStore#maxItemsPerCommit}. Retrying unchanged
     * will fail again; the caller must split the work, and accept that the parts are not atomic
     * with each other.
     */
    TOO_MANY_ITEMS,
    /**
     * The mutations did not all share one atomicity domain. This is a caller error, not a race:
     * retrying is pointless until the caller groups them.
     */
    DOMAIN_MISMATCH,
  }

  private static final CommitResult APPLIED = new CommitResult(true, null, List.of());

  private final boolean applied;
  private final @Nullable Failure failure;
  private final List<Precondition> failedPreconditions;

  private CommitResult(
      boolean applied, @Nullable Failure failure, List<Precondition> failedPreconditions) {
    this.applied = applied;
    this.failure = failure;
    this.failedPreconditions = List.copyOf(failedPreconditions);
  }

  /** Everything applied. */
  public static @NonNull CommitResult applied() {
    return APPLIED;
  }

  /**
   * Nothing applied because a precondition was false.
   *
   * @param failed the conditions that did not hold; a store may report one or all of them
   */
  public static @NonNull CommitResult preconditionFailed(@NonNull List<Precondition> failed) {
    return new CommitResult(false, Failure.PRECONDITION_FAILED, failed);
  }

  /** Nothing applied because the list was too large. */
  public static @NonNull CommitResult tooManyItems() {
    return new CommitResult(false, Failure.TOO_MANY_ITEMS, List.of());
  }

  /** Nothing applied because the mutations spanned atomicity domains. */
  public static @NonNull CommitResult domainMismatch() {
    return new CommitResult(false, Failure.DOMAIN_MISMATCH, List.of());
  }

  public boolean isApplied() {
    return applied;
  }

  /** Empty when {@link #isApplied()} is true. */
  public @NonNull Optional<Failure> failure() {
    return Optional.ofNullable(failure);
  }

  /**
   * The conditions that did not hold. Non-empty only for {@link Failure#PRECONDITION_FAILED}, and
   * even then a store may report a subset — a store that stops at the first false condition is
   * conformant.
   */
  public @NonNull List<Precondition> failedPreconditions() {
    return failedPreconditions;
  }

  @Override
  public String toString() {
    return applied ? "CommitResult{applied}" : "CommitResult{" + failure + "}";
  }
}
