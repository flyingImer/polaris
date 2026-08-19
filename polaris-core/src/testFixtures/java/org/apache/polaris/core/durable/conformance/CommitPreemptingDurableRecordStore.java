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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.LookupPath;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.RecordKind;
import org.apache.polaris.spi.durable.RecordRef;
import org.apache.polaris.spi.durable.RecordVersions;
import org.jspecify.annotations.NonNull;

/**
 * A {@link DurableRecordStore} that deterministically injects a competing writer: on the FIRST
 * {@code commit} call it derives a competing mutation list from the intercepted one, commits that
 * to the real store, and only then forwards the intercepted commit — so the competitor's rows land
 * strictly between the caller's decision to write and the write's precondition evaluation.
 *
 * <p>That interleaving is exactly the check-then-commit race window, reproduced without threads:
 * real threads cannot be relied on to interleave into the window, and two same-key mutations inside
 * ONE commit roll each other back rather than leaving a winner. Deriving the competitor from the
 * intercepted mutations (rather than taking a pre-built list) means the caller never has to predict
 * ids the racing code reserves at runtime.
 *
 * <p>In the style of {@link CommitRecordingDurableRecordStore}: one intercepted method, everything
 * else forwarded 1:1.
 */
public final class CommitPreemptingDurableRecordStore implements DurableRecordStore {

  private final DurableRecordStore delegate;
  private final Function<List<Mutation>, List<Mutation>> competingMutations;
  private final AtomicBoolean preempted = new AtomicBoolean();

  /**
   * @param competingMutations derives the competing writer's mutation list from the first
   *     intercepted commit's mutations; its result is committed to the delegate first and must
   *     apply
   */
  public CommitPreemptingDurableRecordStore(
      DurableRecordStore delegate, Function<List<Mutation>, List<Mutation>> competingMutations) {
    this.delegate = delegate;
    this.competingMutations = competingMutations;
  }

  @Override
  public @NonNull CommitResult commit(@NonNull List<Mutation> mutations) {
    if (preempted.compareAndSet(false, true)) {
      CommitResult planted = delegate.commit(competingMutations.apply(List.copyOf(mutations)));
      if (!planted.isApplied()) {
        throw new IllegalStateException(
            "the competing writer's commit must apply for the race to exist; got " + planted);
      }
    }
    return delegate.commit(mutations);
  }

  @Override
  public long generateNewId() {
    return delegate.generateNewId();
  }

  @Override
  public @NonNull <T> Optional<T> get(@NonNull RecordRef ref, @NonNull Class<T> type) {
    return delegate.get(ref, type);
  }

  @Override
  public @NonNull <T> List<Optional<T>> getMany(
      @NonNull List<RecordRef> refs, @NonNull Class<T> type) {
    return delegate.getMany(refs, type);
  }

  @Override
  public @NonNull <T> Page<T> list(
      @NonNull RecordKind kind,
      @NonNull LookupPath path,
      @NonNull List<Object> anchors,
      @NonNull PageToken pageToken,
      @NonNull Class<T> type) {
    return delegate.list(kind, path, anchors, pageToken, type);
  }

  @Override
  public @NonNull <T> Page<T> list(
      @NonNull LookupPath path,
      @NonNull List<Object> anchors,
      @NonNull PageToken pageToken,
      @NonNull Class<T> type) {
    return delegate.list(path, anchors, pageToken, type);
  }

  @Override
  public @NonNull List<Optional<RecordVersions>> versionsOf(@NonNull List<RecordRef> refs) {
    return delegate.versionsOf(refs);
  }

  @Override
  public @NonNull Object domainOf(@NonNull RecordRef target) {
    return delegate.domainOf(target);
  }

  @Override
  public int maxItemsPerCommit() {
    return delegate.maxItemsPerCommit();
  }
}
