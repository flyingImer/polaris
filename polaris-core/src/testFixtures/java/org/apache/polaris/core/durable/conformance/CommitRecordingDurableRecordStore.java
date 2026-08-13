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
 * A {@link DurableRecordStore} that records every {@code commit} call into a shared log and
 * forwards everything to a real store.
 *
 * <p>This is how the orchestration conformance cases observe grouping and rollback order at the
 * seam: the orchestrator's contract states how many commits reach which store in what order, and
 * this wrapper makes exactly that observable — without asserting anything about any store's or
 * orchestrator's internals. Several wrappers share one log, so cross-store ordering is a single
 * sequence.
 */
public final class CommitRecordingDurableRecordStore implements DurableRecordStore {

  /** One recorded {@link DurableRecordStore#commit} call, in cross-store arrival order. */
  public record RecordedCommit(String store, List<Mutation> mutations, CommitResult result) {}

  private final String label;
  private final DurableRecordStore delegate;
  private final List<RecordedCommit> log;

  /**
   * @param label names this store in the shared log's entries
   * @param log the shared, caller-owned log; wrappers append, callers assert
   */
  public CommitRecordingDurableRecordStore(
      String label, DurableRecordStore delegate, List<RecordedCommit> log) {
    this.label = label;
    this.delegate = delegate;
    this.log = log;
  }

  @Override
  public @NonNull CommitResult commit(@NonNull List<Mutation> mutations) {
    CommitResult result = delegate.commit(mutations);
    log.add(new RecordedCommit(label, List.copyOf(mutations), result));
    return result;
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
