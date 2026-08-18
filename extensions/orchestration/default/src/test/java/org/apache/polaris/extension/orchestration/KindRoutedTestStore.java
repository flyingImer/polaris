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

import java.util.List;
import java.util.Map;
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
 * A test-local stand-in for the routing primitives implementation: resolves each record kind
 * through the given map, forwards {@code domainOf} untouched, refuses a commit whose kinds resolve
 * to two stores, and rejects an unmapped kind naming it — the contract surface the orchestrator
 * consumes, and nothing else.
 *
 * <p>The REAL routing implementation lives in the primitives extensions module, a {@code
 * polaris-server} (Java 21) module this {@code polaris-client} test classpath cannot resolve; it is
 * proven by its own unit test, its Seam-1 conformance binding, and the re-homed Seam-2 runner, and
 * the orchestrator runs over it for real in those. This double exists so the orchestrator's unit
 * tests stay pinned to the SPI contract rather than to one implementation.
 */
final class KindRoutedTestStore implements DurableRecordStore {

  private final Map<RecordKind, DurableRecordStore> storeByKind;

  KindRoutedTestStore(Map<RecordKind, DurableRecordStore> storeByKind) {
    this.storeByKind = Map.copyOf(storeByKind);
  }

  private DurableRecordStore forKind(RecordKind kind) {
    DurableRecordStore store = storeByKind.get(kind);
    if (store == null) {
      throw new IllegalArgumentException(
          "No store holds record kind '" + kind.id() + "' in this mapping");
    }
    return store;
  }

  @Override
  public @NonNull CommitResult commit(@NonNull List<Mutation> mutations) {
    if (mutations.isEmpty()) {
      return CommitResult.applied();
    }
    DurableRecordStore store = forKind(mutations.get(0).target().kind());
    for (Mutation mutation : mutations) {
      if (forKind(mutation.target().kind()) != store) {
        return CommitResult.domainMismatch();
      }
    }
    return store.commit(mutations);
  }

  @Override
  public long generateNewId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NonNull <T> Optional<T> get(@NonNull RecordRef ref, @NonNull Class<T> type) {
    return forKind(ref.kind()).get(ref, type);
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
    return forKind(target.kind()).domainOf(target);
  }

  @Override
  public int maxItemsPerCommit() {
    return 1000;
  }
}
