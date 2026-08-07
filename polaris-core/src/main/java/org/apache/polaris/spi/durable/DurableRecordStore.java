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
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.jspecify.annotations.NonNull;

/**
 * The durable-primitives SPI in its target shape: one write operation, four reads, and two
 * declarations.
 *
 * <p><b>This is a PoC interface and deliberately parallel to {@link DurablePrimitives} rather than
 * a replacement for it.</b> Reshaping {@code DurablePrimitives} in place would break <b>105</b>
 * call sites across 6 files at once — 79 of them in {@code AtomicOperationMetaStoreManager} alone —
 * and a mechanical rewrite of that size would bury the design it is meant to demonstrate. (An
 * earlier draft of this sentence said 280; that was a raw grep count including 42 false positives,
 * and it is corrected here rather than left to propagate.) Keeping both lets a new implementation
 * be written and conformance-tested against the target shape while every existing caller keeps
 * working; the migration then becomes its own visible question rather than a precondition for
 * evaluating the shape.
 *
 * <h2>The shape, and where each part comes from</h2>
 *
 * <p><b>One write.</b> {@link #commit} takes a list of {@link Mutation} and applies all of it or
 * none of it. Ten of the shipped interface's write methods fold into this one; the per-kind write
 * methods existed because the interface had no record-kind vocabulary, and {@link RecordKind}
 * supplies one.
 *
 * <p><b>Four reads.</b> {@link #get}, {@link #getMany}, {@link #list}, {@link #versionsOf}.
 * Thirteen shipped reads fold into these four, and the collapse is only possible because {@link
 * RecordRef}'s two addressing modes — already paid for by the write side — unify by-identity and
 * by-uniqueness-key lookups.
 *
 * <p><b>Two declarations.</b> {@link #domainOf} states which atomicity domain a target belongs to,
 * and {@link #maxItemsPerCommit} states a physical limit. Both are declarations rather than probes:
 * a caller never asks whether the store can do something, it is told a fact and the store then
 * behaves accordingly. A probe would let a caller branch on capability, which turns one contract
 * into several.
 *
 * <h2>Deliberately absent</h2>
 *
 * <p><b>No call-context or realm parameter on any method.</b> An instance serves one realm and is
 * never asked which. Established by reading both shipped backends: neither reads the realm from the
 * per-call context parameter it currently declares, and one of them implements its real reads with
 * signatures that do not have the parameter at all.
 *
 * <p><b>No callback parameters, and no caller-side filtering to replace them.</b> The shipped
 * {@code listFullEntities} takes a {@code Predicate} and a {@code Function}. Neither can cross a
 * wire, so neither is available to a remote implementation — and <em>"the caller filters instead"
 * is not the substitute</em>, because for a remote store that means shipping every candidate record
 * across the network and discarding most of them. A filter this SPI accepts must be one the store
 * can evaluate. That is what {@link ListScope} is: a declared scope, pushed down, not a predicate
 * handed over.
 *
 * <p>Of the three real callers of the shipped callback form, two pass {@code entity -> true} and
 * need no filter at all. The third is task leasing, whose predicate parses a JSON blob, reads a
 * realm config and checks a clock — application logic no store can evaluate, local or remote.
 * <b>That one is not a filter to relocate; it is a missing operation</b>, and it is tracked as such
 * rather than served by widening this interface.
 *
 * <p><b>No set-emptiness condition and no arbitrary-field comparison.</b> See {@link Precondition}.
 */
public interface DurableRecordStore {

  // ---------------------------------------------------------------- writes

  /**
   * Apply every mutation, or none of them.
   *
   * <p><b>Atomicity is scoped to one atomicity domain and is unconditional within it.</b> Every
   * mutation in the list, and every record any of their preconditions reference, must belong to the
   * same domain as reported by {@link #domainOf}. Grouping a wider list into per-domain batches is
   * the caller's job, and in the four-layer shape that caller is orchestration.
   *
   * <p>A failed precondition fails the whole commit. It is never a silent no-op and never a partial
   * apply.
   *
   * @throws IllegalArgumentException if the mutations and their condition targets do not all share
   *     one atomicity domain, or if the list exceeds {@link #maxItemsPerCommit}
   */
  @NonNull CommitResult commit(@NonNull List<Mutation> mutations);

  /**
   * Generate an id that is unique within this realm and never reused.
   *
   * <p>Realm-wide, not catalog-wide. The distinction matters because a record's identity and its
   * uniqueness key are different tuples, and a caller building a create needs an id before it has
   * anywhere to put it.
   */
  long generateNewId();

  // ---------------------------------------------------------------- reads

  /** One record, or empty if no record matches the reference. */
  @NonNull <T> Optional<T> get(@NonNull RecordRef ref, @NonNull Class<T> type);

  /**
   * Several records by reference, returned in the order requested.
   *
   * <p>A reference with no matching record yields an empty {@link Optional} in that position rather
   * than being omitted, so a caller can correlate results with requests positionally.
   */
  @NonNull <T> List<Optional<T>> getMany(@NonNull List<RecordRef> refs, @NonNull Class<T> type);

  /**
   * A page of records within a scope.
   *
   * <p>{@link ListScope} is a closed set of shapes rather than an open filter, which is what keeps
   * this implementable by a store that is not a relational database. <b>The store evaluates the
   * scope.</b> A caller does not receive a wider page and narrow it — that would push the cost onto
   * the wire for a remote store, which is the one thing this interface may not do. A requirement
   * the closed set cannot express is a missing scope shape or a missing operation, not a caller's
   * job.
   */
  @NonNull <T> Page<T> list(
      @NonNull ListScope scope, @NonNull PageToken pageToken, @NonNull Class<T> type);

  /**
   * The versions of several records, without the records themselves.
   *
   * <p>Kept as its own operation rather than folded into {@link #getMany} as a projection hint
   * because a store can serve it from a genuinely narrower read — upstream's relational
   * implementation projects four columns instead of sixteen, with a test asserting the wide columns
   * are absent from the SQL. Every known caller compares the version against one it already holds
   * before deciding whether to re-fetch.
   */
  @NonNull List<Optional<RecordVersions>> versionsOf(@NonNull List<RecordRef> refs);

  // ---------------------------------------------------------------- declarations

  /**
   * Which atomicity domain a target belongs to.
   *
   * <p>Two targets whose returned values are {@link Object#equals equal} can be committed
   * atomically together; two whose values differ cannot. The value is opaque to callers — they may
   * compare it and must not interpret it.
   *
   * <p>This is a declaration computed from the target, not a round trip: a relational store backed
   * by one database returns a constant, while a store that partitions returns the partition the
   * target lives in.
   *
   * <p><b>A domain never spans two of these stores.</b> A backend whose real atomic boundary is
   * wider than one store must expose that wider boundary as a single store, because one {@link
   * #commit} reaches exactly one implementation.
   */
  @NonNull Object domainOf(@NonNull RecordRef target);

  /**
   * The most mutations one {@link #commit} accepts.
   *
   * <p>Declared rather than discovered, so a caller can size a request instead of retrying into a
   * limit. A store must reject an over-limit commit rather than silently splitting it: splitting
   * would give up the atomicity the caller was promised, without telling them.
   */
  int maxItemsPerCommit();
}
