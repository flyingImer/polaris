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
 * <p><b>THIS NAME IS TEMPORARY SCAFFOLDING.</b> It exists only so the old and new shapes can
 * coexist while callers migrate. The end state is written down here so nobody — including whoever
 * wrote it — mistakes the scaffold for a fifth concept in the layering:
 *
 * <ol>
 *   <li>Add this interface under a temporary name (where we are now).
 *   <li>Migrate callers off {@link DurablePrimitives} onto it.
 *   <li>Delete {@link DurablePrimitives}, which by then has no callers left.
 *   <li>Rename this interface to {@code DurablePrimitives}.
 * </ol>
 *
 * <p><b>So this is not a new layer, and it is not a new concept.</b> It is the durable-primitives
 * SPI — the third of the four durable layers — in its target shape, wearing a placeholder name for
 * the duration of a migration. `CONTEXT.md`'s naming rules (R2) forbid a {@code -Store} suffix on a
 * seam, and that is exactly why this name cannot be the final one: it is a marker that the rename
 * is still owed, not a name anybody should get used to.
 *
 * <p>The migration itself is <b>not</b> PoC scope. Until it happens, both interfaces exist and
 * every existing caller keeps working — which is the only reason a parallel interface is needed at
 * all. Reshaping {@code DurablePrimitives} in place would break 105 call sites, 79 of them in
 * {@code AtomicOperationMetaStoreManager}, so {@code polaris-core} would not compile and no
 * conformance test could run.
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
 * can evaluate. That is what a declared lookup path is: a {@link LookupPath name} the durable
 * logical data model declares for the kind, anchored by the values the declaration states, pushed
 * down — not a predicate handed over, and not per-kind vocabulary in this interface.
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
   * <p><b>An over-limit list and a domain-spanning list are reported as results, not thrown</b>
   * (decided 2026-08-13): they return {@link CommitResult.Failure#TOO_MANY_ITEMS} and {@link
   * CommitResult.Failure#DOMAIN_MISMATCH}, because a correct caller can run into the cap in
   * ordinary operation and must branch on the distinguishable reason — split the work, or regroup —
   * rather than catch.
   *
   * <p><b>Exceptions are reserved for two cases: a malformed request no retry or reshaping fixes,
   * and infrastructure that stopped the store from reporting an outcome at all.</b> A returned
   * {@link CommitResult} therefore always states a known outcome. Only the second exception case
   * leaves the durable effect in question, and it says what the store knows about that effect, so a
   * caller never has to guess from the absence of a result.
   *
   * @throws IllegalArgumentException if a mutation references an unregistered record kind, or if a
   *     {@link Mutation.Op#DELETE} carries a payload
   * @throws CommitDisruptedException if infrastructure failed before the commit, during it, or
   *     after one that already succeeded, so the store could not report an outcome; {@link
   *     CommitDisruptedException#durableEffect()} states whether the store can prove nothing was
   *     applied or cannot tell
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
   * One record together with the token that lets a commit say it has not changed since, or empty if
   * no record matches the reference.
   *
   * <p>This exists for a record kind carrying no version attribute, which therefore cannot ride its
   * read into a commit through {@link Precondition#versionEquals}. The token is opaque and is
   * verified only by the store that issued it, so it may travel back to this store and no other.
   *
   * <p><b>A second read rather than a widened {@link #get}.</b> Every existing caller reads a
   * record without needing a token, and a store is free to answer this one less cheaply than {@code
   * get} — folding the two would charge every reader for a guarantee most of them never use. A kind
   * that issues no token rejects rather than answering without one.
   */
  @NonNull <T> Optional<Read<T>> read(@NonNull RecordRef ref, @NonNull Class<T> type);

  /**
   * Several records by reference, returned in the order requested.
   *
   * <p>A reference with no matching record yields an empty {@link Optional} in that position rather
   * than being omitted, so a caller can correlate results with requests positionally.
   */
  @NonNull <T> List<Optional<T>> getMany(@NonNull List<RecordRef> refs, @NonNull Class<T> type);

  /**
   * A page of records on one of the kind's declared lookup paths.
   *
   * <p>{@code path} names a lookup path the durable logical data model declares for {@code kind},
   * and {@code anchors} carries the values the path's declared anchor signature states, in order.
   * The declaration is what keeps this implementable by a store that is not a relational database:
   * a path is something every implementation registered for the kind can realize against its own
   * layout, never an open filter. <b>The store evaluates the path.</b> A caller does not receive a
   * wider page and narrow it — that would push the cost onto the wire for a remote store, which is
   * the one thing this interface may not do. A requirement no declared path expresses is a missing
   * declaration or a missing operation, not a caller's job.
   *
   * <p>A {@code (kind, path)} pair the store's registration does not declare, or an anchor list
   * that does not match the declared signature, is rejected rather than guessed at.
   */
  @NonNull <T> Page<T> list(
      @NonNull RecordKind kind,
      @NonNull LookupPath path,
      @NonNull List<Object> anchors,
      @NonNull PageToken pageToken,
      @NonNull Class<T> type);

  /**
   * A page of records of <b>every</b> registered kind declaring {@code path}, evaluated store-side
   * as a union over those kinds' declarations.
   *
   * <p>This is what serves an existence check over a whole subtree level — "does this parent have
   * any children of any kind" — which a caller answers by listing the parent path with a page limit
   * of one and testing for emptiness, rather than by a separate boolean operation. The union is the
   * store's to evaluate: nothing is fetched per kind and merged caller-side.
   *
   * <p>A path no registered kind declares is rejected, the same as an undeclared {@code (kind,
   * path)} pair on the single-kind form.
   */
  @NonNull <T> Page<T> list(
      @NonNull LookupPath path,
      @NonNull List<Object> anchors,
      @NonNull PageToken pageToken,
      @NonNull Class<T> type);

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
