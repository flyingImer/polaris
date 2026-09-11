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

import java.util.Optional;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * A condition a store tests before applying a mutation, refusing the whole commit if it is false.
 *
 * <p>Preconditions exist because a caller reads before it writes and the data can change in
 * between. A condition states what the caller assumed when it read. There is no interactive
 * transaction: reads happen before the commit and ride into it as conditions.
 *
 * <h2>What this type settles, and how it deviates from what was expected</h2>
 *
 * <p>{@code docs/adr/0011-precondition-vocabulary.md} deliberately left the concrete shape open,
 * naming one specific risk: that a generalized {@code (target, attribute, comparison, value)} form
 * would let the <em>attribute</em> dimension degenerate into an open field-name string, which is
 * the expression grammar the read-side analysis already rejected. Writing the type settles two
 * things the ADR could not:
 *
 * <p><b>1. "On another record" is not a separate family of forms.</b> Because a condition carries
 * its own {@link RecordRef}, a condition on the record being written and a condition on some other
 * record are the same form with a different reference. Whether the reference happens to equal the
 * mutation's target is not the contract's business. That collapses what the design discussion
 * called a six-form vocabulary into <b>three operators plus "no condition"</b>. This is a
 * simplification relative to the prose, recorded here rather than applied silently.
 *
 * <p><b>2. An attribute dimension does exist, and it is a closed enum rather than a string.</b> The
 * hope that naming the operator would remove the attribute dimension entirely does not survive
 * contact with the data model: an entity carries both a record version and a separate grant-records
 * version, and real conditions need each. So {@link VersionAttribute} exists. It has two constants,
 * adding a third is a deliberate contract change, and no caller can name an attribute the contract
 * has not declared. <b>The degeneration the ADR feared is avoided, but not in the way it hoped</b>
 * — by closing the dimension, not by lacking one.
 *
 * <h2>Deliberately absent</h2>
 *
 * <p>There is no set-emptiness operator ("no children under this parent"). None of etcd, DynamoDB,
 * Cassandra, Kubernetes, FoundationDB or JPA offers one natively, so including it would oblige
 * every backend to scan. Requirements that look like they need it are met by a witness on an owning
 * record, by write ordering, or inside one implementation. There is also no comparison against an
 * arbitrary field: only one surveyed system offers it and nothing in the operation census needs it.
 */
public final class Precondition {

  /** The closed set of attributes a condition may test. */
  public enum VersionAttribute {
    /** The record's own version, bumped by any write to the record. */
    RECORD_VERSION,
    /**
     * An entity's grant-records version, bumped when a grant on or to that entity changes. Separate
     * from {@link #RECORD_VERSION} because a grant write must be able to invalidate a cached grant
     * set without claiming the entity itself changed.
     */
    GRANT_RECORDS_VERSION,
  }

  /** The closed set of condition operators. */
  public enum Op {
    /** No condition. The mutation applies unconditionally. */
    NONE,
    /** The referenced record must not exist. */
    NOT_EXISTS,
    /** The referenced record must exist. */
    EXISTS,
    /**
     * The referenced record must exist and the named version attribute must equal the given value.
     */
    VERSION_EQUALS,
    /**
     * The referenced record must exist and must still be in the state a {@link ReadToken} was
     * issued for.
     *
     * <p>This is what a record kind carrying no version attribute uses in place of {@link
     * #VERSION_EQUALS}. It is not a comparison against a field the caller names: the contract knows
     * only that the token came from a read of the same reference and that the issuing store can
     * verify it, never how the store decides two states are the same.
     */
    UNCHANGED_SINCE,
  }

  private static final Precondition NONE = new Precondition(Op.NONE, null, null, 0);

  private final Op op;
  private final @Nullable RecordRef ref;
  private final @Nullable VersionAttribute attribute;
  private final long expectedVersion;
  private final @Nullable ReadToken token;

  private Precondition(
      Op op, @Nullable RecordRef ref, @Nullable VersionAttribute attribute, long expectedVersion) {
    this(op, ref, attribute, expectedVersion, null);
  }

  private Precondition(
      Op op,
      @Nullable RecordRef ref,
      @Nullable VersionAttribute attribute,
      long expectedVersion,
      @Nullable ReadToken token) {
    this.op = op;
    this.ref = ref;
    this.attribute = attribute;
    this.expectedVersion = expectedVersion;
    this.token = token;
  }

  /**
   * No condition; the mutation applies unconditionally.
   *
   * <p>This is the correct choice for a record whose every field is part of its own key —
   * re-asserting it is naturally idempotent, so a must-not-exist check would turn a harmless repeat
   * into an error.
   */
  public static @NonNull Precondition none() {
    return NONE;
  }

  /** The referenced record must not exist. */
  public static @NonNull Precondition notExists(@NonNull RecordRef ref) {
    return new Precondition(Op.NOT_EXISTS, ref, null, 0);
  }

  /** The referenced record must exist. */
  public static @NonNull Precondition exists(@NonNull RecordRef ref) {
    return new Precondition(Op.EXISTS, ref, null, 0);
  }

  /**
   * The referenced record must exist and the named version must equal {@code expectedVersion}.
   *
   * <p>The expected value must be the one the caller actually read. A stale expectation fails the
   * whole commit; it must never be silently applied against current state.
   */
  public static @NonNull Precondition versionEquals(
      @NonNull RecordRef ref, @NonNull VersionAttribute attribute, long expectedVersion) {
    return new Precondition(Op.VERSION_EQUALS, ref, attribute, expectedVersion);
  }

  /**
   * The referenced record must exist and must still be in the state {@code token} was issued for.
   *
   * <p>For a record kind that carries no version attribute this is the only way a read rides into a
   * commit. The token must be one that a read of {@code ref} returned from the same store the
   * commit goes to: a token is opaque to every caller and is verified only by the store that issued
   * it.
   *
   * <p>The reference must be the mutation's own target. This form on some other record is invalid
   * input rather than a wider condition, because a token's meaning is bound to the read that
   * produced it and no other store can check it.
   */
  public static @NonNull Precondition unchangedSince(
      @NonNull RecordRef ref, @NonNull ReadToken token) {
    return new Precondition(Op.UNCHANGED_SINCE, ref, null, 0, token);
  }

  public @NonNull Op op() {
    return op;
  }

  /** The record this condition tests. Empty only when {@link #op()} is {@link Op#NONE}. */
  public @NonNull Optional<RecordRef> ref() {
    return Optional.ofNullable(ref);
  }

  /** Present only when {@link #op()} is {@link Op#VERSION_EQUALS}. */
  public @NonNull Optional<VersionAttribute> attribute() {
    return Optional.ofNullable(attribute);
  }

  /** Meaningful only when {@link #op()} is {@link Op#VERSION_EQUALS}. */
  public long expectedVersion() {
    return expectedVersion;
  }

  /** Present only when {@link #op()} is {@link Op#UNCHANGED_SINCE}. */
  public @NonNull Optional<ReadToken> token() {
    return Optional.ofNullable(token);
  }

  @Override
  public String toString() {
    return switch (op) {
      case NONE -> "Precondition{NONE}";
      case VERSION_EQUALS ->
          "Precondition{" + op + " " + ref + " " + attribute + "=" + expectedVersion + "}";
      // The token is deliberately left out. This rendering reaches a conflict message a client can
      // see, and for some record kinds a token's parts are secret material.
      case UNCHANGED_SINCE -> "Precondition{" + op + " " + ref + "}";
      default -> "Precondition{" + op + " " + ref + "}";
    };
  }
}
