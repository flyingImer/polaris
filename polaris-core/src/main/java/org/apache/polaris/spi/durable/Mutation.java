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
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * One record-level change, with the conditions that gate it.
 *
 * <p>A list of these is the entire write surface of {@code DurablePrimitives}. Everything the
 * shipped interface expresses as a separate write method — write an entity, write a batch of
 * entities, write a grant record, write events, delete an entity, delete a grant record — becomes a
 * mutation in a list, and one commit applies the list or none of it.
 *
 * <h2>Why the conditions are a list rather than one</h2>
 *
 * <p>A single mutation can need several conditions at once. Rename is the worked case: the source
 * must still be at the version the caller read, <em>and</em> the destination name must be free,
 * <em>and</em> the destination parent must still exist. Those are three conditions on three
 * different records for one write.
 *
 * <p>This is also why a mutation's atomicity domain is the union of its write target and all of its
 * condition targets, not just the target. A condition that cannot be evaluated atomically with the
 * write it gates is not a condition, it is a hope.
 *
 * <h2>What is deliberately not here</h2>
 *
 * <p>No call-context or realm parameter. A {@code DurablePrimitives} instance serves exactly one
 * realm and is never asked which; verified by reading both shipped backends, neither of which reads
 * the realm from the per-call context they currently declare.
 *
 * <p>No ordering guarantee between mutations in one commit, and none is needed within a domain:
 * they either all apply or none do, so their relative order is unobservable. Ordering matters only
 * <em>across</em> domains, where it is orchestration's concern rather than a mutation's.
 */
public final class Mutation {

  /** What a mutation does to its target. */
  public enum Op {
    /**
     * Create the record. Whether a colliding create is an error or a no-op is decided by the
     * accompanying precondition, not by this operator: a kind whose every field is part of its own
     * key uses {@link Precondition#none()} and is naturally idempotent, while a kind with a
     * uniqueness key separate from its identity uses {@link Precondition#notExists} and a collision
     * is an error.
     */
    CREATE,
    /** Replace the record's mutable state. Normally paired with a version condition. */
    UPDATE,
    /**
     * Remove the record. Carries conditions like any other mutation. Addressed by the target
     * reference alone: a DELETE carries no payload, and any condition it needs is stated explicitly
     * through {@link Precondition} — a payload can carry no meaning a declared condition cannot, so
     * permitting one would open a second, implicit condition channel.
     */
    DELETE,
  }

  private final RecordKind kind;
  private final Op op;
  private final RecordRef target;
  private final @Nullable Object record;
  private final List<Precondition> preconditions;

  private Mutation(
      RecordKind kind,
      Op op,
      RecordRef target,
      @Nullable Object record,
      List<Precondition> preconditions) {
    this.kind = kind;
    this.op = op;
    this.target = target;
    this.record = record;
    this.preconditions = List.copyOf(preconditions);
  }

  /**
   * A mutation with no conditions.
   *
   * @param kind the record kind being written
   * @param op what to do
   * @param target which record
   * @param record the record's new state; must be null for {@link Op#DELETE}, which is addressed by
   *     {@code target} alone
   */
  public static @NonNull Mutation of(
      @NonNull RecordKind kind,
      @NonNull Op op,
      @NonNull RecordRef target,
      @Nullable Object record) {
    return new Mutation(kind, op, target, record, List.of());
  }

  /**
   * A mutation gated by one or more conditions.
   *
   * @param preconditions every condition that must hold; an empty list means unconditional
   */
  public static @NonNull Mutation of(
      @NonNull RecordKind kind,
      @NonNull Op op,
      @NonNull RecordRef target,
      @Nullable Object record,
      @NonNull List<Precondition> preconditions) {
    return new Mutation(kind, op, target, record, preconditions);
  }

  public @NonNull RecordKind kind() {
    return kind;
  }

  public @NonNull Op op() {
    return op;
  }

  /** The record this mutation writes. */
  public @NonNull RecordRef target() {
    return target;
  }

  /**
   * The record's new state; null for a delete, which carries no payload.
   *
   * <p><b>A DELETE is addressed by its {@link #target() target reference} alone, and a store
   * rejects a DELETE carrying a payload</b> (decided 2026-08-13): every meaning a payload could
   * carry on a delete — "only if still at this version", "only if it exists" — is already
   * expressible through the explicit {@link Precondition} vocabulary, so a payload there would be a
   * second, implicit condition channel, which is the thing the declared vocabulary exists to rule
   * out.
   *
   * <p>Typed as {@code Object} because the write surface is record-kind agnostic: the payload's
   * type is a property of {@link #kind()}, and an implementation resolves it through the same
   * per-kind mapper registration that tells it where the kind is stored. Making this generic would
   * push the kind into every signature that carries a mutation, which is the coupling the
   * single-commit shape exists to avoid.
   */
  public @Nullable Object record() {
    return record;
  }

  /** Every condition that must hold for this mutation to apply. Empty means unconditional. */
  public @NonNull List<Precondition> preconditions() {
    return preconditions;
  }

  @Override
  public String toString() {
    return "Mutation{"
        + op
        + " "
        + kind
        + " "
        + target
        + " conditions="
        + preconditions.size()
        + "}";
  }
}
