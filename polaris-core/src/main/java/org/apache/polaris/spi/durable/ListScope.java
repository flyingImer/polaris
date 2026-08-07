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
 * The closed set of scopes a {@link DurableRecordStore#list} call may ask for.
 *
 * <p><b>Closed on purpose.</b> An open filter — a predicate, an expression tree, a field-name map —
 * would be unimplementable by a store that is not a relational database, and would be pushed down
 * by no store at all: the shipped relational implementation applies its caller-supplied predicate
 * in Java after fetching the rows, so the open form never bought a pushdown even where it looked
 * like it should.
 *
 * <p>Three shapes cover every list the shipped interface performs. They are distinguished by what a
 * store must be able to index, not by what a caller finds convenient to ask.
 */
public final class ListScope {

  /** Which of the three shapes a scope uses. */
  public enum Shape {
    /** Every record of a kind directly under one parent, optionally narrowed to one subtype. */
    CHILDREN_OF_PARENT,
    /** Every record of a kind referencing one other record — a foreign-key equality. */
    REFERENCING,
    /** Every record of a kind under one parent whose location falls under a prefix. */
    UNDER_LOCATION_PREFIX,
  }

  private final RecordKind kind;
  private final Shape shape;
  private final @Nullable RecordRef anchor;
  private final @Nullable Integer subtype;
  private final @Nullable String locationPrefix;

  private ListScope(
      RecordKind kind,
      Shape shape,
      @Nullable RecordRef anchor,
      @Nullable Integer subtype,
      @Nullable String locationPrefix) {
    this.kind = kind;
    this.shape = shape;
    this.anchor = anchor;
    this.subtype = subtype;
    this.locationPrefix = locationPrefix;
  }

  /** Records of {@code kind} directly under {@code parent}. */
  public static @NonNull ListScope childrenOf(@NonNull RecordKind kind, @NonNull RecordRef parent) {
    return new ListScope(kind, Shape.CHILDREN_OF_PARENT, parent, null, null);
  }

  /** Records of {@code kind} directly under {@code parent}, narrowed to one subtype. */
  public static @NonNull ListScope childrenOf(
      @NonNull RecordKind kind, @NonNull RecordRef parent, int subtype) {
    return new ListScope(kind, Shape.CHILDREN_OF_PARENT, parent, subtype, null);
  }

  /**
   * Records of {@code kind} that reference {@code referenced}.
   *
   * <p>This is the shape both grant-record directions use — grants on a securable and grants to a
   * grantee are the same scope with a different anchor, which is why the shipped interface's two
   * methods collapse into one.
   */
  public static @NonNull ListScope referencing(
      @NonNull RecordKind kind, @NonNull RecordRef referenced) {
    return new ListScope(kind, Shape.REFERENCING, referenced, null, null);
  }

  /**
   * Records of {@code kind} under {@code parent} whose storage location starts with {@code prefix}.
   *
   * <p>Present because the overlapping-location check needs it and cannot be served by the other
   * two shapes. A store without prefix indexing must serve it some other way; it may not refuse it,
   * because the check is a correctness requirement rather than an optimisation.
   */
  public static @NonNull ListScope underLocationPrefix(
      @NonNull RecordKind kind, @NonNull RecordRef parent, @NonNull String prefix) {
    return new ListScope(kind, Shape.UNDER_LOCATION_PREFIX, parent, null, prefix);
  }

  public @NonNull RecordKind kind() {
    return kind;
  }

  public @NonNull Shape shape() {
    return shape;
  }

  /** The parent or referenced record this scope hangs off. */
  public @NonNull RecordRef anchor() {
    return java.util.Objects.requireNonNull(anchor, "every shape has an anchor");
  }

  /** Present only for {@link Shape#CHILDREN_OF_PARENT} narrowed to a subtype. */
  public @NonNull Optional<Integer> subtype() {
    return Optional.ofNullable(subtype);
  }

  /** Present only for {@link Shape#UNDER_LOCATION_PREFIX}. */
  public @NonNull Optional<String> locationPrefix() {
    return Optional.ofNullable(locationPrefix);
  }

  @Override
  public String toString() {
    return "ListScope{" + shape + " " + kind + " @" + anchor + "}";
  }
}
