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
 * <p><b>Closed on purpose, and the store evaluates it.</b> An open filter — a predicate, an
 * expression tree, a field-name map — cannot cross a wire, so it is unavailable to a remote store.
 * The tempting substitute, handing the caller a wider page to narrow itself, is worse rather than
 * equivalent: for a remote store it ships every candidate record across the network to discard most
 * of them. So a scope is something the store can push down, and every shape below names something a
 * store must be able to index.
 *
 * <p>Whether the shipped relational implementation happens to evaluate its caller-supplied
 * predicate in Java today is beside the point and is deliberately not cited as justification.
 * Current behaviour is a requirement to serve, never a precedent for a contract.
 *
 * <p>Three shapes cover every list the shipped interface performs.
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

  private final @Nullable RecordKind kind;
  private final Shape shape;
  private final @Nullable RecordRef anchor;
  private final @Nullable Integer subtype;
  private final @Nullable String locationPrefix;

  private ListScope(
      @Nullable RecordKind kind,
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

  /**
   * Records of <b>any</b> kind directly under {@code parent}.
   *
   * <p>Serves an existence check over a whole subtree level — "does this parent have any children
   * at all" — which a caller answers by listing with a page limit of one and testing for emptiness
   * rather than by a separate boolean operation.
   */
  public static @NonNull ListScope anyKindUnder(@NonNull RecordRef parent) {
    return new ListScope(null, Shape.CHILDREN_OF_PARENT, parent, null, null);
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

  /**
   * The record kind to list, or empty for <b>every</b> kind in the scope.
   *
   * <p>Absent kind is what serves the shipped {@code hasChildren}, whose real caller passes null
   * for its optional entity type meaning "children of any type". That could not be expressed while
   * kind was a closed enum, because the type vocabulary has no "any" constant — only a "no type"
   * one, which means root rather than any. Once kind became a caller-supplied value, "any" is
   * simply its absence.
   */
  public @NonNull Optional<RecordKind> kind() {
    return Optional.ofNullable(kind);
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
    return "ListScope{" + shape + " " + (kind == null ? "*" : kind) + " @" + anchor + "}";
  }
}
