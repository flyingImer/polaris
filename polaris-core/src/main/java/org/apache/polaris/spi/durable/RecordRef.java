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
 * A reference to one durable record, by identity or by uniqueness key.
 *
 * <p><b>Both addressing modes are required, and each is required by something the other cannot
 * serve.</b> The by-identity mode is what an update or delete of a known record uses. The
 * by-uniqueness-key mode is what a create's must-not-exist check needs — at create time the
 * record's identity is a freshly generated id, so a must-not-exist condition on identity is
 * trivially true and therefore useless. Rename needs the uniqueness mode independently, to check
 * that the destination name is free.
 *
 * <p>For {@link RecordKind#ENTITY} the two modes are genuinely different tuples: identity is {@code
 * (realm, id)} while uniqueness is {@code (realm, parent, type, name)}. For {@link
 * RecordKind#GRANT_RECORD} they are the same tuple, because every field of a grant record is part
 * of its own key. Callers should not assume the modes are interchangeable for a given kind.
 *
 * <p><b>Realm is deliberately absent from every tuple below.</b> A {@code DurablePrimitives}
 * instance is constructed for one realm and never asked which realm it serves, so carrying realm in
 * a reference would be dead weight — a conclusion reached by checking both shipped backends rather
 * than by preference: neither reads the realm from the per-call context parameter it currently
 * declares.
 */
public final class RecordRef {

  /** Which of the two addressing modes a reference uses. */
  public enum Mode {
    /** Addressed by the record's own identity. */
    IDENTITY,
    /** Addressed by the record kind's declared uniqueness key. */
    UNIQUENESS_KEY,
  }

  private final RecordKind kind;
  private final Mode mode;
  private final List<Object> key;

  private RecordRef(RecordKind kind, Mode mode, List<Object> key) {
    this.kind = kind;
    this.mode = mode;
    this.key = List.copyOf(key);
  }

  /**
   * A reference to a record by its own identity.
   *
   * @param kind the record kind
   * @param key the identity tuple's components, in the order the kind's identity declares them
   */
  public static @NonNull RecordRef byIdentity(@NonNull RecordKind kind, @NonNull List<Object> key) {
    return new RecordRef(kind, Mode.IDENTITY, key);
  }

  /**
   * A reference to a record by its kind's declared uniqueness key.
   *
   * @param kind the record kind
   * @param key the uniqueness tuple's components, in the order the kind's uniqueness rule declares
   *     them
   */
  public static @NonNull RecordRef byUniquenessKey(
      @NonNull RecordKind kind, @NonNull List<Object> key) {
    return new RecordRef(kind, Mode.UNIQUENESS_KEY, key);
  }

  public @NonNull RecordKind kind() {
    return kind;
  }

  public @NonNull Mode mode() {
    return mode;
  }

  /**
   * The key's components, in the order the addressed tuple declares them.
   *
   * <p>This is deliberately a positional tuple rather than a map of field names. A field-name map
   * would reintroduce the open field-name string that the read-side analysis rejected: it would let
   * a caller address by any attribute an implementation happens to store, which is an expression
   * grammar rather than a reference.
   */
  public @NonNull List<Object> key() {
    return key;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RecordRef other)) {
      return false;
    }
    return kind == other.kind && mode == other.mode && key.equals(other.key);
  }

  @Override
  public int hashCode() {
    int result = kind.hashCode();
    result = 31 * result + mode.hashCode();
    result = 31 * result + key.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "RecordRef{" + kind + " " + mode + " " + key + "}";
  }
}
