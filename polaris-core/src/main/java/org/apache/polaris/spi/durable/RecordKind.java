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

import java.util.Objects;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * A record kind, supplied by the caller.
 *
 * <p><b>This is deliberately not an enum, and the reason is a hard constraint rather than a
 * preference.</b> Issue 47's S4 requires that record types be supplied by the caller and that
 * adding a record kind <em>must not change the primitives contract</em>. An enum in this package
 * would break that: adding tag-assignment — S4's own worked example — would mean editing a file
 * inside the contract. A first draft of this type was an enum with five constants, and a javadoc
 * that narrowed S4 to "changes no method signature" in order to claim compliance. An enum in an SPI
 * is part of the contract, so that draft violated S4 and the narrowing was the tell.
 *
 * <p>So a kind is an opaque identifier. This package declares no constants for it. Whoever owns a
 * family of records declares their own kinds and registers a mapper for each with the store that
 * holds them; see {@code PolarisRecordKinds} for the kinds Polaris itself defines. Adding a kind
 * touches that declaration and one mapper registration, and nothing here.
 *
 * <p>A store that receives a kind it has no mapper for rejects the call. It does not guess, and it
 * does not silently drop the record — the two-level {@code kind → store name → implementation}
 * mapping is what makes an unknown kind a configuration error rather than a runtime surprise.
 *
 * <p>Identifiers should be namespaced ({@code "polaris.entity"} rather than {@code "entity"}) so
 * that an extension declaring its own kinds cannot collide with Polaris's or with another
 * extension's. Equality is exact string equality on the identifier; there is no normalisation,
 * because a store uses this value as a registry key and silent normalisation would make two
 * callers' kinds indistinguishable in one place and distinct in another.
 */
public final class RecordKind {

  private final String id;

  private RecordKind(String id) {
    this.id = id;
  }

  /**
   * A kind with the given identifier.
   *
   * @param id a namespaced identifier, e.g. {@code "polaris.entity"}. Must be non-blank.
   */
  public static @NonNull RecordKind of(@NonNull String id) {
    Objects.requireNonNull(id, "record kind id");
    if (id.isBlank()) {
      throw new IllegalArgumentException("record kind id must not be blank");
    }
    return new RecordKind(id);
  }

  /** The opaque identifier. A store uses this as its mapper-registry key. */
  public @NonNull String id() {
    return id;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    return o instanceof RecordKind other && id.equals(other.id);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public String toString() {
    return id;
  }
}
