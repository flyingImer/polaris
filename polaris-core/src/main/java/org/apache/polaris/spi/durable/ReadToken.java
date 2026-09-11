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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * What a record looked like when a store read it, carried back into a commit by {@link
 * Precondition#unchangedSince}.
 *
 * <p>Opaque in the same sense as {@link DurableRecordStore#domainOf}'s value: a caller may hold a
 * token and hand it back, and two tokens may be compared for equality, but nothing above the
 * primitives layer interprets what is inside. Only the store that issued a token verifies it, so
 * the contents are that store's business — a relational store carries the row's value columns, an
 * in-memory store the stored record's fields, and a backend with a native record version carries
 * that version. That is also why the contract holds no content hash: a store that already has a
 * cheaper answer is free to use it.
 *
 * <p><b>A token never crosses stores</b>, never appears in a manager signature, and never becomes a
 * wire or model field. There is no canonical serialization, two stores' tokens are not comparable,
 * and no caller builds one.
 *
 * <p><b>{@link #toString()} deliberately reveals nothing.</b> A refused condition is rendered into
 * a conflict message a client can see, and for some record kinds these parts are secret material,
 * so the parts are counted rather than printed.
 */
public final class ReadToken {

  private final List<@Nullable Object> parts;

  private ReadToken(List<@Nullable Object> parts) {
    this.parts = parts;
  }

  /**
   * A token over the values the issuing store treats as the record's state, in a fixed order of
   * that store's choosing.
   *
   * <p>Null elements are allowed and are significant: a nullable column that WAS null is part of
   * the state the reader saw, so it has to compare equal only to null later.
   */
  public static @NonNull ReadToken of(@NonNull List<@Nullable Object> parts) {
    return new ReadToken(Collections.unmodifiableList(new ArrayList<>(parts)));
  }

  /**
   * The values this token was built from, in the order the issuing store chose, for that store to
   * verify against. Meaningful to no one else.
   */
  public @NonNull List<@Nullable Object> parts() {
    return parts;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof ReadToken token && parts.equals(token.parts);
  }

  @Override
  public int hashCode() {
    return parts.hashCode();
  }

  /** Counts the parts and never prints them; see the class javadoc. */
  @Override
  public String toString() {
    return "ReadToken{" + parts.size() + " parts}";
  }
}
