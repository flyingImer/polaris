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
package org.apache.polaris.core.durable.conformance;

import java.util.List;
import java.util.function.Function;
import java.util.function.IntFunction;
import org.apache.polaris.spi.durable.LookupPath;
import org.apache.polaris.spi.durable.RecordKind;
import org.apache.polaris.spi.durable.RecordRef;
import org.jspecify.annotations.Nullable;

/**
 * One declared {@code (kind, path)} pair, carried as data so the suite's path cases are generated
 * rather than written per kind: the case code iterates entries like this one and never names a
 * concrete kind or path.
 *
 * <p>The anchor signature here restates the durable logical data model's declaration for the pair
 * (the same declaration each implementation realizes in its registration); the minting functions
 * supply the kind-specific payloads the generic cases need. A provider with a private kind adds an
 * entry per declared path and gets the same coverage with no suite change.
 *
 * @param kind the record kind
 * @param path one of the kind's declared lookup paths
 * @param anchorTypes the declared required anchors' types, in order
 * @param trailingType the declared optional trailing anchor's type, or null when the path declares
 *     none
 * @param anchors mints the anchor tuple for a seed; distinct seeds give tuples no record of one
 *     ever matches under the other
 * @param trailingValue a legal trailing anchor value, or null when the path declares no trailing
 *     anchor
 * @param mint mints a record that the path resolves under the given anchors; ordinals differentiate
 *     records under one anchor tuple, and the trailing value (when non-null) mints a record the
 *     trailing anchor selects
 * @param identityRef the minted record's identity reference, for creating it through {@code commit}
 * @param ancestorMint mints a record the path serves through its declared ancestor direction — a
 *     record whose own scope value is a slash-terminated ancestor segment of the queried anchor,
 *     shallower than it — or null when the path declares no such direction. Modeled like {@code
 *     trailingType}: a capability a declaration opts into, generating its case only where declared.
 */
public record PathCase(
    RecordKind kind,
    LookupPath path,
    List<Class<?>> anchorTypes,
    @Nullable Class<?> trailingType,
    IntFunction<List<Object>> anchors,
    @Nullable Object trailingValue,
    Minter mint,
    Function<Object, RecordRef> identityRef,
    @Nullable Minter ancestorMint) {

  /** The common form: a path declaring no ancestor direction. */
  public PathCase(
      RecordKind kind,
      LookupPath path,
      List<Class<?>> anchorTypes,
      @Nullable Class<?> trailingType,
      IntFunction<List<Object>> anchors,
      @Nullable Object trailingValue,
      Minter mint,
      Function<Object, RecordRef> identityRef) {
    this(kind, path, anchorTypes, trailingType, anchors, trailingValue, mint, identityRef, null);
  }

  /** Mints one record resolvable under the given anchor tuple. */
  @FunctionalInterface
  public interface Minter {
    Object mint(List<Object> anchors, int ordinal, @Nullable Object trailingValue);
  }
}
