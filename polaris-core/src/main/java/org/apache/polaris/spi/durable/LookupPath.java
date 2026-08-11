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
 * The name of one of a record kind's declared lookup paths.
 *
 * <p>A kind's lookup paths — the access patterns {@link DurableRecordStore#list} can serve beyond
 * the two addressing tuples — are declared in the durable logical data model document (each kind's
 * "other lookup paths" section, formalized as named paths with anchor signatures) and realized in
 * each implementation's registration, the same declared-once-normative discipline the model already
 * uses for identity and uniqueness keys. A path declaration names the path and states its anchor
 * signature: the ordered values a caller must supply, which the store binds to whatever its own
 * physical layout indexes.
 *
 * <p><b>This package declares no path constants, for the same reason {@link RecordKind} declares no
 * kind constants.</b> The lookup vocabulary belongs to whoever owns the record family: Polaris
 * declares its own paths next to its kinds (see {@code PolarisRecordKinds}), and an extension
 * declaring a private kind declares that kind's paths the same way, with no change to this SPI or
 * to anyone else's backend. A shared kind gains a new shared path only through a data-model change.
 *
 * <p>A store that receives a {@code (kind, path)} pair its registration does not declare rejects
 * the call rather than guessing — an undeclared path is a configuration error, not an open filter
 * to approximate.
 *
 * <p>Equality is exact string equality on the name. The name is scoped by the kind it is declared
 * for; two kinds may each declare a path of the same name (that is what the cross-kind form of
 * {@code list} relies on), and the declarations remain independent.
 */
public final class LookupPath {

  private final String name;

  private LookupPath(String name) {
    this.name = name;
  }

  /**
   * The path with the given declared name.
   *
   * @param name the name as the data model declares it, e.g. {@code "by-parent"}. Must be
   *     non-blank.
   */
  public static @NonNull LookupPath of(@NonNull String name) {
    Objects.requireNonNull(name, "lookup path name");
    if (name.isBlank()) {
      throw new IllegalArgumentException("lookup path name must not be blank");
    }
    return new LookupPath(name);
  }

  /** The declared name. A store uses this, with the kind, as its path-registry key. */
  public @NonNull String name() {
    return name;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    return o instanceof LookupPath other && name.equals(other.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public String toString() {
    return name;
  }
}
