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
package org.apache.polaris.spi.feature.catalog;

import java.util.Objects;
import java.util.Optional;

/**
 * The {@link ExtensionPayload} for feature-SPIs (like the Iceberg catalog) whose operations carry a
 * per-call OSS ETag and nothing else. Composes {@link ExtensionPayload} (to satisfy the {@code E
 * extends ExtensionPayload} bound on {@link PolarisResult}) and {@link ETagCarrier} (to be
 * generically etag-readable by the runtime adapter) as two explicit, independent facets -- see
 * {@link ETagCarrier} for why they are not related by inheritance.
 *
 * <p>The OSS default Iceberg catalog implementation binds {@code E = ETagPayload}: the etag-bearing
 * operations construct a fresh instance per call with their computed ETag; every other operation
 * uses the shared {@link #NONE}. A provider (e.g. a federated catalog) that only needs to carry the
 * upstream ETag can reuse this type directly rather than defining its own.
 */
public record ETagPayload(Optional<String> etag) implements ExtensionPayload, ETagCarrier {
  public ETagPayload {
    Objects.requireNonNull(etag, "etag");
  }

  /** The shared instance for operations that carry no ETag. */
  public static final ETagPayload NONE = new ETagPayload(Optional.empty());
}
