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
 * The outcome of a conditional-load catalog feature-SPI operation (Iceberg REST's If-None-Match
 * {@code loadTable}): either the body was loaded ({@link Loaded}) or the caller's cached copy is
 * still current ({@link NotModified}). Both cases carry the OSS-carried ETag -- including
 * not-modified, since a 304 response still carries an ETag.
 *
 * <p>The not-modified DECISION is made by the feature-SPI implementation (a local impl compares
 * If-None-Match to its derived ETag; a federated impl delegates to the remote catalog), never by
 * the runtime adapter (ADR-0003 amendment refinement 2). The runtime adapter is a pure
 * pass-through: {@code Loaded} maps to 200 + body + ETag header, {@code NotModified} maps to 304 +
 * ETag header, and it reads only the ETag, never the {@link ExtensionPayload}.
 *
 * @param <O> the Iceberg response type the {@code Loaded} case returns
 * @param <E> the provider-private extension payload type
 */
public sealed interface ConditionalLoadOutcome<O, E extends ExtensionPayload>
    permits ConditionalLoadOutcome.Loaded, ConditionalLoadOutcome.NotModified {

  /**
   * The body was loaded; {@code result} carries the Iceberg response, its ETag, and the extension.
   */
  record Loaded<O, E extends ExtensionPayload>(PolarisResult<O, E> result)
      implements ConditionalLoadOutcome<O, E> {
    public Loaded {
      Objects.requireNonNull(result, "result");
    }
  }

  /**
   * The caller's cached copy is still current; there is no body, but the ETag still transits (so
   * the impl's not-modified decision is fully expressed) alongside any provider extension.
   */
  record NotModified<O, E extends ExtensionPayload>(Optional<String> etag, E extension)
      implements ConditionalLoadOutcome<O, E> {
    public NotModified {
      Objects.requireNonNull(etag, "etag");
      Objects.requireNonNull(extension, "extension");
    }
  }
}
