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
package org.apache.polaris.spi.substrate;

import org.apache.polaris.core.persistence.resolver.DefaultEntityResolver;
import org.apache.polaris.core.persistence.resolver.Resolver;
import org.apache.polaris.core.persistence.resolver.ResolutionRequest;
import org.apache.polaris.core.persistence.resolver.ResolutionResult;
import org.jspecify.annotations.NonNull;

/**
 * Functional entity-resolution SPI (ADR-0008 Decision 1). A provider implements ONE method and owns
 * resolution end to end: given a {@link ResolutionRequest} carrying names (caller principal,
 * reference catalog, name-paths, top-level names), it returns a {@link ResolutionResult} that is a
 * single-call, point-in-time consistent snapshot of the resolved entities, their grant records, and
 * the caller's activated roles.
 *
 * <p>This replaces the stateful add-then-{@code resolveAll}-then-{@code getResolved} builder
 * ({@link Resolver}) at the seam: the request carries names (not resolved handles) so it is
 * serializable and a remote provider can resolve a whole request in one consistent read, instead of
 * the generic builder's per-name loading and caller-driven revalidation. The result is a data
 * carrier addressable by the request; reading a resolved path is a direct lookup on the result, not
 * a call into a separate view (ADR-0008 Decision 2 and Decision 6).
 *
 * <p>The consistency guarantee is part of the contract (ADR-0008 Decision 4): each result is a
 * single-call snapshot with no mix of stale and fresh entities. Two {@code resolve} calls in one
 * request are two independent snapshots. The mechanism is the implementation's choice; the OSS
 * default ({@link DefaultEntityResolver}) keeps today's multi-pass revalidation, a remote provider
 * does one consistent read.
 */
public interface EntityResolver {

  /**
   * Resolve every name in the request into a consistent snapshot of resolved entities, grant
   * records, and activated roles. Required paths that do not fully resolve produce an error status;
   * optional paths that do not fully resolve produce a partial result and no error (ADR-0008
   * Decision 3). The provider only honors the per-path required/optional flag; it does not decide
   * strictness, and conflict enforcement stays at the durable/operation layer.
   */
  @NonNull ResolutionResult resolve(@NonNull ResolutionRequest request);
}
