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
package org.apache.polaris.core.persistence.resolver;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The result of an {@link EntityResolver#resolve} call: clean data, addressable by the request
 * (ADR-0008 Decision 2). The provider fills this structure and returns it; it is a data carrier,
 * not a behavior-bearing object, and implements no read ergonomics beyond direct lookups. Reading a
 * resolved path is a direct lookup on {@link #resolvedPaths()} keyed by the {@link ResolvedPathKey}
 * the caller requested (ADR-0008 Decision 6 — no separate read-view).
 *
 * <p>The result is a single-call, point-in-time consistent snapshot (ADR-0008 Decision 4): the
 * resolved entities, their grant records, and the caller's activated roles are internally
 * consistent (no mix of stale and fresh).
 *
 * <p>On a non-success {@link #status()}, only the status is meaningful (use {@link #isSuccess()}
 * before reading resolved data); a required path that failed to resolve is reported through the
 * status, an optional path that failed to fully resolve is simply absent or partial in {@link
 * #resolvedPaths()}.
 */
public record ResolutionResult(
    @NonNull ResolverStatus status,
    @Nullable ResolvedPolarisEntity resolvedCallerPrincipal,
    @NonNull List<ResolvedPolarisEntity> resolvedCallerPrincipalRoles,
    @Nullable ResolvedPolarisEntity resolvedReferenceCatalog,
    @Nullable Map<Long, ResolvedPolarisEntity> resolvedCatalogRoles,
    @NonNull Map<ResolvedPathKey, List<ResolvedPolarisEntity>> resolvedPaths) {

  public ResolutionResult {
    Objects.requireNonNull(status, "status must be non-null");
    resolvedCallerPrincipalRoles = List.copyOf(resolvedCallerPrincipalRoles);
    resolvedPaths = Map.copyOf(resolvedPaths);
  }

  /** A failure result carrying only the status; resolved data is empty. */
  public static ResolutionResult failure(@NonNull ResolverStatus status) {
    return new ResolutionResult(status, null, List.of(), null, null, Map.of());
  }

  public boolean isSuccess() {
    return status.getStatus() == ResolverStatus.StatusEnum.SUCCESS;
  }

  /**
   * The resolved entities for a requested path, or {@code null} if the path was not requested or
   * (if optional) did not resolve. The last element is the leaf; earlier elements are the resolved
   * parents.
   */
  public @Nullable List<ResolvedPolarisEntity> resolvedPath(@NonNull ResolvedPathKey key) {
    return resolvedPaths.get(key);
  }
}
