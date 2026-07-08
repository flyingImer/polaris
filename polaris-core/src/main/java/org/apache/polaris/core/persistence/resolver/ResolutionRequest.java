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
import java.util.Objects;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * A resolution request, expressed by names only (ADR-0008 Decision 2). It carries the caller
 * principal, the reference catalog, the name-paths to resolve (each marked required or optional via
 * {@link ResolverPath#optional()}), and the top-level names to resolve. It carries names, not
 * resolved handles, so it is serializable and remote-friendly (the same shape principle as
 * ADR-0005's {@code AuthorizationRequest}).
 *
 * <p>Component selection (which of principal / roles / catalog to resolve) is not yet a request
 * field: the current contract resolves the caller principal, activated roles, and reference catalog
 * alongside the requested paths, matching {@link Resolver#resolveAll()}. A selection field is a
 * deferred refinement (ADR-0008 leaves the exact request fields to implementation).
 */
public record ResolutionRequest(
    @NonNull PolarisPrincipal principal,
    @Nullable String referenceCatalogName,
    @NonNull List<ResolverPath> paths,
    @NonNull List<ResolverEntityName> topLevelNames) {

  public ResolutionRequest {
    Objects.requireNonNull(principal, "principal must be non-null");
    paths = List.copyOf(paths);
    topLevelNames = List.copyOf(topLevelNames);
  }

  /** Request resolving only the caller principal, activated roles, and reference catalog. */
  public static ResolutionRequest of(
      @NonNull PolarisPrincipal principal, @Nullable String referenceCatalogName) {
    return new ResolutionRequest(principal, referenceCatalogName, List.of(), List.of());
  }

  /** Request resolving the given name-paths within the reference catalog. */
  public static ResolutionRequest ofPaths(
      @NonNull PolarisPrincipal principal,
      @Nullable String referenceCatalogName,
      @NonNull List<ResolverPath> paths) {
    return new ResolutionRequest(principal, referenceCatalogName, paths, List.of());
  }
}
