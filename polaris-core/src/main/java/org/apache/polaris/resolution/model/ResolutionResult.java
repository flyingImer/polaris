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
package org.apache.polaris.resolution.model;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The resolution result: clean data, addressable by the request's name-paths. The provider fills
 * this structure and returns it; it carries no behavior and no live handles. Consumers read a
 * resolved path directly ({@link #resolvedPath}); there is no separate read-view.
 */
public record ResolutionResult(
    String resolvedPrincipal,
    List<String> activatedPrincipalRoles,
    String referenceCatalog,
    List<String> activatedCatalogRoles,
    Map<List<String>, ResolvedPath> pathsByName,
    List<ResolvedEntity> resolvedTopLevelEntities,
    ResolutionStatus status) {

  public ResolutionResult {
    activatedPrincipalRoles = List.copyOf(activatedPrincipalRoles);
    activatedCatalogRoles = List.copyOf(activatedCatalogRoles);
    pathsByName = Map.copyOf(pathsByName);
    resolvedTopLevelEntities = List.copyOf(resolvedTopLevelEntities);
  }

  /** Look up a resolved path by the exact name-path the caller requested. */
  public Optional<ResolvedPath> resolvedPath(List<String> names) {
    return Optional.ofNullable(pathsByName.get(names));
  }

  public boolean isSuccess() {
    return status == ResolutionStatus.SUCCESS;
  }
}
