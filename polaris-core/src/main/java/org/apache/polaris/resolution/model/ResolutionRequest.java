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

import java.util.ArrayList;
import java.util.List;

/**
 * A resolution request: names only, so it is self-describing and remote-friendly (no live entity
 * handles). It carries the caller principal, the reference catalog, the name-paths to resolve (each
 * required or optional), and top-level names.
 */
public record ResolutionRequest(
    String principalName,
    String referenceCatalog,
    List<RequestedPath> paths,
    List<String> topLevelNames) {

  public ResolutionRequest {
    paths = List.copyOf(paths);
    topLevelNames = List.copyOf(topLevelNames);
  }

  public static Builder builder(String principalName, String referenceCatalog) {
    return new Builder(principalName, referenceCatalog);
  }

  public static final class Builder {
    private final String principalName;
    private final String referenceCatalog;
    private final List<RequestedPath> paths = new ArrayList<>();
    private final List<String> topLevelNames = new ArrayList<>();

    private Builder(String principalName, String referenceCatalog) {
      this.principalName = principalName;
      this.referenceCatalog = referenceCatalog;
    }

    public Builder requiredPath(List<String> names) {
      paths.add(RequestedPath.required(names));
      return this;
    }

    public Builder optionalPath(List<String> names) {
      paths.add(RequestedPath.optional(names));
      return this;
    }

    public Builder topLevelName(String name) {
      topLevelNames.add(name);
      return this;
    }

    public ResolutionRequest build() {
      return new ResolutionRequest(principalName, referenceCatalog, paths, topLevelNames);
    }
  }
}
