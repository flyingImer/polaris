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
package org.apache.polaris.forkless.oss.resolution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.resolution.model.RequestedPath;
import org.apache.polaris.resolution.model.ResolutionRequest;
import org.apache.polaris.resolution.model.ResolutionResult;
import org.apache.polaris.resolution.model.ResolutionStatus;
import org.apache.polaris.resolution.model.ResolvedEntity;
import org.apache.polaris.resolution.model.ResolvedPath;
import org.apache.polaris.spi.feature.EntityResolver;

/**
 * OSS-default resolver. Resolves name by name against a live entity store, materializing a result.
 * It takes one snapshot of the store per {@code resolve} so the whole result reflects a single
 * point in time (no mix of stale and fresh); the materialized result is then independent of later
 * store changes.
 */
public class DefaultEntityResolver implements EntityResolver {

  private final Map<String, String> store;
  private final Map<String, List<String>> principalRoles;

  public DefaultEntityResolver(
      Map<String, String> store, Map<String, List<String>> principalRoles) {
    // Not copied: this models a live durable store the resolver reads from.
    this.store = store;
    this.principalRoles = principalRoles;
  }

  @Override
  public ResolutionResult resolve(ResolutionRequest request) {
    Map<String, String> snapshot = new HashMap<>(store);

    Map<List<String>, ResolvedPath> pathsByName = new LinkedHashMap<>();
    ResolutionStatus status = ResolutionStatus.SUCCESS;

    for (RequestedPath rp : request.paths()) {
      List<ResolvedEntity> resolved = new ArrayList<>();
      boolean fully = true;
      for (String name : rp.names()) {
        String subType = snapshot.get(name);
        if (subType == null) {
          fully = false;
          break;
        }
        resolved.add(new ResolvedEntity(name, subType));
      }
      pathsByName.put(rp.names(), new ResolvedPath(rp.names(), resolved, fully));
      if (!fully && rp.required()) {
        status = ResolutionStatus.PATH_NOT_FULLY_RESOLVED;
      }
    }

    List<ResolvedEntity> topLevel = new ArrayList<>();
    for (String name : request.topLevelNames()) {
      String subType = snapshot.get(name);
      if (subType != null) {
        topLevel.add(new ResolvedEntity(name, subType));
      }
    }

    List<String> roles = principalRoles.getOrDefault(request.principalName(), List.of());
    return new ResolutionResult(
        request.principalName(),
        roles,
        request.referenceCatalog(),
        List.of(),
        pathsByName,
        topLevel,
        status);
  }
}
