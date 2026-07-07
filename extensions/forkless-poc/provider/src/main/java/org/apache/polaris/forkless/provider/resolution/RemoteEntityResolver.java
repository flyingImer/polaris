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
package org.apache.polaris.forkless.provider.resolution;

import java.util.ArrayList;
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
 * Provider-analog resolver. It owns resolution end to end: one consistent read of the whole backing
 * store per request, then the entire request resolved from that one snapshot. This is what a remote
 * provider does to collapse the per-name and per-revalidation round-trips into a single read. It
 * implements the interface directly, so there is no concrete resolver to subclass and delegate
 * around. Framework-free: depends only on polaris-core.
 */
public class RemoteEntityResolver implements EntityResolver {

  private final Map<String, String> backingStore;
  private int snapshotReads = 0;

  public RemoteEntityResolver(Map<String, String> backingStore) {
    this.backingStore = backingStore;
  }

  /** How many times the whole-store snapshot was read; one per {@code resolve}. */
  public int snapshotReads() {
    return snapshotReads;
  }

  @Override
  public ResolutionResult resolve(ResolutionRequest request) {
    // One read of the entire backing store for the whole request.
    Map<String, String> snapshot = Map.copyOf(backingStore);
    snapshotReads++;

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

    return new ResolutionResult(
        request.principalName(),
        List.of(),
        request.referenceCatalog(),
        List.of(),
        pathsByName,
        topLevel,
        status);
  }
}
