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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.resolution.model.ResolutionRequest;
import org.apache.polaris.resolution.model.ResolutionResult;
import org.apache.polaris.resolution.model.ResolutionStatus;
import org.junit.jupiter.api.Test;

class DefaultEntityResolverTest {

  private static Map<String, String> store() {
    Map<String, String> s = new HashMap<>();
    s.put("cat", "CATALOG");
    s.put("ns", "NAMESPACE");
    s.put("tbl", "TABLE");
    return s;
  }

  @Test
  void requiredPathThatDoesNotFullyResolveIsAnError() {
    DefaultEntityResolver resolver = new DefaultEntityResolver(store(), Map.of());
    ResolutionRequest req =
        ResolutionRequest.builder("alice", "cat")
            .requiredPath(List.of("cat", "ns", "missing"))
            .build();

    ResolutionResult result = resolver.resolve(req);

    assertThat(result.status()).isEqualTo(ResolutionStatus.PATH_NOT_FULLY_RESOLVED);
    assertThat(result.resolvedPath(List.of("cat", "ns", "missing")))
        .get()
        .satisfies(p -> assertThat(p.isFullyResolved()).isFalse());
  }

  @Test
  void optionalPathThatDoesNotFullyResolveIsPartialButNotAnError() {
    DefaultEntityResolver resolver = new DefaultEntityResolver(store(), Map.of());
    ResolutionRequest req =
        ResolutionRequest.builder("alice", "cat")
            .optionalPath(List.of("cat", "ns", "missing"))
            .build();

    ResolutionResult result = resolver.resolve(req);

    assertThat(result.status()).isEqualTo(ResolutionStatus.SUCCESS);
    assertThat(result.resolvedPath(List.of("cat", "ns", "missing")).orElseThrow().isFullyResolved())
        .isFalse();
  }

  @Test
  void resolvedPathExposesLeafSubTypeAndActivatedRoles() {
    DefaultEntityResolver resolver =
        new DefaultEntityResolver(store(), Map.of("alice", List.of("data_engineer")));
    ResolutionRequest req =
        ResolutionRequest.builder("alice", "cat").requiredPath(List.of("cat", "ns", "tbl")).build();

    ResolutionResult result = resolver.resolve(req);

    assertThat(result.status()).isEqualTo(ResolutionStatus.SUCCESS);
    assertThat(result.activatedPrincipalRoles()).containsExactly("data_engineer");
    assertThat(result.resolvedPath(List.of("cat", "ns", "tbl")).orElseThrow().leafSubType())
        .contains("TABLE");
  }

  @Test
  void resultIsAPointInTimeSnapshotUnaffectedByLaterStoreMutation() {
    Map<String, String> liveStore = store();
    DefaultEntityResolver resolver = new DefaultEntityResolver(liveStore, Map.of());
    ResolutionRequest req =
        ResolutionRequest.builder("alice", "cat").requiredPath(List.of("cat", "ns", "tbl")).build();

    ResolutionResult result = resolver.resolve(req);

    // Mutate the live store AFTER resolving.
    liveStore.remove("tbl");
    liveStore.put("cat", "SOMETHING_ELSE");

    // The already-returned result reflects the snapshot taken during resolve, not the mutation.
    assertThat(result.resolvedPath(List.of("cat", "ns", "tbl")).orElseThrow().isFullyResolved())
        .isTrue();
    assertThat(result.resolvedPath(List.of("cat", "ns", "tbl")).orElseThrow().leafSubType())
        .contains("TABLE");
  }
}
