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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.resolution.model.ResolutionRequest;
import org.apache.polaris.resolution.model.ResolutionResult;
import org.apache.polaris.resolution.model.ResolutionStatus;
import org.junit.jupiter.api.Test;

class RemoteEntityResolverTest {

  private static Map<String, String> store() {
    Map<String, String> s = new HashMap<>();
    s.put("cat", "CATALOG");
    s.put("ns", "NAMESPACE");
    s.put("t1", "TABLE");
    s.put("t2", "TABLE");
    return s;
  }

  @Test
  void resolvesWholeRequestInOneRead() {
    RemoteEntityResolver resolver = new RemoteEntityResolver(store());
    ResolutionRequest req =
        ResolutionRequest.builder("alice", "cat")
            .requiredPath(List.of("cat", "ns", "t1"))
            .requiredPath(List.of("cat", "ns", "t2"))
            .optionalPath(List.of("cat", "ns", "t3"))
            .build();

    ResolutionResult result = resolver.resolve(req);

    // One consistent read for the entire multi-path request, not one per name.
    assertThat(resolver.snapshotReads()).isEqualTo(1);
    assertThat(result.status()).isEqualTo(ResolutionStatus.SUCCESS);
    assertThat(result.resolvedPath(List.of("cat", "ns", "t1")).orElseThrow().isFullyResolved())
        .isTrue();
    assertThat(result.resolvedPath(List.of("cat", "ns", "t2")).orElseThrow().isFullyResolved())
        .isTrue();
  }

  @Test
  void requiredMissingLeafIsAnError() {
    RemoteEntityResolver resolver = new RemoteEntityResolver(store());
    ResolutionRequest req =
        ResolutionRequest.builder("alice", "cat")
            .requiredPath(List.of("cat", "ns", "nope"))
            .build();

    ResolutionResult result = resolver.resolve(req);

    assertThat(result.status()).isEqualTo(ResolutionStatus.PATH_NOT_FULLY_RESOLVED);
    assertThat(resolver.snapshotReads()).isEqualTo(1);
  }
}
