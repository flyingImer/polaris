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
package org.apache.polaris.service.task;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.arc.Arc;
import io.quarkus.arc.ManagedContext;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.context.RequestIdSupplier;
import org.apache.polaris.service.context.catalog.PolarisPrincipalHolder;
import org.apache.polaris.service.context.catalog.RealmContextHolder;
import org.apache.polaris.service.context.catalog.RequestIdHolder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

@QuarkusTest
class TaskContextLifecycleTest {
  @Inject TaskContextPropagator propagator;
  @Inject RealmContextHolder realmHolder;
  @Inject PolarisPrincipalHolder principalHolder;
  @Inject RequestIdHolder requestIdHolder;
  @Inject RealmContext realmContext;
  @Inject PolarisPrincipal principal;
  @Inject RequestIdSupplier requestIdSupplier;

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "request-123")
  void testContextSurvivesSourceScopeAndRepeatedRestoration(String requestId) throws Exception {
    try (var worker = Executors.newSingleThreadExecutor()) {
      CapturedTaskContext captured =
          worker
              .submit(
                  () -> {
                    ManagedContext scope = Arc.container().requestContext();
                    scope.activate();
                    try {
                      realmHolder.set(() -> "source-realm");
                      principalHolder.set(PolarisPrincipal.of("alice", Map.of(), Set.of()));
                      requestIdHolder.set(requestId);
                      return propagator.capture();
                    } finally {
                      scope.terminate();
                    }
                  })
              .get(10, TimeUnit.SECONDS);

      // Reuse the snapshot in successive worker scopes after the source scope has ended.
      for (int attempt = 0; attempt < 2; attempt++) {
        worker
            .submit(
                () -> {
                  ManagedContext scope = Arc.container().requestContext();
                  assertThat(scope.isActive()).isFalse();
                  scope.activate();
                  try {
                    // Resolve the supplier before restoration to check that it reads the holder
                    // lazily, and that the previous worker scope did not leak its request ID.
                    assertThat(requestIdSupplier.get()).isNull();
                    propagator.restore(captured);
                    assertThat(realmContext.getRealmIdentifier()).isEqualTo("source-realm");
                    assertThat(principal.getName()).isEqualTo("alice");
                    assertThat(requestIdSupplier.get()).isEqualTo(requestId);
                  } finally {
                    scope.terminate();
                  }
                })
            .get(10, TimeUnit.SECONDS);
      }
    }
  }
}
