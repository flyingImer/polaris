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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.context.catalog.RealmContextHolder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link RealmContextPropagator}. */
class RealmContextPropagatorTest {

  private RealmContextHolder holder;
  private RealmContextPropagator propagator;

  @BeforeEach
  void setUp() {
    holder = new RealmContextHolder();
    propagator = new RealmContextPropagator(holder);
  }

  @Test
  void capture_withRealmInHolder_setsRealmId() {
    holder.set(() -> "test-realm");

    TaskContext.Builder builder = TaskContext.builder();
    propagator.capture(builder);
    builder.principal(dummyPrincipal());

    TaskContext ctx = builder.build();
    assertThat(ctx.realmId()).isEqualTo("test-realm");
  }

  @Test
  void capture_withNullRealmInHolder_doesNotSetRealmId() {
    // Holder is empty – get() returns null.
    TaskContext.Builder builder = TaskContext.builder();
    propagator.capture(builder);
    builder.principal(dummyPrincipal());

    // realmId was not set, so build() must throw.
    assertThrows(IllegalStateException.class, builder::build);
  }

  @Test
  void restore_setsRealmInHolder() throws Exception {
    TaskContext ctx = buildContext("restored-realm");

    RealmContextHolder targetHolder = new RealmContextHolder();
    RealmContextPropagator targetPropagator = new RealmContextPropagator(targetHolder);

    try (AutoCloseable scope = targetPropagator.restore(ctx)) {
      assertThat(targetHolder.get()).isNotNull();
      assertThat(targetHolder.get().getRealmIdentifier()).isEqualTo("restored-realm");
    }
  }

  @Test
  void restore_returnedCloseableIsNoOp() throws Exception {
    RealmContextHolder targetHolder = new RealmContextHolder();
    RealmContextPropagator targetPropagator = new RealmContextPropagator(targetHolder);
    TaskContext ctx = buildContext("realm");

    AutoCloseable scope = targetPropagator.restore(ctx);
    // Must not throw and must return a non-null closeable.
    assertThat(scope).isNotNull();
    scope.close(); // no-op; should not throw
  }

  // ---- helpers ----

  private TaskContext buildContext(String realmId) {
    return TaskContext.builder().realmId(realmId).principal(dummyPrincipal()).build();
  }

  private PolarisPrincipal dummyPrincipal() {
    return PolarisPrincipal.of("test-user", Map.of(), Set.of());
  }
}
