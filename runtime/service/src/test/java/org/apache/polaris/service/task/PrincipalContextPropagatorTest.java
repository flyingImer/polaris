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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.auth.ImmutablePolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.context.catalog.PolarisPrincipalHolder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link PrincipalContextPropagator}. */
class PrincipalContextPropagatorTest {

  private PolarisPrincipalHolder holder;

  @BeforeEach
  void setUp() {
    holder = new PolarisPrincipalHolder();
  }

  @Test
  void capture_whenPrincipalResolvable_setsPrincipalOnBuilder() {
    PolarisPrincipal principal = PolarisPrincipal.of("alice", Map.of(), Set.of());

    @SuppressWarnings("unchecked")
    Instance<PolarisPrincipal> principalInstance = mock(Instance.class);
    when(principalInstance.isResolvable()).thenReturn(true);
    when(principalInstance.get()).thenReturn(principal);

    PrincipalContextPropagator propagator =
        new PrincipalContextPropagator(holder, principalInstance);

    TaskContext.Builder builder = TaskContext.builder().realmId("realm");
    propagator.capture(builder);

    TaskContext ctx = builder.build();
    assertThat(ctx.principal()).isNotNull();
    assertThat(ctx.principal().getName()).isEqualTo("alice");
  }

  @Test
  void capture_whenPrincipalNotResolvable_doesNotSetPrincipal() {
    @SuppressWarnings("unchecked")
    Instance<PolarisPrincipal> principalInstance = mock(Instance.class);
    when(principalInstance.isResolvable()).thenReturn(false);

    PrincipalContextPropagator propagator =
        new PrincipalContextPropagator(holder, principalInstance);

    TaskContext.Builder builder = TaskContext.builder().realmId("realm");
    propagator.capture(builder);

    // principal was not set; build() must throw.
    assertThrows(IllegalStateException.class, builder::build);
  }

  @Test
  void capture_clonesThePrincipal() {
    PolarisPrincipal original = PolarisPrincipal.of("bob", Map.of(), Set.of());

    @SuppressWarnings("unchecked")
    Instance<PolarisPrincipal> principalInstance = mock(Instance.class);
    when(principalInstance.isResolvable()).thenReturn(true);
    when(principalInstance.get()).thenReturn(original);

    PrincipalContextPropagator propagator =
        new PrincipalContextPropagator(holder, principalInstance);

    TaskContext.Builder builder = TaskContext.builder().realmId("realm");
    propagator.capture(builder);
    TaskContext ctx = builder.build();

    // The stored principal must be an immutable clone, not the original instance.
    assertThat(ctx.principal()).isInstanceOf(ImmutablePolarisPrincipal.class);
    assertThat(ctx.principal()).isNotSameAs(original);
    assertThat(ctx.principal().getName()).isEqualTo("bob");
  }

  @Test
  void restore_setsPrincipalInHolder() throws Exception {
    PolarisPrincipal principal = PolarisPrincipal.of("carol", Map.of(), Set.of());
    TaskContext ctx =
        TaskContext.builder()
            .realmId("realm")
            .principal(ImmutablePolarisPrincipal.builder().from(principal).build())
            .build();

    @SuppressWarnings("unchecked")
    Instance<PolarisPrincipal> principalInstance = mock(Instance.class);
    PrincipalContextPropagator propagator =
        new PrincipalContextPropagator(holder, principalInstance);

    try (AutoCloseable scope = propagator.restore(ctx)) {
      assertThat(scope).isNotNull();
      // Holder's get() requires CDI (not testable directly here), but we can verify set() was
      // called without error, which means the AtomicReference accepted the value exactly once.
    }
    // Calling restore() a second time on a fresh holder must succeed.
    PolarisPrincipalHolder freshHolder = new PolarisPrincipalHolder();
    PrincipalContextPropagator freshPropagator =
        new PrincipalContextPropagator(freshHolder, principalInstance);
    freshPropagator.restore(ctx).close();
  }

  @Test
  void restore_calledTwiceOnSameHolder_throws() throws Exception {
    PolarisPrincipal principal = PolarisPrincipal.of("dave", Map.of(), Set.of());
    TaskContext ctx =
        TaskContext.builder()
            .realmId("realm")
            .principal(ImmutablePolarisPrincipal.builder().from(principal).build())
            .build();

    @SuppressWarnings("unchecked")
    Instance<PolarisPrincipal> principalInstance = mock(Instance.class);
    PrincipalContextPropagator propagator =
        new PrincipalContextPropagator(holder, principalInstance);

    propagator.restore(ctx).close();
    // Second restore on the same holder must throw because set() uses compareAndSet.
    assertThrows(IllegalStateException.class, () -> propagator.restore(ctx));
  }
}
