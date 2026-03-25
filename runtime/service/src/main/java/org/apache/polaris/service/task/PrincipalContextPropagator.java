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

import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.polaris.core.auth.ImmutablePolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.context.catalog.PolarisPrincipalHolder;

/**
 * Propagates the authenticated principal across the async task boundary via {@link
 * PolarisPrincipalHolder}.
 *
 * <p>A clone of the principal is captured at submission time so the task thread uses a stable
 * snapshot that is independent of the originating request scope's lifecycle.
 */
@ApplicationScoped
public class PrincipalContextPropagator implements AsyncContextPropagator {

  private final PolarisPrincipalHolder polarisPrincipalHolder;
  private final Instance<PolarisPrincipal> polarisPrincipal;

  @SuppressWarnings("unused") // Required by CDI
  protected PrincipalContextPropagator() {
    this(null, null);
  }

  @Inject
  public PrincipalContextPropagator(
      PolarisPrincipalHolder polarisPrincipalHolder, Instance<PolarisPrincipal> polarisPrincipal) {
    this.polarisPrincipalHolder = polarisPrincipalHolder;
    this.polarisPrincipal = polarisPrincipal;
  }

  @Nullable
  @Override
  public Object capture() {
    if (polarisPrincipal.isResolvable()) {
      try {
        PolarisPrincipal principal = polarisPrincipal.get();
        // Clone so the task thread has a stable snapshot regardless of the request scope lifecycle.
        return ImmutablePolarisPrincipal.builder().from(principal).build();
      } catch (jakarta.enterprise.context.ContextNotActiveException e) {
        // No active request scope — principal is simply absent.
        return null;
      }
    }
    return null;
  }

  @Override
  public AutoCloseable restore(@Nullable Object capturedState) {
    if (capturedState != null) {
      polarisPrincipalHolder.set((PolarisPrincipal) capturedState);
    }
    return () -> {};
  }
}
