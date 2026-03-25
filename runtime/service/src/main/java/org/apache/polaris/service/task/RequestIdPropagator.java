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
import jakarta.inject.Inject;
import org.apache.polaris.service.context.catalog.RequestIdHolder;
import org.apache.polaris.service.tracing.RequestIdFilter;
import org.jboss.resteasy.reactive.server.core.CurrentRequestManager;
import org.jboss.resteasy.reactive.server.core.ResteasyReactiveRequestContext;
import org.slf4j.MDC;

/**
 * Propagates the request ID across the async task boundary.
 *
 * <p>At capture time the request ID is read from the current request-scoped source. On a normal
 * HTTP request thread that means the JAX-RS request properties populated by {@code
 * RequestIdFilter}. On a task thread scheduling another task, the same ID is already restored in
 * {@link RequestIdHolder}, so capture falls back to the holder when no live JAX-RS request exists.
 *
 * <p>At restore time the ID is written to both the {@link RequestIdHolder} (so that {@code
 * RequestIdSupplier} works in task threads) and to the SLF4J MDC (so that log messages emitted by
 * the task carry the originating request ID).
 *
 * <p>MDC cleanup is performed by the returned {@link AutoCloseable} so that thread-pool threads are
 * left in a clean state after the task completes.
 */
@ApplicationScoped
public class RequestIdPropagator implements AsyncContextPropagator {

  private final RequestIdHolder requestIdHolder;

  @SuppressWarnings("unused") // Required by CDI
  protected RequestIdPropagator() {
    this(null);
  }

  @Inject
  public RequestIdPropagator(RequestIdHolder requestIdHolder) {
    this.requestIdHolder = requestIdHolder;
  }

  @Nullable
  @Override
  public Object capture() {
    ResteasyReactiveRequestContext context = CurrentRequestManager.get();
    if (context != null) {
      return context.getContainerRequestContext().getProperty(RequestIdFilter.REQUEST_ID_KEY);
    }
    // Async handlers can enqueue follow-up tasks from a fresh CDI request scope with no active
    // JAX-RS request. In that case the originating request ID has already been restored into the
    // request-scoped holder by this propagator's restore() path.
    try {
      return requestIdHolder.get();
    } catch (jakarta.enterprise.context.ContextNotActiveException e) {
      // No active request scope — request ID is simply absent.
      return null;
    }
  }

  @Override
  public AutoCloseable restore(@Nullable Object capturedState) {
    String requestId = (String) capturedState;
    requestIdHolder.set(requestId);

    if (requestId == null) {
      return () -> {};
    }

    String previous = MDC.get(RequestIdFilter.REQUEST_ID_KEY);
    MDC.put(RequestIdFilter.REQUEST_ID_KEY, requestId);
    return () -> {
      if (previous != null) {
        MDC.put(RequestIdFilter.REQUEST_ID_KEY, previous);
      } else {
        MDC.remove(RequestIdFilter.REQUEST_ID_KEY);
      }
    };
  }
}
