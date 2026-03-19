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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.service.context.catalog.RealmContextHolder;

/**
 * Propagates the realm context across the async task boundary via {@link RealmContextHolder}.
 *
 * <p>At capture time {@link RealmContextHolder} is request-scoped; its CDI proxy resolves to the
 * holder in the currently active request scope. On a normal HTTP request thread that scope holds
 * the realm set by the request filter. When an async task handler schedules a follow-up task (no
 * active JAX-RS request), {@code RealmContextHolder} in that task's scope already contains the
 * realm restored by this propagator's {@link #restore} path, so capture continues to work correctly
 * for nested task submission.
 */
@ApplicationScoped
public class RealmContextPropagator implements AsyncContextPropagator {

  private final RealmContextHolder realmContextHolder;

  @SuppressWarnings("unused") // Required by CDI
  protected RealmContextPropagator() {
    this(null);
  }

  @Inject
  public RealmContextPropagator(RealmContextHolder realmContextHolder) {
    this.realmContextHolder = realmContextHolder;
  }

  @Override
  public void capture(TaskContext.Builder builder) {
    // RealmContextHolder is request-scoped; its CDI proxy resolves to the current scope here.
    var rc = realmContextHolder.get();
    if (rc != null) {
      builder.realmId(rc.getRealmIdentifier());
    }
  }

  @Override
  public AutoCloseable restore(TaskContext context) {
    // Set the realm in the new request scope that @ActivateRequestContext created for this task.
    realmContextHolder.set(context::realmId);
    return () -> {}; // RequestScoped bean is destroyed with the scope; no extra cleanup needed.
  }
}
