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
import org.apache.polaris.core.auth.PolarisPrincipal;

/**
 * Captures the execution context that must be restored when an async task runs in a new CDI request
 * scope. Built at task-submission time via {@link AsyncContextPropagator#capture(Builder)}, then
 * passed to {@link AsyncContextPropagator#restore(TaskContext)} in the task thread.
 *
 * <p>This snapshot complements, rather than replaces, {@link
 * org.apache.polaris.core.context.CallContext#copy()}. {@code CallContext.copy()} preserves the
 * business-logic context used by task handlers; {@code TaskContext} preserves the request-scoped
 * values that CDI-injected beans need after {@code @ActivateRequestContext} creates a fresh scope
 * in the task thread.
 *
 * <p>Fields are explicit and typed – this is not a generic attribute bag. Adding a new propagated
 * value is a deliberate schema change to this class.
 */
public final class TaskContext {

  private final String realmId;
  private final PolarisPrincipal principal;
  @Nullable private final String requestId;

  private TaskContext(Builder builder) {
    this.realmId = builder.realmId;
    this.principal = builder.principal;
    this.requestId = builder.requestId;
  }

  public String realmId() {
    return realmId;
  }

  public PolarisPrincipal principal() {
    return principal;
  }

  @Nullable
  public String requestId() {
    return requestId;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String realmId;
    private PolarisPrincipal principal;
    private String requestId;

    public Builder realmId(String realmId) {
      this.realmId = realmId;
      return this;
    }

    public Builder principal(PolarisPrincipal principal) {
      this.principal = principal;
      return this;
    }

    public Builder requestId(@Nullable String requestId) {
      this.requestId = requestId;
      return this;
    }

    public TaskContext build() {
      if (realmId == null) {
        throw new IllegalStateException(
            "Task context incomplete: realmId not captured (check RealmContextPropagator).");
      }
      if (principal == null) {
        throw new IllegalStateException(
            "Task context incomplete: principal not captured (check PrincipalContextPropagator).");
      }
      return new TaskContext(this);
    }
  }
}
