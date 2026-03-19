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

import jakarta.annotation.Nonnull;

/**
 * Extension point for propagating request-scoped context across the async task boundary.
 *
 * <p>Implementations are CDI beans (typically {@code @ApplicationScoped}). {@link TaskExecutorImpl}
 * discovers all implementations via CDI {@code Instance} injection, so adding a new propagation
 * behavior only requires a new implementation. If the new behavior carries additional context data,
 * it also requires extending the shared {@link TaskContext} schema. In that sense, execution wiring
 * is open, while the typed schema is deliberately a closed contract. The values carried across the
 * async boundary are intentionally defined by the typed {@link TaskContext}.
 *
 * <p>Lifecycle:
 *
 * <ol>
 *   <li>{@link #capture} is called in the request thread (active request scope) to populate a
 *       {@link TaskContext.Builder} from the current context.
 *   <li>The built {@link TaskContext} is captured and passed through the async boundary.
 *   <li>{@link #restore} is called inside the task thread's new CDI request scope to re-establish
 *       the captured context. The returned {@link AutoCloseable} is closed after the task finishes
 *       to perform any needed cleanup (e.g. MDC restoration).
 * </ol>
 */
public interface AsyncContextPropagator {

  /**
   * Reads relevant context from the current (request-thread) scope and writes it into {@code
   * builder}.
   */
  void capture(TaskContext.Builder builder);

  /**
   * Restores the captured context into the task thread's active request scope. Returns an {@link
   * AutoCloseable} that must be closed after the task finishes. Implementations that need no
   * cleanup must return a no-op: {@code () -> {}}. Returning {@code null} is not permitted.
   */
  @Nonnull
  AutoCloseable restore(TaskContext context);
}
