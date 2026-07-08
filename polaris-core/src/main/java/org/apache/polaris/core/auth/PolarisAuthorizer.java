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
package org.apache.polaris.core.auth;

import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/** Interface for invoking authorization checks. */
public interface PolarisAuthorizer {
  /**
   * Core authorization entry point: decide allow/deny for a names-only {@link AuthorizationRequest}
   * (ADR-0005 Decision 4). The request carries only the principal and typed intents over
   * name-addressed securables, no resolved state. An implementation that needs resolved entities
   * composes the {@link org.apache.polaris.core.persistence.resolver.EntityResolver} SPI itself,
   * turning the request's names into a resolved snapshot; a remote authorizer can forward the names
   * directly.
   *
   * <p>When a request contains multiple intents, they form a single batch contract: implementations
   * must AND-combine the intents and may short-circuit evaluation on the first deny.
   */
  @NonNull AuthorizationDecision authorize(@NonNull AuthorizationRequest request);

  /**
   * Convenience method that throws a {@link ForbiddenException} when authorization is denied.
   *
   * <p>Implementations should provide allow/deny decisions via {@link #authorize}.
   */
  default void authorizeOrThrow(@NonNull AuthorizationRequest request) {
    AuthorizationDecision decision = authorize(request);
    if (!decision.isAllowed()) {
      String message = decision.getMessage().orElse("Authorization denied");
      throw new ForbiddenException("%s", message);
    }
  }

  void authorizeOrThrow(
      @NonNull PolarisPrincipal polarisPrincipal,
      @NonNull Set<PolarisBaseEntity> activatedEntities,
      @NonNull PolarisAuthorizableOperation authzOp,
      @Nullable PolarisResolvedPathWrapper target,
      @Nullable PolarisResolvedPathWrapper secondary);

  void authorizeOrThrow(
      @NonNull PolarisPrincipal polarisPrincipal,
      @NonNull Set<PolarisBaseEntity> activatedEntities,
      @NonNull PolarisAuthorizableOperation authzOp,
      @Nullable List<PolarisResolvedPathWrapper> targets,
      @Nullable List<PolarisResolvedPathWrapper> secondaries);

  /**
   * Decision-native per-op convenience: returns allow/deny for one already-resolved operation
   * instead of throwing, so callers can branch on the outcome rather than probe with try/catch.
   *
   * <p>The default derives the decision from {@link #authorizeOrThrow(PolarisPrincipal, Set,
   * PolarisAuthorizableOperation, PolarisResolvedPathWrapper, PolarisResolvedPathWrapper)} so an
   * implementation that only knows how to throw still works. A decision-native implementation
   * should override this to compute the decision directly (ADR-0005 Decision 1).
   */
  default @NonNull AuthorizationDecision authorize(
      @NonNull PolarisPrincipal polarisPrincipal,
      @NonNull Set<PolarisBaseEntity> activatedEntities,
      @NonNull PolarisAuthorizableOperation authzOp,
      @Nullable PolarisResolvedPathWrapper target,
      @Nullable PolarisResolvedPathWrapper secondary) {
    try {
      authorizeOrThrow(polarisPrincipal, activatedEntities, authzOp, target, secondary);
      return AuthorizationDecision.allow();
    } catch (ForbiddenException e) {
      return AuthorizationDecision.deny(e.getMessage());
    }
  }
}
