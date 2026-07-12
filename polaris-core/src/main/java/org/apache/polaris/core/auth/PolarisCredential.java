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

import java.util.Set;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;
import org.jspecify.annotations.Nullable;

/**
 * Polaris-specific authentication attributes extracted from an incoming request: the principal
 * id, name, roles, and token. Framework-agnostic; a runtime module adapts this to whatever
 * identity/credential model it serves (e.g. Quarkus Security's {@code Credential}).
 */
@PolarisImmutable
public interface PolarisCredential {

  static PolarisCredential of(
      @Nullable Long principalId, @Nullable String principalName, Set<String> principalRoles) {
    return of(principalId, principalName, principalRoles, null);
  }

  static PolarisCredential of(
      @Nullable Long principalId,
      @Nullable String principalName,
      Set<String> principalRoles,
      @Nullable String token) {
    return ImmutablePolarisCredential.builder()
        .principalId(principalId)
        .principalName(principalName)
        .principalRoles(principalRoles)
        .token(token)
        .build();
  }

  /** The principal id, or null if unknown. Used for principal lookups by id. */
  @Nullable Long getPrincipalId();

  /** The principal name, or null if unknown. Used for principal lookups by name. */
  @Nullable String getPrincipalName();

  /** The principal roles, or empty if the principal has no roles. */
  Set<String> getPrincipalRoles();

  /**
   * The access token of the current user, or null if not applicable. Redacted from {@code
   * toString()} so it is not leaked into logs.
   */
  @Value.Redacted
  @Nullable String getToken();
}
