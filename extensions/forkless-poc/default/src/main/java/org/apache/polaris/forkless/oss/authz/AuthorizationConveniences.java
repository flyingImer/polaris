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
package org.apache.polaris.forkless.oss.authz;

import org.apache.polaris.authz.model.AuthorizationDecision;
import org.apache.polaris.authz.model.AuthorizationRequest;
import org.apache.polaris.spi.substrate.PolarisDecisionAuthorizer;

/**
 * Convenience adapters over the decision-native {@link PolarisDecisionAuthorizer}. The legacy
 * throw-on-deny shape lives here as a thin wrapper, showing it is a caller-side convenience built
 * OVER {@code authorize}, not the primitive the contract is defined in terms of.
 */
public final class AuthorizationConveniences {

  private AuthorizationConveniences() {}

  public static void authorizeOrThrow(
      PolarisDecisionAuthorizer authorizer, AuthorizationRequest request) {
    AuthorizationDecision decision = authorizer.authorize(request);
    if (!decision.isAllowed()) {
      throw new AccessDeniedException(decision.message().orElse("Authorization denied"));
    }
  }
}
