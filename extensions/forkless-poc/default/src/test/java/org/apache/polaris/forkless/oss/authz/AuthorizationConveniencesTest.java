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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Set;
import org.apache.polaris.authz.model.AuthorizationIntent;
import org.apache.polaris.authz.model.AuthorizationRequest;
import org.apache.polaris.authz.model.PolarisPrincipalRef;
import org.apache.polaris.forkless.oss.authz.RbacDecisionAuthorizer.Grant;
import org.junit.jupiter.api.Test;

class AuthorizationConveniencesTest {

  private static final PolarisPrincipalRef BOB = new PolarisPrincipalRef("bob", List.of());
  private static final AuthorizationRequest REQUEST =
      new AuthorizationRequest(
          BOB, List.of(new AuthorizationIntent.SingleTarget("SELECT", List.of("db", "t"))));

  @Test
  void throwsAccessDeniedOnDeny() {
    RbacDecisionAuthorizer authorizer = new RbacDecisionAuthorizer(Set.of());

    assertThatThrownBy(() -> AuthorizationConveniences.authorizeOrThrow(authorizer, REQUEST))
        .isInstanceOf(AccessDeniedException.class);
  }

  @Test
  void returnsNormallyOnAllow() {
    RbacDecisionAuthorizer authorizer =
        new RbacDecisionAuthorizer(Set.of(new Grant("bob", "SELECT", List.of("db", "t"))));

    assertThatCode(() -> AuthorizationConveniences.authorizeOrThrow(authorizer, REQUEST))
        .doesNotThrowAnyException();
  }
}
