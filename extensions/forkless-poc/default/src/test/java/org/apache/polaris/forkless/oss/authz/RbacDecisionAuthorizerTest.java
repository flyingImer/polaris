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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Set;
import org.apache.polaris.authz.model.AuthorizationDecision;
import org.apache.polaris.authz.model.AuthorizationEvaluationException;
import org.apache.polaris.authz.model.AuthorizationIntent;
import org.apache.polaris.authz.model.AuthorizationRequest;
import org.apache.polaris.authz.model.PolarisPrincipalRef;
import org.apache.polaris.forkless.oss.authz.RbacDecisionAuthorizer.Grant;
import org.junit.jupiter.api.Test;

class RbacDecisionAuthorizerTest {

  private static final PolarisPrincipalRef ALICE =
      new PolarisPrincipalRef("alice", List.of("analyst"));

  @Test
  void allowsWhenEveryIntentIsGranted() {
    RbacDecisionAuthorizer authorizer =
        new RbacDecisionAuthorizer(
            Set.of(
                new Grant("analyst", "SELECT", List.of("db", "sales")), // via role
                new Grant("alice", "CREATE_TABLE", List.of()))); // via principal, targetless
    AuthorizationRequest request =
        new AuthorizationRequest(
            ALICE,
            List.of(
                new AuthorizationIntent.SingleTarget("SELECT", List.of("db", "sales")),
                new AuthorizationIntent.Targetless("CREATE_TABLE")));

    AuthorizationDecision decision = authorizer.authorize(request);

    assertThat(decision.isAllowed()).isTrue();
    assertThat(decision.message()).isEmpty();
  }

  @Test
  void deniesWholeBatchAndShortCircuitsOnFirstDeny() {
    // No grants: the first intent is denied. The second intent has a blank privilege, which the
    // authorizer treats as a bad request and would THROW on if it were ever evaluated.
    RbacDecisionAuthorizer authorizer = new RbacDecisionAuthorizer(Set.of());
    AuthorizationIntent denied =
        new AuthorizationIntent.SingleTarget("SELECT", List.of("db", "sales"));
    AuthorizationIntent wouldThrow = new AuthorizationIntent.Targetless("");

    // Proof the second intent WOULD throw if reached: evaluate it on its own.
    assertThatThrownBy(
            () -> authorizer.authorize(new AuthorizationRequest(ALICE, List.of(wouldThrow))))
        .isInstanceOf(AuthorizationEvaluationException.class);

    // In a batch AFTER a deny, that second intent is never reached: a clean deny, no throw. That is
    // AND-combine short-circuiting on the first deny.
    AuthorizationDecision decision =
        authorizer.authorize(new AuthorizationRequest(ALICE, List.of(denied, wouldThrow)));

    assertThat(decision.isAllowed()).isFalse();
    assertThat(decision.message()).isPresent();
  }
}
