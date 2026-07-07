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
package org.apache.polaris.forkless.provider.authz;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.authz.model.AuthorizationDecision;
import org.apache.polaris.authz.model.AuthorizationIntent;
import org.apache.polaris.authz.model.AuthorizationRequest;
import org.apache.polaris.authz.model.PolarisPrincipalRef;
import org.junit.jupiter.api.Test;

class RemoteDecisionAuthorizerTest {

  @Test
  void delegatesMultiIntentBatchInExactlyOneRemoteCall() {
    AtomicInteger remoteCalls = new AtomicInteger();
    RemoteDecisionAuthorizer authorizer =
        new RemoteDecisionAuthorizer(
            request -> {
              remoteCalls.incrementAndGet();
              return AuthorizationDecision.allow();
            });
    AuthorizationRequest batch =
        new AuthorizationRequest(
            new PolarisPrincipalRef("svc", List.of("writer")),
            List.of(
                new AuthorizationIntent.SingleTarget("SELECT", List.of("db", "a")),
                new AuthorizationIntent.SingleTarget("INSERT", List.of("db", "a")),
                new AuthorizationIntent.Rename("ALTER", List.of("db", "a"), List.of("db", "b"))));

    AuthorizationDecision decision = authorizer.authorize(batch);

    assertThat(decision.isAllowed()).isTrue();
    // Three intents, one round-trip: the whole batch is decided in a single remote call.
    assertThat(remoteCalls.get()).isEqualTo(1);
    assertThat(authorizer.invocationCount()).isEqualTo(1);
  }
}
