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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.polaris.authz.model.AuthorizationDecision;
import org.apache.polaris.authz.model.AuthorizationEvaluationException;
import org.apache.polaris.authz.model.AuthorizationRequest;
import org.apache.polaris.spi.substrate.PolarisDecisionAuthorizer;

/**
 * Provider-analog authorizer that delegates the WHOLE batch to a supplied remote engine in ONE
 * call. Because the contract is decision-native and batches AND-combine on the far side, a request
 * of any number of intents costs exactly one round-trip. Framework-free: only JDK types and the OSS
 * core contract.
 */
public class RemoteDecisionAuthorizer implements PolarisDecisionAuthorizer {

  private final Function<AuthorizationRequest, AuthorizationDecision> remoteEngine;
  private final AtomicInteger invocations = new AtomicInteger();

  public RemoteDecisionAuthorizer(
      Function<AuthorizationRequest, AuthorizationDecision> remoteEngine) {
    this.remoteEngine = Objects.requireNonNull(remoteEngine, "remoteEngine must be non-null");
  }

  @Override
  public AuthorizationDecision authorize(AuthorizationRequest request) {
    if (request == null) {
      throw new AuthorizationEvaluationException("request must be non-null");
    }
    invocations.incrementAndGet();
    AuthorizationDecision decision = remoteEngine.apply(request);
    if (decision == null) {
      throw new AuthorizationEvaluationException("remote engine returned no decision");
    }
    return decision;
  }

  /** Number of times the remote engine has been invoked; one per batch. */
  public int invocationCount() {
    return invocations.get();
  }
}
