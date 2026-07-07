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
package org.apache.polaris.spi.substrate;

import org.apache.polaris.authz.model.AuthorizationDecision;
import org.apache.polaris.authz.model.AuthorizationRequest;

/**
 * Decision-native authorization seam: evaluate an {@link AuthorizationRequest} and RETURN an
 * allow/deny {@link AuthorizationDecision}. Deny is a value, not a thrown exception.
 *
 * <p>A request carries one principal and one or more intents. The intents form ONE batch: the
 * implementation AND-combines them and may short-circuit on the first deny, so a remote authorizer
 * decides the whole batch in a single round-trip. Only an evaluation error or a malformed request
 * is signalled by throwing {@link org.apache.polaris.authz.model.AuthorizationEvaluationException}.
 */
public interface PolarisDecisionAuthorizer {

  AuthorizationDecision authorize(AuthorizationRequest request);
}
