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
package org.apache.polaris.authz.model;

import java.util.List;
import java.util.Objects;

/**
 * A batch authorization request: one principal and one or more intents evaluated together as a
 * single AND-combined unit, so a remote authorizer can decide the whole batch in one round-trip.
 */
public record AuthorizationRequest(
    PolarisPrincipalRef principal, List<AuthorizationIntent> intents) {

  public AuthorizationRequest {
    Objects.requireNonNull(principal, "principal must be non-null");
    intents = List.copyOf(intents);
  }
}
