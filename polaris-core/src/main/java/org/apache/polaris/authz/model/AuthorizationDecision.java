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

import java.util.Objects;
import java.util.Optional;

/**
 * The result of an authorization request: allow or deny, with an optional human-readable message on
 * deny. A deny is a value, not a thrown exception, so a caller can inspect it, combine it, or defer
 * the throw to the edge of its choosing.
 */
public record AuthorizationDecision(boolean isAllowed, Optional<String> message) {

  public AuthorizationDecision {
    Objects.requireNonNull(message, "message must be non-null");
  }

  public static AuthorizationDecision allow() {
    return new AuthorizationDecision(true, Optional.empty());
  }

  public static AuthorizationDecision deny(String message) {
    return new AuthorizationDecision(false, Optional.of(message));
  }
}
