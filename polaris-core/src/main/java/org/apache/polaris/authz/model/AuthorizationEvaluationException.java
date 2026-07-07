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

/**
 * Signals that an authorization request could NOT be evaluated: a malformed request, or an error
 * inside the authorizer. This is not how a deny is reported. A deny is a normal {@link
 * AuthorizationDecision} return value; only the inability to reach a decision throws.
 */
public class AuthorizationEvaluationException extends RuntimeException {

  public AuthorizationEvaluationException(String message) {
    super(message);
  }

  public AuthorizationEvaluationException(String message, Throwable cause) {
    super(message, cause);
  }
}
