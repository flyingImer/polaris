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
package org.apache.polaris.core.policy.validator;

import org.apache.polaris.exceptions.PolarisBadRequestException;

/**
 * Thrown when a policy is invalid or violates defined rules. It is a {@link
 * PolarisBadRequestException}, mapped to HTTP 400 with the stable wire code {@code policy.invalid}.
 */
public class InvalidPolicyException extends PolarisBadRequestException {
  private static final String ERROR_CODE = "policy.invalid";

  public InvalidPolicyException(String message) {
    super(ERROR_CODE, message);
  }

  public InvalidPolicyException(String message, Throwable cause) {
    super(ERROR_CODE, message, cause);
  }

  public InvalidPolicyException(Throwable cause) {
    super(ERROR_CODE, "Invalid policy", cause);
  }
}
