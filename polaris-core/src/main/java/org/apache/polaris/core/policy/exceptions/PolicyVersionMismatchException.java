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
package org.apache.polaris.core.policy.exceptions;

import org.apache.polaris.exceptions.PolarisConflictException;

/**
 * Thrown when a policy version does not match. It is a {@link PolarisConflictException}, mapped to
 * HTTP 409 with the stable wire code {@code policy.version_mismatch}.
 */
public class PolicyVersionMismatchException extends PolarisConflictException {
  private static final String ERROR_CODE = "policy.version_mismatch";

  public PolicyVersionMismatchException(String message) {
    super(ERROR_CODE, message);
  }

  public PolicyVersionMismatchException(String message, Throwable cause) {
    super(ERROR_CODE, message, cause);
  }
}
