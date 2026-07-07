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

import com.google.errorprone.annotations.FormatMethod;
import org.apache.polaris.exceptions.PolarisBadRequestException;

/**
 * Thrown when a policy cannot be attached. It is a {@link PolarisBadRequestException}, mapped to
 * HTTP 400 with the stable wire code {@code policy.attach_failed}.
 */
public class PolicyAttachException extends PolarisBadRequestException {
  private static final String ERROR_CODE = "policy.attach_failed";

  @FormatMethod
  public PolicyAttachException(String message, Object... args) {
    super(ERROR_CODE, String.format(message, args));
  }

  @FormatMethod
  public PolicyAttachException(Throwable cause, String message, Object... args) {
    super(ERROR_CODE, String.format(message, args), cause);
  }
}
