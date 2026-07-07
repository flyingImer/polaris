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
package org.apache.polaris.exceptions;

import org.apache.polaris.core.exceptions.PolarisException;

/**
 * HTTP-semantic base for "bad request" errors (the mapper turns this base into 400). Carries a
 * stable declared error code supplied at construction and exposed via {@link #errorCode()}; the
 * wire type is that code, never the class simple name.
 */
public class PolarisBadRequestException extends PolarisException {

  private final String errorCode;

  public PolarisBadRequestException(String errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  public PolarisBadRequestException(String errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  public String errorCode() {
    return errorCode;
  }
}
