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

package org.apache.polaris.core.exceptions;

import org.apache.polaris.exceptions.PolarisBadRequestException;

/**
 * Raised when a store declines a commit because the mutation list was larger than the per-commit
 * limit it declares. It is a {@link PolarisBadRequestException}, so the error mapper renders it as
 * HTTP 400 with the stable wire code {@code commit.too_many_items} and needs no dedicated arm.
 *
 * <p>Retrying the identical request cannot succeed. The caller has to split the work, and accept
 * that the parts are not atomic with each other. That is why this is not reported as a conflict: a
 * conflict invites the retry that will fail again.
 */
public class TooManyItemsException extends PolarisBadRequestException {
  private static final String ERROR_CODE = "commit.too_many_items";

  public TooManyItemsException(String message) {
    super(ERROR_CODE, message);
  }

  public TooManyItemsException(String message, Throwable cause) {
    super(ERROR_CODE, message, cause);
  }
}
