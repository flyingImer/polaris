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
package org.apache.polaris.core.persistence;

import com.google.errorprone.annotations.FormatMethod;
import org.apache.polaris.exceptions.PolarisConflictException;

/**
 * Exception raised when data is accessed concurrently with conflict.
 *
 * <p>It is a {@link PolarisConflictException}, so anywhere it reaches the error mapper it renders
 * as HTTP 409 with the stable wire code {@code entity.concurrency_conflict} and needs no dedicated
 * mapper arm. Before this it extended {@code RuntimeException} directly and fell through the mapper
 * to a 500. The {@code loadTasks} path in {@code AtomicOperationMetaStoreManager} throws it
 * uncaught, so that path now returns 409 instead of 500.
 */
public class RetryOnConcurrencyException extends PolarisConflictException {
  private static final String ERROR_CODE = "entity.concurrency_conflict";

  @FormatMethod
  public RetryOnConcurrencyException(String message, Object... args) {
    super(ERROR_CODE, String.format(message, args));
  }

  @FormatMethod
  public RetryOnConcurrencyException(Throwable cause, String message, Object... args) {
    super(ERROR_CODE, String.format(message, args), cause);
  }

  public RetryOnConcurrencyException(Throwable cause) {
    super(ERROR_CODE, cause == null ? null : cause.toString(), cause);
  }
}
