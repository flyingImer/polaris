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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

/**
 * Bounded policy for an explicitly replayable operation. The outermost caller owns the budget;
 * nested uses execute once. The supplier must recreate all required validation/authorization and
 * have no external effects. Neither stale client expectations nor UNKNOWN enter this loop.
 */
public final class ConfirmedConflictRetry {
  private static final ThreadLocal<Boolean> ACTIVE = new ThreadLocal<>();
  private static final int MAX_ATTEMPTS = 32;
  private static final long BUDGET_NANOS = TimeUnit.SECONDS.toNanos(2);

  private ConfirmedConflictRetry() {}

  public static <T> T run(Supplier<T> operation) {
    if (ACTIVE.get() != null) return operation.get();
    ACTIVE.set(true);
    long start = System.nanoTime();
    try {
      for (int attempt = 1; ; attempt++) {
        try {
          return operation.get();
        } catch (ConfirmedTransactionConflictException e) {
          long remaining = BUDGET_NANOS - (System.nanoTime() - start);
          if (attempt >= MAX_ATTEMPTS || remaining <= 0 || Thread.currentThread().isInterrupted())
            throw e;
          long delay =
              TimeUnit.MILLISECONDS.toNanos(
                  ThreadLocalRandom.current().nextLong(1, 1L << Math.min(attempt, 8)));
          LockSupport.parkNanos(Math.min(delay, remaining));
          if (Thread.currentThread().isInterrupted() || System.nanoTime() - start >= BUDGET_NANOS)
            throw e;
        }
      }
    } finally {
      ACTIVE.remove();
    }
  }
}
