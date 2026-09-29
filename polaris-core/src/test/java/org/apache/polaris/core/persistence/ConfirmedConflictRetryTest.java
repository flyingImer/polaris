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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ConfirmedConflictRetryTest {
  @Test
  void onlyConfirmedAbortIsReplayed() {
    AtomicInteger attempts = new AtomicInteger();
    assertThat(
            ConfirmedConflictRetry.run(
                () -> {
                  if (attempts.incrementAndGet() == 1)
                    throw new ConfirmedTransactionConflictException(new IllegalStateException());
                  return 42;
                }))
        .isEqualTo(42);
    assertThat(attempts).hasValue(2);
    for (RuntimeException failure :
        new RuntimeException[] {
          new RetryOnConcurrencyException("Client version changed"),
          new CommitRejectedException(new IllegalStateException()),
          new CommitOutcomeUnknownException(new IllegalStateException())
        }) {
      attempts.set(0);
      assertThatThrownBy(
              () ->
                  ConfirmedConflictRetry.run(
                      () -> {
                        attempts.incrementAndGet();
                        throw failure;
                      }))
          .isSameAs(failure);
      assertThat(attempts).hasValue(1);
    }
  }

  @Test
  void nestedOwnersDoNotMultiplyBudget() {
    AtomicInteger attempts = new AtomicInteger();
    assertThatThrownBy(
            () ->
                ConfirmedConflictRetry.run(
                    () ->
                        ConfirmedConflictRetry.run(
                            () -> {
                              attempts.incrementAndGet();
                              throw new ConfirmedTransactionConflictException(
                                  new IllegalStateException());
                            })))
        .isInstanceOf(ConfirmedTransactionConflictException.class);
    assertThat(attempts.get()).isBetween(1, 32);
  }

  @Test
  void interruptedConflictDoesNotStartAnotherAttempt() {
    AtomicInteger attempts = new AtomicInteger();
    try {
      assertThatThrownBy(
              () ->
                  ConfirmedConflictRetry.run(
                      () -> {
                        attempts.incrementAndGet();
                        Thread.currentThread().interrupt();
                        throw new ConfirmedTransactionConflictException(
                            new IllegalStateException());
                      }))
          .isInstanceOf(ConfirmedTransactionConflictException.class);
      assertThat(attempts).hasValue(1);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }
  }
}
