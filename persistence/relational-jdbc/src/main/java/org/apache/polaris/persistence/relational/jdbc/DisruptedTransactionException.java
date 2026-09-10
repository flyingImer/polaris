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
package org.apache.polaris.persistence.relational.jdbc;

import java.sql.SQLException;
import org.apache.polaris.spi.durable.CommitDisruptedException.DurableEffect;

/**
 * A transaction that failed, carrying what this module can prove about the durable effect of the
 * statements it had issued.
 *
 * <p>It stays a {@link SQLException}, and keeps the original SQL state and vendor code, for two
 * reasons: every existing caller of the transaction helper catches {@code SQLException} and must
 * keep working unchanged, and the retry surface classifies by SQL state, which it can only do if the
 * state survives the wrap.
 *
 * <p>Only the transaction helper creates one, because it is the only place that knows where a
 * failure landed: before anything was issued, after statements were issued but the rollback
 * succeeded, or at a point where nothing can be proven either way. This type is how that knowledge
 * reaches the store without every other caller having to learn about it.
 */
class DisruptedTransactionException extends SQLException {

  private final DurableEffect durableEffect;

  DisruptedTransactionException(DurableEffect durableEffect, String message, SQLException cause) {
    super(message, cause.getSQLState(), cause.getErrorCode(), cause);
    this.durableEffect = durableEffect;
  }

  /** Re-wraps with a fuller message, keeping the effect this instance already established. */
  DisruptedTransactionException(DisruptedTransactionException original, String message) {
    super(message, original.getSQLState(), original.getErrorCode(), original);
    this.durableEffect = original.durableEffect;
  }

  DurableEffect durableEffect() {
    return durableEffect;
  }
}
