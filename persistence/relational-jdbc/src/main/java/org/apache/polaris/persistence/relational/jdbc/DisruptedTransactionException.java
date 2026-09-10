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
 * keep working unchanged, and the retry surface classifies by SQL state, which it can only do if
 * the state survives the wrap.
 *
 * <p><b>One behaviour change to disclose:</b> the retry surface also has a fallback that reads the
 * exception MESSAGE when the SQL state is null, and this wrap replaces the message with its own.
 * A stateless driver failure whose text named a refused or reset connection was retried before and
 * is not retried now, for transactions only; the other operations reach that fallback unchanged.
 * The direction is fewer whole-transaction replays, which is the safe side of the same hazard the
 * retry set's own rule names, so the wrap keeps its message and the change is recorded here rather
 * than worked around.
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

  /**
   * For a cause that carries no SQL state of its own, such as a rejected request that failed while
   * building a statement. With no state there is nothing for the retry surface to classify, which is
   * correct here: re-running the operation would fail the same way.
   */
  DisruptedTransactionException(DurableEffect durableEffect, String message, Throwable cause) {
    super(message, null, 0, cause);
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
