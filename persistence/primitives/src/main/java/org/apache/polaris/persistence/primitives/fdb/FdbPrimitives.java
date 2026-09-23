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
package org.apache.polaris.persistence.primitives.fdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.polaris.persistence.primitives.api.StorageFailure.Outcome.CONFLICT;
import static org.apache.polaris.persistence.primitives.api.StorageFailure.Outcome.REJECTED;
import static org.apache.polaris.persistence.primitives.api.StorageFailure.Outcome.UNKNOWN;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.apache.polaris.persistence.primitives.api.DurablePrimitives;
import org.apache.polaris.persistence.primitives.api.StorageFailure;

/** Uses ordinary conflict-tracked reads, never snapshot reads or Database.run retries. */
public final class FdbPrimitives implements DurablePrimitives {
  private final Database database;

  public FdbPrimitives(String clusterFile) {
    database = FDB.selectAPIVersion(730).open(clusterFile);
  }

  @Override
  public Attempt begin() {
    return beginLegacy();
  }

  @Override
  public LegacyAttempt beginLegacy() {
    return new Tx(database.createTransaction());
  }

  @Override
  public void close() {
    database.close();
  }

  private static final class Tx implements LegacyAttempt {
    private final Transaction transaction;
    private boolean finished;
    private boolean closed;

    Tx(Transaction transaction) {
      this.transaction = transaction;
      transaction.options().setTimeout(10000);
    }

    private void checkOpen() {
      if (finished) throw new IllegalStateException("Attempt finished");
    }

    private StorageFailure failure(RuntimeException error, boolean committing) {
      Throwable cause = error instanceof CompletionException ? error.getCause() : error;
      // Only documented definitely-aborted conflicts are replay candidates.
      int code = cause instanceof FDBException f ? f.getCode() : -1;
      return new StorageFailure(
          code == 1020 || code == 1007
              ? CONFLICT
              : code == 2101 || code == 2102 || code == 2103
                  ? REJECTED
                  : committing ? UNKNOWN : REJECTED,
          cause);
    }

    @Override
    public byte[] get(String key) {
      checkOpen();
      try {
        return transaction.get(key.getBytes(UTF_8)).join();
      } catch (RuntimeException e) {
        throw failure(e, false);
      }
    }

    @Override
    public List<Entry> scan(String begin, String end, int limit) {
      checkOpen();
      if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
      try {
        return transaction
            .getRange(begin.getBytes(UTF_8), end.getBytes(UTF_8), limit)
            .asList()
            .join()
            .stream()
            .map(kv -> new Entry(new String(kv.getKey(), UTF_8), kv.getValue()))
            .toList();
      } catch (RuntimeException e) {
        throw failure(e, false);
      }
    }

    @Override
    public void applyForLegacyReadYourWrites(List<Mutation> mutations) {
      checkOpen();
      try {
        for (var m : mutations) {
          if (m.end() != null) transaction.clear(m.key().getBytes(UTF_8), m.end().getBytes(UTF_8));
          else if (m.value() == null) transaction.clear(m.key().getBytes(UTF_8));
          else transaction.set(m.key().getBytes(UTF_8), m.value());
        }
      } catch (RuntimeException e) {
        throw failure(e, false);
      }
    }

    @Override
    public void commit(List<Mutation> mutations) {
      applyForLegacyReadYourWrites(mutations);
      finished = true;
      try {
        transaction.commit().join();
      } catch (RuntimeException e) {
        throw failure(e, true);
      }
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        finished = true;
        transaction.close();
      }
    }
  }
}
