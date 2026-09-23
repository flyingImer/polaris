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
package org.apache.polaris.persistence.primitives.spanner;

import static com.google.cloud.spanner.Mutation.delete;
import static com.google.cloud.spanner.Mutation.newInsertOrUpdateBuilder;
import static org.apache.polaris.persistence.primitives.api.StorageFailure.Outcome.CONFLICT;
import static org.apache.polaris.persistence.primitives.api.StorageFailure.Outcome.REJECTED;
import static org.apache.polaris.persistence.primitives.api.StorageFailure.Outcome.UNKNOWN;

import com.google.cloud.ByteArray;
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionManager;
import com.google.spanner.v1.TransactionOptions.IsolationLevel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.polaris.persistence.primitives.api.DurablePrimitives;
import org.apache.polaris.persistence.primitives.api.StorageFailure;

/** Explicit emulator connection for this PoC; no ambient credentials or project discovery. */
public final class SpannerPrimitives implements DurablePrimitives {
  private final Spanner service;
  private final DatabaseClient client;

  public SpannerPrimitives(String endpoint, String database) {
    if (!(endpoint.startsWith("127.0.0.1:") || endpoint.startsWith("localhost:"))) {
      throw new IllegalArgumentException("This prototype accepts only a local emulator endpoint");
    }
    DatabaseId id = DatabaseId.of(database);
    service =
        SpannerOptions.newBuilder()
            .setProjectId(id.getInstanceId().getProject())
            .setCredentials(NoCredentials.getInstance())
            .setEmulatorHost(endpoint)
            .setBuiltInMetricsEnabled(false)
            .setGrpcGcpOtelMetricsEnabled(false)
            .build()
            .getService();
    client = service.getDatabaseClient(id);
  }

  /** Explicit setup for a disposable local emulator, never invoked by runtime requests. */
  public static void initializeEmulator(String endpoint, String database) {
    DatabaseId id = DatabaseId.of(database);
    try (var backend = new SpannerPrimitives(endpoint, database)) {
      try {
        var info =
            InstanceInfo.newBuilder(id.getInstanceId())
                .setInstanceConfigId(
                    InstanceConfigId.of(id.getInstanceId().getProject(), "emulator-config"))
                .setDisplayName("Polaris Java primitives PoC")
                .setNodeCount(1)
                .build();
        backend.service.getInstanceAdminClient().createInstance(info).get();
        backend
            .service
            .getDatabaseAdminClient()
            .createDatabase(
                id.getInstanceId().getInstance(),
                id.getDatabase(),
                List.of(
                    "CREATE TABLE PolarisPocRecords (k STRING(4096) NOT NULL, v BYTES(MAX) NOT NULL) PRIMARY KEY (k)"))
            .get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      } catch (ExecutionException e) {
        throw new IllegalStateException(e.getCause());
      }
    }
  }

  @Override
  public Attempt begin() {
    return new Tx(
        client.transactionManager(Options.isolationLevel(IsolationLevel.SERIALIZABLE)), false);
  }

  @Override
  public LegacyAttempt beginLegacy() {
    return new Tx(
        client.transactionManager(Options.isolationLevel(IsolationLevel.SERIALIZABLE)), true);
  }

  @Override
  public void close() {
    service.close();
  }

  private static final class Tx implements LegacyAttempt {
    private final TransactionManager manager;
    private final TransactionContext tx;
    private final boolean legacy;
    private boolean finished;
    private boolean closed;

    Tx(TransactionManager manager, boolean legacy) {
      this.manager = manager;
      this.legacy = legacy;
      this.tx = manager.begin();
    }

    private void checkOpen() {
      if (finished) throw new IllegalStateException("Attempt finished");
    }

    private StorageFailure failure(SpannerException e, boolean committing) {
      return new StorageFailure(
          e.getErrorCode() == ErrorCode.ABORTED
              ? CONFLICT
              : committing && e.getErrorCode() != ErrorCode.INVALID_ARGUMENT ? UNKNOWN : REJECTED,
          e);
    }

    @Override
    public byte[] get(String key) {
      checkOpen();
      try {
        Struct row = tx.readRow("PolarisPocRecords", Key.of(key), List.of("v"));
        return row == null ? null : row.getBytes(0).toByteArray();
      } catch (SpannerException e) {
        throw failure(e, false);
      }
    }

    @Override
    public List<Entry> scan(String begin, String end, int limit) {
      checkOpen();
      if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
      var statement =
          Statement.newBuilder(
                  "SELECT k, v FROM PolarisPocRecords WHERE k >= @lo AND k < @hi ORDER BY k LIMIT @n")
              .bind("lo")
              .to(begin)
              .bind("hi")
              .to(end)
              .bind("n")
              .to(limit)
              .build();
      try (var r = tx.executeQuery(statement)) {
        List<Entry> result = new ArrayList<>();
        while (r.next()) result.add(new Entry(r.getString(0), r.getBytes(1).toByteArray()));
        return result;
      } catch (SpannerException e) {
        throw failure(e, false);
      }
    }

    @Override
    public void applyForLegacyReadYourWrites(List<DurablePrimitives.Mutation> mutations) {
      checkOpen();
      if (!legacy)
        throw new IllegalStateException("Terminal batch attempt cannot write before commit");
      try {
        for (var m : mutations) {
          if (m.end() != null) {
            tx.executeUpdate(
                Statement.newBuilder("DELETE FROM PolarisPocRecords WHERE k >= @lo AND k < @hi")
                    .bind("lo")
                    .to(m.key())
                    .bind("hi")
                    .to(m.end())
                    .build());
          } else if (m.value() == null) {
            tx.executeUpdate(
                Statement.newBuilder("DELETE FROM PolarisPocRecords WHERE k = @k")
                    .bind("k")
                    .to(m.key())
                    .build());
          } else {
            long updated =
                tx.executeUpdate(
                    Statement.newBuilder("UPDATE PolarisPocRecords SET v = @v WHERE k = @k")
                        .bind("k")
                        .to(m.key())
                        .bind("v")
                        .to(ByteArray.copyFrom(m.value()))
                        .build());
            if (updated == 0)
              tx.executeUpdate(
                  Statement.newBuilder("INSERT INTO PolarisPocRecords (k, v) VALUES (@k, @v)")
                      .bind("k")
                      .to(m.key())
                      .bind("v")
                      .to(ByteArray.copyFrom(m.value()))
                      .build());
          }
        }
      } catch (SpannerException e) {
        throw failure(e, false);
      }
    }

    @Override
    public void commit(List<DurablePrimitives.Mutation> mutations) {
      checkOpen();
      if (legacy) applyForLegacyReadYourWrites(mutations);
      else {
        for (var m : mutations) {
          var nativeMutation =
              m.end() != null
                  ? delete(
                      "PolarisPocRecords",
                      KeySet.range(KeyRange.closedOpen(Key.of(m.key()), Key.of(m.end()))))
                  : m.value() == null
                      ? delete("PolarisPocRecords", Key.of(m.key()))
                      : newInsertOrUpdateBuilder("PolarisPocRecords")
                          .set("k")
                          .to(m.key())
                          .set("v")
                          .to(ByteArray.copyFrom(m.value()))
                          .build();
          tx.buffer(nativeMutation);
        }
      }
      finished = true;
      try {
        manager.commit();
      } catch (SpannerException e) {
        throw failure(e, true);
      }
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        finished = true;
        manager.close();
      }
    }
  }
}
