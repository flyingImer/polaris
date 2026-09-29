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
package org.apache.polaris.persistence.primitives.jdbc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.polaris.persistence.primitives.api.StorageFailure.Outcome.CONFLICT;
import static org.apache.polaris.persistence.primitives.api.StorageFailure.Outcome.REJECTED;
import static org.apache.polaris.persistence.primitives.api.StorageFailure.Outcome.UNKNOWN;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.apache.polaris.persistence.primitives.api.DurablePrimitives;
import org.apache.polaris.persistence.primitives.api.StorageFailure;

/** PostgreSQL and CockroachDB use the same generic record adapter. */
public final class JdbcPrimitives implements DurablePrimitives {
  private final String url;
  private final Properties properties;

  public JdbcPrimitives(String url, Properties properties) {
    this.url = url;
    this.properties = new Properties();
    this.properties.putAll(properties);
  }

  /** Explicit PoC schema bootstrap, separate from metadata transactions. */
  public void initialize() {
    try (var c = connect();
        var s = c.createStatement()) {
      s.execute(
          "CREATE TABLE IF NOT EXISTS polaris_poc_records (k BYTEA PRIMARY KEY, v BYTEA NOT NULL)");
    } catch (SQLException e) {
      throw new StorageFailure(REJECTED, e);
    }
  }

  private Connection connect() throws SQLException {
    return DriverManager.getConnection(url, properties);
  }

  @Override
  public Attempt begin() {
    try {
      return new Tx(connect());
    } catch (SQLException e) {
      throw new StorageFailure(REJECTED, e);
    }
  }

  private static final class Tx implements Attempt {
    private final Connection connection;
    private final boolean h2;
    private boolean finished;

    Tx(Connection connection) throws SQLException {
      this.connection = connection;
      this.h2 = connection.getMetaData().getDatabaseProductName().equals("H2");
      try {
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        connection.setAutoCommit(false);
      } catch (SQLException e) {
        connection.close();
        throw e;
      }
    }

    private void checkOpen() {
      if (finished) throw new IllegalStateException("Attempt finished");
    }

    private StorageFailure failure(SQLException e, boolean committing) {
      String state = e.getSQLState();
      return new StorageFailure(
          "40001".equals(state) || "40P01".equals(state)
              ? CONFLICT
              : committing ? UNKNOWN : REJECTED,
          e);
    }

    @Override
    public byte[] get(String key) {
      checkOpen();
      try (var s = connection.prepareStatement("SELECT v FROM polaris_poc_records WHERE k = ?")) {
        s.setBytes(1, key.getBytes(UTF_8));
        try (var r = s.executeQuery()) {
          return r.next() ? r.getBytes(1) : null;
        }
      } catch (SQLException e) {
        throw failure(e, false);
      }
    }

    @Override
    public List<byte[]> getMany(List<String> keys) {
      checkOpen();
      var values = new HashMap<String, byte[]>();
      for (int from = 0; from < keys.size(); from += 500) {
        var chunk = keys.subList(from, Math.min(from + 500, keys.size()));
        String sql =
            "SELECT k, v FROM polaris_poc_records WHERE k IN ("
                + String.join(",", Collections.nCopies(chunk.size(), "?"))
                + ")";
        try (var statement = connection.prepareStatement(sql)) {
          for (int i = 0; i < chunk.size(); i++)
            statement.setBytes(i + 1, chunk.get(i).getBytes(UTF_8));
          try (var rows = statement.executeQuery()) {
            while (rows.next()) values.put(new String(rows.getBytes(1), UTF_8), rows.getBytes(2));
          }
        } catch (SQLException e) {
          throw failure(e, false);
        }
      }
      return keys.stream().map(values::get).toList();
    }

    @Override
    public List<Entry> scan(String begin, String end, int limit) {
      checkOpen();
      if (limit <= 0) throw new IllegalArgumentException("limit must be positive");
      try (var s =
          connection.prepareStatement(
              "SELECT k, v FROM polaris_poc_records WHERE k >= ? AND k < ? ORDER BY k LIMIT ?")) {
        s.setBytes(1, begin.getBytes(UTF_8));
        s.setBytes(2, end.getBytes(UTF_8));
        s.setInt(3, limit);
        List<Entry> result = new ArrayList<>();
        try (var r = s.executeQuery()) {
          while (r.next()) result.add(new Entry(new String(r.getBytes(1), UTF_8), r.getBytes(2)));
        }
        return result;
      } catch (SQLException e) {
        throw failure(e, false);
      }
    }

    private void apply(List<Mutation> mutations) {
      checkOpen();
      try {
        // Preserve mutation order, including overlapping point/range changes. Batch only adjacent
        // statements of the same shape. Each batch remains on the same JDBC transaction.
        for (int from = 0; from < mutations.size(); ) {
          String sql = mutationSql(mutations.get(from));
          int to = from + 1;
          while (to < mutations.size()
              && to - from < 500
              && mutationSql(mutations.get(to)).equals(sql)) to++;
          try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (var mutation : mutations.subList(from, to)) {
              statement.setBytes(1, mutation.key().getBytes(UTF_8));
              if (mutation.end() != null) statement.setBytes(2, mutation.end().getBytes(UTF_8));
              else if (mutation.value() != null) statement.setBytes(2, mutation.value());
              statement.addBatch();
            }
            statement.executeBatch();
          }
          from = to;
        }
      } catch (SQLException e) {
        throw failure(e, false);
      }
    }

    private String mutationSql(Mutation mutation) {
      if (mutation.end() != null) return "DELETE FROM polaris_poc_records WHERE k >= ? AND k < ?";
      if (mutation.value() == null) return "DELETE FROM polaris_poc_records WHERE k = ?";
      return h2
          ? "MERGE INTO polaris_poc_records (k, v) KEY(k) VALUES (?, ?)"
          : "INSERT INTO polaris_poc_records (k, v) VALUES (?, ?) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v";
    }

    @Override
    public void commit(List<Mutation> mutations) {
      checkOpen();
      apply(mutations);
      finished = true;
      try {
        connection.commit();
      } catch (SQLException e) {
        throw failure(e, true);
      }
    }

    @Override
    public void close() {
      try {
        if (!connection.isClosed()) {
          try {
            if (!finished) connection.rollback();
          } finally {
            connection.close();
          }
        }
      } catch (SQLException e) {
        // Close never changes an already reported commit outcome.
      } finally {
        finished = true;
      }
    }
  }
}
