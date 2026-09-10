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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.apache.polaris.core.persistence.EntityAlreadyExistsException;
import org.apache.polaris.persistence.relational.jdbc.QueryGenerator.PreparedQuery;
import org.apache.polaris.persistence.relational.jdbc.models.Converter;
import org.apache.polaris.spi.durable.CommitDisruptedException.DurableEffect;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatasourceOperations {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasourceOperations.class);

  // PG STATUS CODES
  // 23505 = unique key violation, consistent across PG/Cockroach/H2; other checks (FK, NOT NULL,
  // CHECK) all propagate
  private static final String UNIQUENESS_CONSTRAINT_VIOLATION_SQL_CODE = "23505";
  private static final String RELATION_DOES_NOT_EXIST = "42P01";

  // H2 STATUS CODES
  // 90079 = Schema not found, 42S02 = Table or view not found
  private static final String H2_SCHEMA_DOES_NOT_EXIST = "90079";
  private static final String H2_TABLE_DOES_NOT_EXIST = "42S02";

  // POSTGRES RETRYABLE EXCEPTIONS
  private static final String SERIALIZATION_FAILURE_SQL_CODE = "40001";

  private final DataSource datasource;
  private final RelationalJdbcConfiguration relationalJdbcConfiguration;
  private final DatabaseType databaseType;

  private static final Random random = new Random();

  public DatasourceOperations(
      DataSource datasource, RelationalJdbcConfiguration relationalJdbcConfiguration) {
    this.datasource = datasource;
    this.relationalJdbcConfiguration = relationalJdbcConfiguration;
    try (Connection connection = this.datasource.getConnection()) {
      // Get explicitly configured database type, if any
      DatabaseType configuredType =
          relationalJdbcConfiguration
              .databaseType()
              .map(DatabaseType::fromDisplayName)
              .orElse(null);

      // Infer database type from connection, falling back to configured type
      this.databaseType = DatabaseType.inferFromConnection(connection, configuredType);

      LOGGER.info("Detected database type: {}", databaseType);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to initialize DatasourceOperations", e);
    }
  }

  DatabaseType getDatabaseType() {
    return databaseType;
  }

  /**
   * Execute SQL script and close the associated input stream
   *
   * @param scriptInputStream : Input stream containing the SQL script.
   * @throws SQLException : Exception while executing the script.
   */
  public void executeScript(InputStream scriptInputStream) throws SQLException {
    try (BufferedReader scriptReader =
        new BufferedReader(
            new InputStreamReader(Objects.requireNonNull(scriptInputStream), UTF_8))) {
      List<String> scriptLines = scriptReader.lines().toList();
      runWithinTransaction(
          connection -> {
            try (Statement statement = connection.createStatement()) {
              StringBuilder sqlBuffer = new StringBuilder();
              for (String line : scriptLines) {
                line = line.trim();
                if (!line.isEmpty() && !line.startsWith("--")) { // Ignore empty lines and comments
                  sqlBuffer.append(line).append("\n");
                  if (line.endsWith(";")) { // Execute statement when semicolon is found
                    String sql = sqlBuffer.toString().trim();
                    try {
                      // since SQL is directly read from the file, there is close to 0 possibility
                      // of this being injected plus this run via an Admin tool, if attacker can
                      // fiddle with this that means lot of other things are already compromised.
                      statement.execute(sql);
                    } catch (SQLException e) {
                      throw new RuntimeException(e);
                    }
                    sqlBuffer.setLength(0); // Clear the buffer for the next statement
                  }
                }
              }
              return true;
            }
          });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Executes SELECT Query and returns the results after applying a transformer
   *
   * @param query : Query to executed
   * @param converterInstance : An instance of the type being selected, used to convert to a
   *     business entity like PolarisBaseEntity
   * @return The list of results yielded by the query
   * @param <T> : Business entity class
   * @throws SQLException : Exception during the query execution.
   */
  public <T> List<T> executeSelect(
      @NonNull PreparedQuery query, @NonNull Converter<T> converterInstance) throws SQLException {
    ArrayList<T> results = new ArrayList<>();
    executeSelectOverStream(query, converterInstance, stream -> stream.forEach(results::add));
    return results;
  }

  /**
   * Executes SELECT Query and takes a consumer over the results. For callers that want more
   * sophisticated control over how query results are handled.
   *
   * @param query : Query to executed
   * @param converterInstance : An entity of the type being selected
   * @param consumer : An function to consume the returned results
   * @param <T> : Entity class
   * @throws SQLException : Exception during the query execution.
   */
  public <T> void executeSelectOverStream(
      @NonNull PreparedQuery query,
      @NonNull Converter<T> converterInstance,
      @NonNull Consumer<Stream<T>> consumer)
      throws SQLException {
    withRetries(
        () -> {
          try (Connection connection = borrowConnection()) {
            executeSelectOverStreamWithConnection(query, converterInstance, consumer, connection);
            return null;
          }
        });
  }

  /** Connection-aware version for use inside runWithinTransaction. */
  public <T> void executeSelectOverStream(
      @NonNull Connection connection,
      @NonNull PreparedQuery query,
      @NonNull Converter<T> converterInstance,
      @NonNull Consumer<Stream<T>> consumer)
      throws SQLException {
    withRetries(
        () -> {
          executeSelectOverStreamWithConnection(query, converterInstance, consumer, connection);
          return null;
        });
  }

  /**
   * Internal implementation that executes the SELECT on the provided connection. Does not manage
   * connection lifecycle or retries.
   */
  private <T> void executeSelectOverStreamWithConnection(
      @NonNull PreparedQuery query,
      @NonNull Converter<T> converterInstance,
      @NonNull Consumer<Stream<T>> consumer,
      @NonNull Connection connection)
      throws SQLException {
    logQuery(query);
    try (PreparedStatement statement = connection.prepareStatement(query.sql())) {
      List<Object> params = query.parameters();
      for (int i = 0; i < params.size(); i++) {
        statement.setObject(i + 1, params.get(i));
      }
      try (ResultSet resultSet = statement.executeQuery()) {
        ResultSetIterator<T> iterator = new ResultSetIterator<>(resultSet, converterInstance);
        consumer.accept(iterator.toStream());
      }
    }
  }

  /** Connection-aware version for use inside runWithinTransaction. */
  public <T> List<T> executeSelect(
      @NonNull Connection connection,
      @NonNull PreparedQuery query,
      @NonNull Converter<T> converterInstance)
      throws SQLException {
    ArrayList<T> results = new ArrayList<>();
    executeSelectOverStream(
        connection, query, converterInstance, stream -> stream.forEach(results::add));
    return results;
  }

  /**
   * Executes the UPDATE or INSERT Query
   *
   * @param preparedQuery : query to be executed
   * @return : Number of rows modified / inserted.
   * @throws SQLException : Exception during Query Execution.
   */
  public int executeUpdate(QueryGenerator.PreparedQuery preparedQuery) throws SQLException {
    return withRetries(
        () -> {
          logQuery(preparedQuery);
          try (Connection connection = borrowConnection();
              PreparedStatement statement = connection.prepareStatement(preparedQuery.sql())) {
            List<Object> params = preparedQuery.parameters();
            for (int i = 0; i < params.size(); i++) {
              statement.setObject(i + 1, params.get(i));
            }
            boolean autoCommit = connection.getAutoCommit();
            connection.setAutoCommit(true);
            try {
              return statement.executeUpdate();
            } finally {
              connection.setAutoCommit(autoCommit);
            }
          }
        });
  }

  /**
   * Executes the INSERT/UPDATE Queries in batches. Requires that all SQL queries have the same
   * parameterized form.
   *
   * @param preparedQueries : queries to be executed
   * @return : Number of rows modified / inserted.
   * @throws SQLException : Exception during Query Execution.
   */
  public int executeBatchUpdate(QueryGenerator.PreparedBatchQuery preparedQueries)
      throws SQLException {
    if (preparedQueries.parametersList().isEmpty() || preparedQueries.sql().isEmpty()) {
      return 0;
    }
    int batchSize = 100;
    AtomicInteger successCount = new AtomicInteger();
    return withRetries(
        () -> {
          try (Connection connection = borrowConnection();
              PreparedStatement statement = connection.prepareStatement(preparedQueries.sql())) {
            boolean autoCommit = connection.getAutoCommit();
            boolean success = false;
            connection.setAutoCommit(false);

            try {
              for (int i = 1; i <= preparedQueries.parametersList().size(); i++) {
                List<Object> params = preparedQueries.parametersList().get(i - 1);
                for (int j = 0; j < params.size(); j++) {
                  statement.setObject(j + 1, params.get(j));
                }

                statement.addBatch(); // Add to batch

                if (i % batchSize == 0) {
                  successCount.addAndGet(Arrays.stream(statement.executeBatch()).sum());
                }
              }

              // Execute remaining queries in the batch
              successCount.addAndGet(Arrays.stream(statement.executeBatch()).sum());
              success = true;
            } finally {
              try {
                if (success) {
                  connection.commit();
                } else {
                  connection.rollback();
                  successCount.set(0);
                }
              } finally {
                connection.setAutoCommit(autoCommit);
              }
            }
          }
          return successCount.get();
        });
  }

  /**
   * Transaction callback to be executed.
   *
   * @param callback : TransactionCallback to be executed within transaction
   * @throws SQLException : Exception caught during transaction execution.
   */
  public void runWithinTransaction(TransactionCallback callback) throws SQLException {
    withRetries(
        () -> {
          Connection borrowed;
          try {
            borrowed = borrowConnection();
          } catch (SQLException e) {
            // Nothing was issued, so nothing can be in storage.
            throw new DisruptedTransactionException(
                DurableEffect.NONE, "Failed to acquire a connection", e);
          }
          try (Connection connection = borrowed) {
            boolean autoCommit;
            try {
              autoCommit = connection.getAutoCommit();
              connection.setAutoCommit(false);
            } catch (SQLException e) {
              throw new DisruptedTransactionException(
                  DurableEffect.NONE, "Failed to start a transaction", e);
            }
            // What the transaction body established, kept so the restore below can never recompute
            // it. An effect is a verdict: once reached it is reported as reached, and a later
            // failure adds information to it rather than replacing it.
            SQLException failure = null;
            RuntimeException escaping = null;
            Error fatal = null;
            DurableEffect finishedEffect = DurableEffect.NONE;
            try {
              // Committed: the statements are in storage, so any later failure leaves them there.
              // Declined: the body rolled back, so nothing is.
              finishedEffect =
                  runAndFinish(callback, connection) ? DurableEffect.UNKNOWN : DurableEffect.NONE;
            } catch (SQLException e) {
              // Every SQLException, not only the classified ones: the restore below has to run on
              // every exit of the body, because a connection handed back with auto-commit off is a
              // connection whose next reset decides an open transaction. The body classifies all of
              // its own failures today, which makes this the guard against it ever stopping.
              failure = e;
            } catch (RuntimeException e) {
              escaping = e;
            } catch (Error e) {
              // An Error is not this class's to interpret, but the connection it was thrown over is
              // still this class's to hand back in the state it was borrowed in.
              fatal = e;
            }
            try {
              connection.setAutoCommit(autoCommit);
            } catch (SQLException e) {
              // Restoring the connection is the last step, and it cannot revise what the body
              // already established. It only ever attaches itself to that verdict.
              if (failure != null) {
                failure.addSuppressed(e);
                throw failure;
              }
              if (escaping != null) {
                escaping.addSuppressed(e);
                throw escaping;
              }
              if (fatal != null) {
                fatal.addSuppressed(e);
                throw fatal;
              }
              throw new DisruptedTransactionException(
                  finishedEffect, "Failed to restore the connection's auto-commit state", e);
            }
            if (failure != null) {
              throw failure;
            }
            if (escaping != null) {
              throw escaping;
            }
            if (fatal != null) {
              throw fatal;
            }
          }
          return null;
        });
  }

  /**
   * Runs the callback and finishes its transaction, classifying every failure by what it leaves in
   * storage, and reporting whether the transaction was committed.
   *
   * <p>A rollback that succeeds proves the statements did not survive, whatever kind of failure
   * asked for it. A commit that fails proves nothing either way, because every statement reached
   * the server before it. A rollback that fails proves nothing either way for a different reason:
   * the commit was never issued, so nothing asked the server to keep the statements, but the fate
   * of the transaction the failed rollback leaves open is now the server's and the next mode
   * reset's, so the store reports UNKNOWN under its pessimism clause rather than claiming NONE.
   *
   * <p>Every exit issues a commit or a rollback first, because the connection returns to a pool
   * whose own mode restore would otherwise commit whatever was still in flight. That holds for what
   * the callback throws as well as for what it returns, an {@link Error} included. One exit has
   * nothing left to issue: a rollback that itself failed, which is the UNKNOWN above.
   *
   * @return true when the transaction was committed, false when the callback declined it and it was
   *     rolled back
   */
  private boolean runAndFinish(TransactionCallback callback, Connection connection)
      throws SQLException {
    boolean success;
    try {
      success = callback.execute(connection);
    } catch (SQLException e) {
      throw rollBackAfter(connection, e);
    } catch (RuntimeException e) {
      // A malformed request is reported by type, not as a disruption, so the exception is passed
      // through once the statements it issued before failing have been rolled back.
      try {
        connection.rollback();
      } catch (SQLException rollbackFailure) {
        DisruptedTransactionException disrupted =
            new DisruptedTransactionException(
                DurableEffect.UNKNOWN, "Transaction failed and its rollback failed", e);
        disrupted.addSuppressed(rollbackFailure);
        throw disrupted;
      }
      throw e;
    } catch (Error e) {
      // The rollback is owed whatever asked for it, and an Error asks for it too: the statements
      // the callback issued sit in an open transaction, and leaving them there hands them to the
      // pool's mode reset. The Error keeps its own type, because nothing above reads a durable
      // effect off one, and a rollback that fails travels with it rather than replacing it.
      try {
        connection.rollback();
      } catch (SQLException rollbackFailure) {
        e.addSuppressed(rollbackFailure);
      }
      throw e;
    }
    if (!success) {
      // The callback declined: a rejection, not a disruption. Its own reason is the caller's, so
      // long as the rollback that makes the rejection true actually runs. When it does not, the
      // statements the declined body issued are still in an open transaction, and a rejection is
      // no longer what happened.
      try {
        connection.rollback();
      } catch (SQLException rollbackFailure) {
        throw new DisruptedTransactionException(
            DurableEffect.UNKNOWN, "Failed to roll back a declined transaction", rollbackFailure);
      }
      return false;
    }
    try {
      connection.commit();
    } catch (SQLException e) {
      // The statements all reached the server before this call, and nothing here can ask the
      // server whether it applied them, so the effect stays unknown whatever happens next. The
      // rollback is still attempted: the transaction may be open, and leaving it open would hand
      // the decision to the auto-commit restore below, which commits whatever it finds.
      DisruptedTransactionException disrupted =
          new DisruptedTransactionException(
              DurableEffect.UNKNOWN, "Failed to commit the transaction", e);
      try {
        connection.rollback();
      } catch (SQLException rollbackFailure) {
        disrupted.addSuppressed(rollbackFailure);
      }
      throw disrupted;
    }
    return true;
  }

  /** Rolls back after a failed callback and says what that leaves in storage. */
  private DisruptedTransactionException rollBackAfter(Connection connection, SQLException cause) {
    try {
      connection.rollback();
    } catch (SQLException rollbackFailure) {
      DisruptedTransactionException disrupted =
          new DisruptedTransactionException(
              DurableEffect.UNKNOWN, "Transaction failed and its rollback failed", cause);
      disrupted.addSuppressed(rollbackFailure);
      return disrupted;
    }
    return new DisruptedTransactionException(
        DurableEffect.NONE, "Transaction failed and was rolled back", cause);
  }

  public Integer execute(Connection connection, QueryGenerator.PreparedQuery preparedQuery)
      throws SQLException {
    logQuery(preparedQuery);
    try (PreparedStatement statement = connection.prepareStatement(preparedQuery.sql())) {
      List<Object> params = preparedQuery.parameters();
      for (int i = 0; i < params.size(); i++) {
        statement.setObject(i + 1, params.get(i));
      }
      return statement.executeUpdate();
    }
  }

  /**
   * Whether the operation may be run again from the top.
   *
   * <p><b>Adding a SQL state here is not a local change.</b> A retry re-runs the whole operation on
   * a fresh connection, so a state may only be listed once it is established that the failure it
   * names leaves nothing in storage. Any change to this set therefore states, in the same change,
   * which durable effect the new state carries; otherwise an operation that may already have
   * applied gets replayed.
   *
   * <p>That bar is met today by the one listed state, whose semantics make an aborted transaction
   * apply nothing. It is NOT established for the fallback below, which reads the message when a
   * driver supplies no state: a reset connection reported that way can be a reset during a commit,
   * which proves nothing about storage. That fallback predates this classification and still serves
   * the single-statement and batch paths, where the same replay hazard therefore remains open.
   */
  private boolean isRetryable(SQLException e) {
    String sqlState = e.getSQLState();

    if (sqlState != null) {
      return sqlState.equals(SERIALIZATION_FAILURE_SQL_CODE); // Serialization failure
    }

    // Additionally, one might check for specific error messages or other conditions
    return e.getMessage().toLowerCase(Locale.ROOT).contains("connection refused")
        || e.getMessage().toLowerCase(Locale.ROOT).contains("connection reset");
  }

  // TODO: consider refactoring to use a retry library, inorder to have fair retries
  // and more knobs for tuning retry pattern.
  @VisibleForTesting
  <T> T withRetries(Operation<T> operation) throws SQLException {
    int attempts = 0;
    // maximum number of retries.
    int maxAttempts = relationalJdbcConfiguration.maxRetries().orElse(1);
    // How long we should try, since the first attempt.
    long maxDuration = relationalJdbcConfiguration.maxDurationInMs().orElse(5000L);
    // How long to wait before first failure.
    long delay = relationalJdbcConfiguration.initialDelayInMs().orElse(100L);

    // maximum time we will retry till.
    long maxRetryTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) + maxDuration;

    while (attempts < maxAttempts) {
      try {
        return operation.execute();
      } catch (SQLException | RuntimeException e) {
        SQLException sqlException;
        if (e instanceof RuntimeException) {
          // Handle Exceptions from ResultSet Iterator consumer, as it throws a RTE, ignore RTE from
          // the transactions.
          if (e.getCause() instanceof SQLException
              && !(e instanceof EntityAlreadyExistsException)) {
            sqlException = (SQLException) e.getCause();
          } else {
            throw e;
          }
        } else {
          sqlException = (SQLException) e;
        }

        attempts++;
        long timeLeft =
            Math.max((maxRetryTime - TimeUnit.NANOSECONDS.toMillis(System.nanoTime())), 0L);
        if (timeLeft == 0 || attempts >= maxAttempts || !isRetryable(sqlException)) {
          String exceptionMessage =
              String.format(
                  "Failed due to '%s' (error code %d, sql-state '%s'), after %s attempts and %s milliseconds",
                  sqlException.getMessage(),
                  sqlException.getErrorCode(),
                  sqlException.getSQLState(),
                  attempts,
                  maxDuration);
          // Giving up must not erase what the transaction helper established about durable
          // effect; a plain re-wrap would drop it and leave the store with nothing to report.
          if (sqlException instanceof DisruptedTransactionException disrupted) {
            throw new DisruptedTransactionException(disrupted, exceptionMessage);
          }
          throw new SQLException(
              exceptionMessage, sqlException.getSQLState(), sqlException.getErrorCode(), e);
        }
        // Add jitter
        long timeToSleep = Math.min(timeLeft, delay + (long) (random.nextFloat() * 0.2 * delay));
        LOGGER.debug(
            "Sleeping {} ms before retrying {} on attempt {} / {}, reason {}",
            timeToSleep,
            operation,
            attempts,
            maxAttempts,
            e.getMessage(),
            e);
        try {
          Thread.sleep(timeToSleep);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Retry interrupted", ie);
        }
        delay *= 2; // Exponential backoff
      }
    }
    // This should never be reached
    return null;
  }

  public interface Operation<T> {
    T execute() throws SQLException;
  }

  // Interface for transaction callback
  public interface TransactionCallback {
    boolean execute(Connection connection) throws SQLException;
  }

  public boolean isUniquenessConstraintViolation(SQLException e) {
    return UNIQUENESS_CONSTRAINT_VIOLATION_SQL_CODE.equals(e.getSQLState());
  }

  public boolean isRelationDoesNotExist(SQLException e) {
    return (RELATION_DOES_NOT_EXIST.equals(e.getSQLState())
            && (databaseType == DatabaseType.POSTGRES || databaseType == DatabaseType.COCKROACHDB))
        || ((H2_SCHEMA_DOES_NOT_EXIST.equals(e.getSQLState())
                || H2_TABLE_DOES_NOT_EXIST.equals(e.getSQLState()))
            && databaseType == DatabaseType.H2);
  }

  private Connection borrowConnection() throws SQLException {
    return datasource.getConnection();
  }

  private static void logQuery(QueryGenerator.PreparedQuery query) {
    LOGGER
        .atDebug()
        .addArgument(query.sql())
        .addArgument(
            () ->
                query.parameters().stream()
                    .map(o -> o != null ? o.toString() : "NULL")
                    .collect(Collectors.joining("\n    ", "\n    ", "")))
        .setMessage("query: {}{}")
        .log();
  }
}
