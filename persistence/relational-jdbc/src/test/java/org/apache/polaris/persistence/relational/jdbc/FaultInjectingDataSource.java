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

import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * A datasource that delegates to a real one until a fault is armed, then fails at one chosen point.
 *
 * <p>Arming is separate from construction on purpose: building the operations helper opens a
 * connection of its own to identify the database, so a datasource that failed from the start could
 * never get that far. Tests build over a working database, then arm.
 *
 * <p>The faults are the positions a transaction can fail at, each with a different consequence for
 * what is left in storage.
 */
class FaultInjectingDataSource implements DataSource {

  enum Fault {
    /** Nothing is issued: the connection is never handed out. */
    ON_CONNECT,
    /** Opening the transaction fails, before any statement is issued. */
    ON_TRANSACTION_START,
    /** A statement fails once the transaction is open. */
    ON_EXECUTE,
    /** A statement fails and the rollback that cleans up after it fails too. */
    ON_EXECUTE_AND_ROLLBACK,
    /**
     * Every statement succeeds, the callback declines the transaction, and the rollback that makes
     * that rejection true fails. Nothing failed on the way in, so this position is reachable only
     * through a declined body, not through any of the others.
     */
    ON_DECLINE_ROLLBACK,
    /** The transaction commit itself fails, after every statement has been issued. */
    ON_COMMIT,
    /** The commit fails and the connection then fails again while being restored. */
    ON_COMMIT_AND_RESTORE,
    /** The commit succeeds and the connection then fails while being restored. */
    ON_RESTORE_AFTER_COMMIT,
  }

  private final DataSource delegate;
  private Fault armed;
  private String sqlState = "08006";
  private boolean autoCommitRestored;

  FaultInjectingDataSource(DataSource delegate) {
    this.delegate = delegate;
  }

  void arm(Fault fault) {
    this.armed = fault;
    this.autoCommitRestored = false;
  }

  /** Clears the fault so a test can read storage back through a working connection. */
  void disarm() {
    this.armed = null;
  }

  void armWithState(Fault fault, String sqlState) {
    this.armed = fault;
    this.sqlState = sqlState;
    this.autoCommitRestored = false;
  }

  /**
   * Whether the connection was asked to go back to auto-commit since the fault was armed. It is the
   * observable for "the transaction helper left the connection as it found it", which no assertion
   * on the thrown effect can see: a pool that is handed a connection still in a transaction resets
   * the mode itself, and that reset commits.
   */
  boolean autoCommitRestored() {
    return autoCommitRestored;
  }

  @Override
  public Connection getConnection() throws SQLException {
    if (armed == Fault.ON_CONNECT) {
      throw new SQLException("connection refused by the fault injector", sqlState);
    }
    return wrap(delegate.getConnection());
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    if (armed == Fault.ON_CONNECT) {
      throw new SQLException("connection refused by the fault injector", sqlState);
    }
    return wrap(delegate.getConnection(username, password));
  }

  /**
   * Wraps the connection so one of its methods fails. A proxy rather than a handwritten delegate:
   * the interface carries far more methods than these tests touch, and forwarding all of them by
   * hand would bury the three lines that matter.
   */
  private Connection wrap(Connection real) {
    InvocationHandler handler =
        (proxy, method, args) -> {
          String name = method.getName();
          if (armed == Fault.ON_TRANSACTION_START
              && name.equals("setAutoCommit")
              && Boolean.FALSE.equals(args[0])) {
            throw new SQLException("transaction start refused by the fault injector", sqlState);
          }
          if ((armed == Fault.ON_EXECUTE || armed == Fault.ON_EXECUTE_AND_ROLLBACK)
              && name.equals("prepareStatement")) {
            throw new SQLException("statement refused by the fault injector", sqlState);
          }
          if ((armed == Fault.ON_EXECUTE_AND_ROLLBACK || armed == Fault.ON_DECLINE_ROLLBACK)
              && name.equals("rollback")) {
            throw new SQLException("rollback refused by the fault injector", sqlState);
          }
          if ((armed == Fault.ON_COMMIT || armed == Fault.ON_COMMIT_AND_RESTORE)
              && name.equals("commit")) {
            throw new SQLException("commit refused by the fault injector", sqlState);
          }
          if (name.equals("setAutoCommit") && Boolean.TRUE.equals(args[0])) {
            // Recorded before the fault below, so the flag says the restore was reached rather than
            // that it succeeded.
            autoCommitRestored = true;
          }
          if ((armed == Fault.ON_RESTORE_AFTER_COMMIT || armed == Fault.ON_COMMIT_AND_RESTORE)
              && name.equals("setAutoCommit")
              && Boolean.TRUE.equals(args[0])) {
            // Restoring auto-commit is the last thing that happens, so a failure here lands after
            // the transaction body has already decided what it left behind.
            throw new SQLException("restore refused by the fault injector", sqlState);
          }
          try {
            return method.invoke(real, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        };
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, handler);
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return delegate.getLogWriter();
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    delegate.setLogWriter(out);
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    delegate.setLoginTimeout(seconds);
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return delegate.getLoginTimeout();
  }

  @Override
  public Logger getParentLogger() {
    throw new UnsupportedOperationException("not needed by these tests");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return delegate.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return delegate.isWrapperFor(iface);
  }
}
