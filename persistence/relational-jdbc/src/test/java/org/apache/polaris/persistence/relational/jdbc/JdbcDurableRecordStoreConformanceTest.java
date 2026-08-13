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

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Optional;
import org.apache.polaris.core.durable.conformance.BaseDurableRecordStoreConformanceTest;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.h2.jdbcx.JdbcConnectionPool;

/**
 * Runs the Seam-1 conformance suite against {@link JdbcDurableRecordStore} on an in-memory H2
 * database, inside the ordinary test task — the same datasource pattern as {@link
 * JdbcDurableRecordStoreTest}. The Testcontainers/Postgres integration test is a different net and
 * stays as-is.
 */
class JdbcDurableRecordStoreConformanceTest extends BaseDurableRecordStoreConformanceTest {

  private static final String REALM = "REALM";
  private static final int SCHEMA_VERSION = 4;

  @Override
  protected DurableRecordStore newStore() {
    return new JdbcDurableRecordStore(freshDatasource(), REALM, SCHEMA_VERSION);
  }

  @Override
  protected DurableRecordStore newStore(int maxItemsPerCommit) {
    return new JdbcDurableRecordStore(freshDatasource(), REALM, SCHEMA_VERSION, maxItemsPerCommit);
  }

  private static DatasourceOperations freshDatasource() {
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:durable_conformance_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1",
            "sa",
            "");
    DatasourceOperations datasourceOperations =
        new DatasourceOperations(dataSource, new TestJdbcConfiguration());
    try (InputStream scriptStream = DatabaseType.H2.openInitScriptResource(SCHEMA_VERSION)) {
      datasourceOperations.executeScript(scriptStream);
    } catch (IOException | SQLException e) {
      throw new RuntimeException(e);
    }
    return datasourceOperations;
  }

  private static final class TestJdbcConfiguration implements RelationalJdbcConfiguration {
    @Override
    public Optional<Integer> maxRetries() {
      return Optional.of(2);
    }

    @Override
    public Optional<Long> maxDurationInMs() {
      return Optional.of(100L);
    }

    @Override
    public Optional<Long> initialDelayInMs() {
      return Optional.of(100L);
    }

    @Override
    public Optional<String> databaseType() {
      return Optional.of("h2");
    }
  }
}
