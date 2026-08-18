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
package org.apache.polaris.extension.primitives.routing;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.durable.conformance.BaseDurableOrchestratorConformanceTest;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.extension.orchestration.DefaultDurableOrchestrator;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.JdbcDurableRecordStore;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.apache.polaris.persistence.treemap.TreeMapDurableRecordStore;
import org.apache.polaris.spi.durable.DurableOrchestrator;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.h2.jdbcx.JdbcConnectionPool;

/**
 * Runs the Seam-2 conformance suite against the C11 multi-store configuration: a mapped locator and
 * the routing store assemble JDBC-on-H2 as {@code main} and the in-memory store as {@code authz}
 * (the second logical store the record says to use) behind one primitives handle, and {@link
 * DefaultDurableOrchestrator} is the implementation under test.
 */
class RoutingAssembledOrchestratorConformanceTest extends BaseDurableOrchestratorConformanceTest {

  private static final PolarisDiagnostics DIAGNOSTICS = new PolarisDefaultDiagServiceImpl();
  private static final String REALM = "REALM";
  private static final int SCHEMA_VERSION = 4;

  @Override
  protected DurableRecordStore newMainStore() {
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:orch_conformance_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1", "sa", "");
    DatasourceOperations datasourceOperations =
        new DatasourceOperations(dataSource, new TestJdbcConfiguration());
    try (InputStream scriptStream = DatabaseType.H2.openInitScriptResource(SCHEMA_VERSION)) {
      datasourceOperations.executeScript(scriptStream);
    } catch (IOException | SQLException e) {
      throw new RuntimeException(e);
    }
    return new JdbcDurableRecordStore(datasourceOperations, REALM, SCHEMA_VERSION);
  }

  @Override
  protected DurableRecordStore newAuthzStore() {
    return new TreeMapDurableRecordStore(DIAGNOSTICS);
  }

  @Override
  protected DurableRecordStore assemble(DurableRecordStore main, DurableRecordStore authz) {
    // No default store: every kind is mapped explicitly, GRANT_RECORD to authz, the rest to main.
    MappedDurableRecordStoreLocator locator =
        new MappedDurableRecordStoreLocator(
            Map.of(
                PolarisRecordKinds.ENTITY, "main",
                PolarisRecordKinds.POLICY_MAPPING, "main",
                PolarisRecordKinds.PRINCIPAL_SECRETS, "main",
                PolarisRecordKinds.EVENT, "main",
                PolarisRecordKinds.GRANT_RECORD, "authz"),
            Map.of("main", main, "authz", authz));
    return new RoutingDurableRecordStore(locator, List.of(main, authz), main);
  }

  @Override
  protected DurableOrchestrator orchestratorOver(DurableRecordStore primitives) {
    return new DefaultDurableOrchestrator(primitives);
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
