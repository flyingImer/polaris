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
import org.apache.polaris.core.durable.conformance.BaseDurableRecordStoreConformanceTest;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.persistence.relational.jdbc.DatabaseType;
import org.apache.polaris.persistence.relational.jdbc.DatasourceOperations;
import org.apache.polaris.persistence.relational.jdbc.JdbcDurableRecordStore;
import org.apache.polaris.persistence.relational.jdbc.RelationalJdbcConfiguration;
import org.apache.polaris.persistence.treemap.TreeMapDurableRecordStore;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.h2.jdbcx.JdbcConnectionPool;

/**
 * The Seam-1 conformance suite over the ROUTING implementation as its own binding: every case runs
 * against a two-store assembly with the kinds split across the JDBC-on-H2 store ({@code main}:
 * ENTITY, GRANT_RECORD) and the in-memory store ({@code authz}: POLICY_MAPPING, PRINCIPAL_SECRETS,
 * EVENT). Green here means the routing store honors the primitives contract indistinguishably from
 * a single backend — which is the ticket's claim: nothing above the SPI can tell.
 *
 * <p>ENTITY and GRANT_RECORD are deliberately co-located: the suite's unedited cases commit the two
 * kinds together and assert their domains equal, so any conforming assembly must keep them on one
 * backend. The other three kinds live on the second store, so kind-scoped routing, the union read's
 * fan-out, and the per-instance domain declarations are all exercised by the same unedited cases.
 */
class RoutingDurableRecordStoreConformanceTest extends BaseDurableRecordStoreConformanceTest {

  private static final PolarisDiagnostics DIAGNOSTICS = new PolarisDefaultDiagServiceImpl();
  private static final String REALM = "REALM";
  private static final int SCHEMA_VERSION = 4;

  @Override
  protected DurableRecordStore newStore() {
    return assembly(
        new JdbcDurableRecordStore(freshDatasource(), REALM, SCHEMA_VERSION),
        new TreeMapDurableRecordStore(DIAGNOSTICS));
  }

  @Override
  protected DurableRecordStore newStore(int maxItemsPerCommit) {
    // Both backends declare the given cap, so the routing minimum is that cap and the holding
    // backend enforces it.
    return assembly(
        new JdbcDurableRecordStore(freshDatasource(), REALM, SCHEMA_VERSION, maxItemsPerCommit),
        new TreeMapDurableRecordStore(DIAGNOSTICS, maxItemsPerCommit));
  }

  private static DurableRecordStore assembly(DurableRecordStore main, DurableRecordStore authz) {
    return new RoutingDurableRecordStore(
        new MappedDurableRecordStoreLocator(
            Map.of(
                PolarisRecordKinds.ENTITY, "main",
                PolarisRecordKinds.GRANT_RECORD, "main",
                PolarisRecordKinds.POLICY_MAPPING, "authz",
                PolarisRecordKinds.PRINCIPAL_SECRETS, "authz",
                PolarisRecordKinds.EVENT, "authz"),
            Map.of("main", main, "authz", authz)),
        List.of(main, authz),
        main);
  }

  private static DatasourceOperations freshDatasource() {
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:routing_conformance_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1",
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
