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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.durable.conformance.BaseDurableRecordStoreConformanceTest;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.spi.durable.CommitDisruptedException;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.RecordRef;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.Test;

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
    return operationsOver(freshPool());
  }

  private static JdbcConnectionPool freshPool() {
    return JdbcConnectionPool.create(
        "jdbc:h2:mem:durable_conformance_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1", "sa", "");
  }

  private static DatasourceOperations operationsOver(javax.sql.DataSource dataSource) {
    DatasourceOperations datasourceOperations =
        new DatasourceOperations(dataSource, new TestJdbcConfiguration());
    try (InputStream scriptStream = DatabaseType.H2.openInitScriptResource(SCHEMA_VERSION)) {
      datasourceOperations.executeScript(scriptStream);
    } catch (IOException | SQLException e) {
      throw new RuntimeException(e);
    }
    return datasourceOperations;
  }

  // ------------------------------------------------------- disruption, by failure position
  //
  // What a caller may infer from a disrupted commit differs by where the transaction failed, so
  // each position is pinned separately and asserts the effect, not merely that something was
  // thrown. The injector is armed after the store is built, because building it opens a connection
  // of its own to identify the database.

  private FaultInjectingDataSource injector;

  private DurableRecordStore storeOverInjector() {
    injector = new FaultInjectingDataSource(freshPool());
    return new JdbcDurableRecordStore(operationsOver(injector), REALM, SCHEMA_VERSION);
  }

  private static List<Mutation> oneCreate() {
    PolarisBaseEntity entity =
        new PolarisBaseEntity.Builder()
            .catalogId(1L)
            .id(4242L)
            .typeCode(PolarisEntityType.NAMESPACE.getCode())
            .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
            .parentId(1L)
            .name("disruption")
            .entityVersion(1)
            .propertiesAsMap(Map.of())
            .internalPropertiesAsMap(Map.of())
            .build();
    return List.of(
        Mutation.of(
            PolarisRecordKinds.ENTITY,
            Mutation.Op.CREATE,
            RecordRef.byIdentity(PolarisRecordKinds.ENTITY, List.of(entity.getId())),
            entity));
  }

  @Test
  void aFailureAcquiringTheConnectionProvesNothingWasApplied() {
    DurableRecordStore store = storeOverInjector();
    injector.arm(FaultInjectingDataSource.Fault.ON_CONNECT);

    assertThatThrownBy(() -> store.commit(oneCreate()))
        .isInstanceOf(CommitDisruptedException.class)
        .extracting(e -> ((CommitDisruptedException) e).durableEffect())
        .isEqualTo(CommitDisruptedException.DurableEffect.NONE);
  }

  @Test
  void aStatementFailureRolledBackProvesNothingWasApplied() {
    DurableRecordStore store = storeOverInjector();
    injector.arm(FaultInjectingDataSource.Fault.ON_EXECUTE);

    assertThatThrownBy(() -> store.commit(oneCreate()))
        .isInstanceOf(CommitDisruptedException.class)
        .extracting(e -> ((CommitDisruptedException) e).durableEffect())
        .isEqualTo(CommitDisruptedException.DurableEffect.NONE);
  }

  @Test
  void aFailureCommittingTheTransactionLeavesTheOutcomeUnknown() {
    DurableRecordStore store = storeOverInjector();
    injector.arm(FaultInjectingDataSource.Fault.ON_COMMIT);

    assertThatThrownBy(() -> store.commit(oneCreate()))
        .isInstanceOf(CommitDisruptedException.class)
        .extracting(e -> ((CommitDisruptedException) e).durableEffect())
        .isEqualTo(CommitDisruptedException.DurableEffect.UNKNOWN);
  }

  @Test
  void aFailureAfterTheCommitSucceededLeavesTheOutcomeUnknown() {
    DurableRecordStore store = storeOverInjector();
    injector.arm(FaultInjectingDataSource.Fault.ON_RESTORE_AFTER_COMMIT);

    // The statements are in storage by this point, so reporting them absent would be a lie.
    assertThatThrownBy(() -> store.commit(oneCreate()))
        .isInstanceOf(CommitDisruptedException.class)
        .extracting(e -> ((CommitDisruptedException) e).durableEffect())
        .isEqualTo(CommitDisruptedException.DurableEffect.UNKNOWN);
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
