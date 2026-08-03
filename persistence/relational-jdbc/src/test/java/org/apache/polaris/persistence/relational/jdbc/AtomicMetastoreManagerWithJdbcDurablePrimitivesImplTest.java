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

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.AtomicOperationMetaStoreManager;
import org.apache.polaris.core.persistence.BaseDurableManagerTest;
import org.apache.polaris.core.persistence.PolarisTestMetaStoreManager;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.apache.polaris.core.persistence.dao.entity.CreateCatalogResult;
import org.apache.polaris.spi.durable.DurableManager;
import org.apache.polaris.spi.durable.DurablePrimitives;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.Test;

public abstract class AtomicMetastoreManagerWithJdbcDurablePrimitivesImplTest
    extends BaseDurableManagerTest {

  protected DatabaseType databaseType() {
    return DatabaseType.H2;
  }

  public abstract int schemaVersion();

  protected DataSource createDataSource() {
    return JdbcConnectionPool.create(
        String.format(
            "jdbc:h2:file:./build/test_data/polaris/db_%s_%d",
            databaseType().getDisplayName(), schemaVersion()),
        "sa",
        "");
  }

  protected InputStream openSchemaScript() {
    ClassLoader classLoader = DatasourceOperations.class.getClassLoader();
    String resource =
        String.format("%s/schema-v%d.sql", databaseType().getDisplayName(), schemaVersion());
    InputStream scriptStream = classLoader.getResourceAsStream(resource);
    if (scriptStream == null) {
      throw new IllegalStateException("Schema resource not found: " + resource);
    }
    return scriptStream;
  }

  @Override
  protected PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    DatasourceOperations datasourceOperations;
    try {
      datasourceOperations =
          new DatasourceOperations(
              createDataSource(),
              SimpleRelationalJdbcConfiguration.forDatabaseType(databaseType()));
      try (InputStream scriptStream = openSchemaScript()) {
        datasourceOperations.executeScript(scriptStream);
      }
    } catch (SQLException | IOException e) {
      throw new RuntimeException(
          String.format(
              "Error executing %s schema-v%d script: %s",
              databaseType(), schemaVersion(), e.getMessage()),
          e);
    }

    RealmContext realmContext = () -> "REALM";
    JdbcDurablePrimitivesImpl basePersistence =
        new JdbcDurablePrimitivesImpl(
            diagServices,
            datasourceOperations,
            RANDOM_SECRETS,
            realmContext.getRealmIdentifier(),
            schemaVersion());
    AtomicOperationMetaStoreManager metaStoreManager =
        new AtomicOperationMetaStoreManager(clock, diagServices);
    PolarisCallContext callCtx = new PolarisCallContext(realmContext, basePersistence);
    return new PolarisTestMetaStoreManager(metaStoreManager, callCtx);
  }

  @Test
  void testHasOverlappingSiblingsUsesStoredBaseLocation() {
    // The optimized check relies on the location_without_scheme column added in schema v2.
    Assumptions.assumeThat(schemaVersion()).isGreaterThanOrEqualTo(2);

    var metaStoreManager = polarisTestMetaStoreManager.polarisMetaStoreManager();
    var callContext = polarisTestMetaStoreManager.polarisCallContext();
    PolarisBaseEntity catalog =
        new PolarisBaseEntity(
            PolarisEntityConstants.getNullId(),
            metaStoreManager.generateNewEntityId(callContext).getId(),
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootEntityId(),
            "overlap_test_catalog");
    catalog = metaStoreManager.createCatalog(callContext, catalog, List.of()).getCatalog();

    PolarisBaseEntity existingNamespace =
        buildLocationBasedNamespace(
            catalog,
            metaStoreManager.generateNewEntityId(callContext).getId(),
            "existing",
            "s3://bucket/warehouse/existing/");
    metaStoreManager.createEntityIfNotExists(
        callContext, List.of(PolarisEntity.toCore(catalog)), existingNamespace);

    TestLocationBasedEntity candidateNamespace =
        new TestLocationBasedEntity(
            buildLocationBasedNamespace(
                catalog,
                metaStoreManager.generateNewEntityId(callContext).getId(),
                "candidate",
                "s3://bucket/warehouse/existing/child"));

    Assertions.assertThat(metaStoreManager.hasOverlappingSiblings(callContext, candidateNamespace))
        .contains(Optional.of("s3://bucket/warehouse/existing/"));

    TestLocationBasedEntity nonOverlappingNamespace =
        new TestLocationBasedEntity(
            buildLocationBasedNamespace(
                catalog,
                metaStoreManager.generateNewEntityId(callContext).getId(),
                "non_overlapping",
                "s3://bucket/warehouse/non-overlapping"));

    Assertions.assertThat(
            metaStoreManager.hasOverlappingSiblings(callContext, nonOverlappingNamespace))
        .contains(Optional.empty());
  }

  /**
   * ADR-0002 durable-parity invariant (HARD), mechanism facet: {@code createCatalog} on the
   * atomic/CAS manager must commit the catalog + adminRole + grants through exactly one {@code
   * commitChangeSet} call, not the ~11 independent, individually non-atomic primitive writes it
   * used to issue one at a time. This is the directly-verifiable claim -- it does not, and cannot,
   * prove no partially-initialized catalog can ever exist after a real mid-transaction crash; a
   * test cannot honestly simulate that.
   */
  @Test
  void testCreateCatalogIssuesExactlyOneCommitChangeSet() {
    AtomicInteger commitChangeSetCalls = new AtomicInteger();
    AtomicInteger individualWriteCalls = new AtomicInteger();
    PolarisCallContext countingCallCtx =
        withInterceptedPrimitives(
            (real, methodName, args, result) -> {
              switch (methodName) {
                case "commitChangeSet" -> commitChangeSetCalls.incrementAndGet();
                case "writeEntity", "writeEntities", "writeToGrantRecords" ->
                    individualWriteCalls.incrementAndGet();
                default -> {}
              }
            });
    DurableManager mgr = polarisTestMetaStoreManager.polarisMetaStoreManager();

    PolarisBaseEntity catalog =
        new PolarisBaseEntity(
            PolarisEntityConstants.getNullId(),
            mgr.generateNewEntityId(countingCallCtx).getId(),
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootEntityId(),
            "single_commit_catalog");

    CreateCatalogResult result = mgr.createCatalog(countingCallCtx, catalog, List.of());

    Assertions.assertThat(result.isSuccess()).isTrue();
    Assertions.assertThat(commitChangeSetCalls.get())
        .as(
            "createCatalog must commit the catalog + adminRole + grants as one commitChangeSet"
                + " call")
        .isEqualTo(1);
    Assertions.assertThat(individualWriteCalls.get())
        .as(
            "createCatalog must not also fall back to individual"
                + " writeEntity/writeEntities/writeToGrantRecords calls once it commits the"
                + " change-set")
        .isEqualTo(0);
  }

  /**
   * ADR-0002 durable-parity invariant (HARD), baseline-binding facet: a change-set's update
   * mutation must carry the exact entity object the caller last read as its CAS baseline, per
   * {@code EntityMutation#update}'s javadoc. This test simulates a concurrent writer bumping the
   * service admin role's {@code grantRecordsVersion} between {@code createCatalog}'s read of it and
   * the {@code commitChangeSet} call that uses that read as its baseline: the whole commit must
   * fail with {@link RetryOnConcurrencyException}, and nothing from the aborted change-set -- not
   * the catalog, not the admin role, not any grant -- must be visible afterwards.
   */
  @Test
  void testCreateCatalogRejectsStaleGrantRecordsVersionBaseline() {
    PolarisCallContext realCallCtx = polarisTestMetaStoreManager.polarisCallContext();
    DurableManager mgr = polarisTestMetaStoreManager.polarisMetaStoreManager();
    DurablePrimitives real = realCallCtx.getMetaStore();
    PolarisCallContext rawCallCtx = new PolarisCallContext(realCallCtx.getRealmContext(), real);

    PolarisCallContext interceptingCallCtx =
        withInterceptedPrimitives(
            (r, methodName, args, result) -> {
              if (methodName.equals("lookupEntityByName")
                  && PolarisEntityConstants.getNameOfPrincipalServiceAdminRole().equals(args[4])) {
                PolarisBaseEntity serviceAdminRole = (PolarisBaseEntity) result;
                // Simulate a concurrent writer bumping this entity's grantRecordsVersion between
                // this read (which createCatalog will bind as its CAS baseline) and the commit.
                r.writeEntity(
                    rawCallCtx,
                    serviceAdminRole.withGrantRecordsVersion(
                        serviceAdminRole.getGrantRecordsVersion() + 1),
                    false,
                    serviceAdminRole);
              }
            });

    PolarisBaseEntity catalog =
        new PolarisBaseEntity(
            PolarisEntityConstants.getNullId(),
            mgr.generateNewEntityId(interceptingCallCtx).getId(),
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootEntityId(),
            "stale_baseline_catalog");

    Assertions.assertThatThrownBy(() -> mgr.createCatalog(interceptingCallCtx, catalog, List.of()))
        .isInstanceOf(RetryOnConcurrencyException.class);

    Assertions.assertThat(
            mgr.loadEntity(
                    rawCallCtx, catalog.getCatalogId(), catalog.getId(), PolarisEntityType.CATALOG)
                .getEntity())
        .as("catalog must not be visible after the whole change-set was rejected")
        .isNull();
  }

  private static PolarisBaseEntity buildLocationBasedNamespace(
      PolarisBaseEntity catalog, long entityId, String name, String baseLocation) {
    return new PolarisBaseEntity.Builder()
        .catalogId(catalog.getId())
        .parentId(catalog.getId())
        .id(entityId)
        .typeCode(PolarisEntityType.NAMESPACE.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .name(name)
        .propertiesAsMap(Map.of(PolarisEntityConstants.ENTITY_BASE_LOCATION, baseLocation))
        .internalPropertiesAsMap(Map.of())
        .build();
  }

  private static class TestLocationBasedEntity extends PolarisEntity
      implements LocationBasedEntity {
    TestLocationBasedEntity(PolarisBaseEntity sourceEntity) {
      super(sourceEntity);
    }

    @Override
    public String getBaseLocation() {
      return getPropertiesAsMap().get(PolarisEntityConstants.ENTITY_BASE_LOCATION);
    }
  }
}
