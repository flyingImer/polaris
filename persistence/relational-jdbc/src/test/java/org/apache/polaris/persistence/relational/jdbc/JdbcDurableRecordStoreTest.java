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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.spi.durable.LookupPath;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.RecordRef;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link JdbcDurableRecordStore}'s declared lookup paths against a real H2 database, so
 * each path's realization runs the SQL it claims to run.
 *
 * <p>This is not the conformance suite. That suite runs the same cases against every store and is
 * separate work; these are the cases whose failure would mean a declared path does not serve what
 * its declaration states.
 */
class JdbcDurableRecordStoreTest {

  private static final String REALM = "REALM";
  private static final int SCHEMA_VERSION = 4;
  private static final PageToken EVERYTHING = PageToken.readEverything();

  private JdbcDurableRecordStore store;

  @BeforeEach
  void setUp() throws SQLException {
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:durable_record_store_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1",
            "sa",
            "");
    DatasourceOperations datasourceOperations =
        new DatasourceOperations(dataSource, new TestJdbcConfiguration());
    try (InputStream scriptStream = DatabaseType.H2.openInitScriptResource(SCHEMA_VERSION)) {
      datasourceOperations.executeScript(scriptStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    store = new JdbcDurableRecordStore(datasourceOperations, REALM, SCHEMA_VERSION);
  }

  private static PolarisBaseEntity entity(long id, long parentId, String name) {
    return entity(id, parentId, name, Map.of());
  }

  // A real type code matters here, unlike in the in-memory store's tests: this store's converter
  // rejects rows whose type or subtype code is not in the vocabulary when reading them back.
  private static PolarisBaseEntity entity(
      long id, long parentId, String name, Map<String, String> properties) {
    return new PolarisBaseEntity.Builder()
        .catalogId(1L)
        .id(id)
        .typeCode(PolarisEntityType.NAMESPACE.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .parentId(parentId)
        .name(name)
        .entityVersion(1)
        .propertiesAsMap(properties)
        .internalPropertiesAsMap(Map.of())
        .build();
  }

  private void create(PolarisBaseEntity e) {
    assertThat(
            store
                .commit(
                    List.of(
                        Mutation.of(
                            PolarisRecordKinds.ENTITY,
                            Mutation.Op.CREATE,
                            RecordRef.byIdentity(PolarisRecordKinds.ENTITY, List.of(e.getId())),
                            e)))
                .isApplied())
        .isTrue();
  }

  @Test
  void listByParentReturnsTheChildrenOfExactlyThatParent() {
    create(entity(10L, 1L, "a"));
    create(entity(11L, 1L, "b"));
    create(entity(12L, 2L, "c"));

    assertThat(
            store
                .list(
                    PolarisRecordKinds.ENTITY,
                    PolarisRecordKinds.ENTITY_BY_PARENT,
                    List.of(1L, 1L),
                    EVERYTHING,
                    PolarisBaseEntity.class)
                .items())
        .extracting(PolarisBaseEntity::getId)
        .containsExactlyInAnyOrder(10L, 11L);
  }

  @Test
  void theOptionalTrailingSubtypeAnchorNarrowsTheParentListing() {
    create(entity(10L, 1L, "plain"));
    create(
        new PolarisBaseEntity.Builder(entity(11L, 1L, "subtyped"))
            .typeCode(PolarisEntityType.TABLE_LIKE.getCode())
            .subTypeCode(PolarisEntitySubType.GENERIC_TABLE.getCode())
            .build());

    assertThat(
            store
                .list(
                    PolarisRecordKinds.ENTITY,
                    PolarisRecordKinds.ENTITY_BY_PARENT,
                    List.of(1L, 1L, PolarisEntitySubType.GENERIC_TABLE.getCode()),
                    EVERYTHING,
                    PolarisBaseEntity.class)
                .items())
        .extracting(PolarisBaseEntity::getId)
        .containsExactly(11L);
  }

  @Test
  void listByLocationPrefixHonoursBothTheCatalogAnchorAndThePrefix() {
    create(
        entity(
            10L,
            1L,
            "t1",
            Map.of(PolarisEntityConstants.ENTITY_BASE_LOCATION, "s3://bucket/warehouse/t1")));
    create(
        entity(
            11L,
            1L,
            "t2",
            Map.of(PolarisEntityConstants.ENTITY_BASE_LOCATION, "s3://bucket/elsewhere/t2")));

    assertThat(
            store
                .list(
                    PolarisRecordKinds.ENTITY,
                    PolarisRecordKinds.ENTITY_BY_LOCATION_PREFIX,
                    List.of(1L, "s3://bucket/warehouse"),
                    EVERYTHING,
                    PolarisBaseEntity.class)
                .items())
        .extracting(PolarisBaseEntity::getId)
        .containsExactly(10L);

    // the catalog anchor is part of the declared signature, not decoration
    assertThat(
            store
                .list(
                    PolarisRecordKinds.ENTITY,
                    PolarisRecordKinds.ENTITY_BY_LOCATION_PREFIX,
                    List.of(9L, "s3://bucket/warehouse"),
                    EVERYTHING,
                    PolarisBaseEntity.class)
                .items())
        .isEmpty();
  }

  @Test
  void bySecurableAndByGranteeAreDistinctDirectionsNotOneSymmetricMatch() {
    PolarisGrantRecord grant = new PolarisGrantRecord(1L, 10L, 1L, 20L, 3);
    assertThat(
            store
                .commit(
                    List.of(
                        Mutation.of(
                            PolarisRecordKinds.GRANT_RECORD,
                            Mutation.Op.CREATE,
                            RecordRef.byIdentity(
                                PolarisRecordKinds.GRANT_RECORD, List.of(1L, 10L, 1L, 20L, 3)),
                            grant)))
                .isApplied())
        .isTrue();

    assertThat(
            store
                .list(
                    PolarisRecordKinds.GRANT_RECORD,
                    PolarisRecordKinds.GRANT_RECORD_BY_SECURABLE,
                    List.of(1L, 10L),
                    EVERYTHING,
                    PolarisGrantRecord.class)
                .items())
        .hasSize(1);
    assertThat(
            store
                .list(
                    PolarisRecordKinds.GRANT_RECORD,
                    PolarisRecordKinds.GRANT_RECORD_BY_GRANTEE,
                    List.of(1L, 20L),
                    EVERYTHING,
                    PolarisGrantRecord.class)
                .items())
        .hasSize(1);
    // the grantee's address on the securable path matches nothing: each direction is its own path
    assertThat(
            store
                .list(
                    PolarisRecordKinds.GRANT_RECORD,
                    PolarisRecordKinds.GRANT_RECORD_BY_SECURABLE,
                    List.of(1L, 20L),
                    EVERYTHING,
                    PolarisGrantRecord.class)
                .items())
        .isEmpty();
  }

  @Test
  void byTargetAndByPolicyServeBothPolicyMappingDirections() {
    PolarisPolicyMappingRecord mapping = new PolarisPolicyMappingRecord(1L, 10L, 1L, 30L, 5, "{}");
    assertThat(
            store
                .commit(
                    List.of(
                        Mutation.of(
                            PolarisRecordKinds.POLICY_MAPPING,
                            Mutation.Op.CREATE,
                            RecordRef.byIdentity(
                                PolarisRecordKinds.POLICY_MAPPING, List.of(1L, 10L, 5, 1L, 30L)),
                            mapping)))
                .isApplied())
        .isTrue();

    assertThat(
            store
                .list(
                    PolarisRecordKinds.POLICY_MAPPING,
                    PolarisRecordKinds.POLICY_MAPPING_BY_TARGET,
                    List.of(1L, 10L),
                    EVERYTHING,
                    PolarisPolicyMappingRecord.class)
                .items())
        .hasSize(1);
    assertThat(
            store
                .list(
                    PolarisRecordKinds.POLICY_MAPPING,
                    PolarisRecordKinds.POLICY_MAPPING_BY_POLICY,
                    List.of(1L, 30L),
                    EVERYTHING,
                    PolarisPolicyMappingRecord.class)
                .items())
        .hasSize(1);
  }

  @Test
  void theKindLessFormUnionsOverTheKindsDeclaringThePath() {
    create(entity(10L, 1L, "child"));

    assertThat(
            store
                .list(
                    PolarisRecordKinds.ENTITY_BY_PARENT, List.of(1L, 1L), EVERYTHING, Object.class)
                .items())
        .hasSize(1);
    assertThat(
            store
                .list(
                    PolarisRecordKinds.ENTITY_BY_PARENT, List.of(1L, 99L), EVERYTHING, Object.class)
                .items())
        .isEmpty();
  }

  @Test
  void anUndeclaredPathOrMismatchedAnchorsAreRejectedRatherThanGuessedAt() {
    assertThatThrownBy(
            () ->
                store.list(
                    PolarisRecordKinds.PRINCIPAL_SECRETS,
                    PolarisRecordKinds.ENTITY_BY_PARENT,
                    List.of(1L, 1L),
                    EVERYTHING,
                    Object.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("declares no lookup path");

    assertThatThrownBy(
            () -> store.list(LookupPath.of("by-nothing"), List.of(1L), EVERYTHING, Object.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No registered kind declares");

    assertThatThrownBy(
            () ->
                store.list(
                    PolarisRecordKinds.ENTITY,
                    PolarisRecordKinds.ENTITY_BY_PARENT,
                    List.of(1L),
                    EVERYTHING,
                    PolarisBaseEntity.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("declares 2 to 3 anchors");

    assertThatThrownBy(
            () ->
                store.list(
                    PolarisRecordKinds.ENTITY,
                    PolarisRecordKinds.ENTITY_BY_PARENT,
                    List.of("1", 1L),
                    EVERYTHING,
                    PolarisBaseEntity.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("declares anchor 0 as Long");
  }

  @Test
  void theLocationPathAlsoMatchesAncestorsOfTheAnchorLikeTheShippedOverlapQuery() {
    create(
        entity(
            10L,
            1L,
            "ns1",
            Map.of(PolarisEntityConstants.ENTITY_BASE_LOCATION, "s3://bucket/warehouse/ns1/")));

    assertThat(
            store
                .list(
                    PolarisRecordKinds.ENTITY,
                    PolarisRecordKinds.ENTITY_BY_LOCATION_PREFIX,
                    List.of(1L, "s3://bucket/warehouse/ns1/table1"),
                    EVERYTHING,
                    PolarisBaseEntity.class)
                .items())
        .extracting(PolarisBaseEntity::getId)
        .containsExactly(10L);
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
