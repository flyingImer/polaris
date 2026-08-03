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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.apache.polaris.core.persistence.dao.entity.EntityMutation;
import org.apache.polaris.core.persistence.dao.entity.GrantMutation;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.Test;

/**
 * Proves {@code DurablePrimitives#commitChangeSet}'s baseline-binding property directly at the
 * primitives layer, independent of any {@code DurableManager}: an update mutation's baseline must
 * be exactly what was read before the mutation was built, and a stale baseline must fail the whole
 * commit rather than silently apply. See {@code EntityMutation#update}'s javadoc for the provenance
 * rule this proves.
 */
class JdbcDurablePrimitivesCommitChangeSetBaselineTest {

  private static final RealmContext REALM_CONTEXT = () -> "REALM";
  private static final int SCHEMA_VERSION = DatabaseType.H2.getLatestSchemaVersion();

  private JdbcDurablePrimitivesImpl newDurablePrimitives() throws IOException, SQLException {
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:commit_change_set_baseline_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1",
            "sa",
            "");
    DatasourceOperations datasourceOperations =
        new DatasourceOperations(
            dataSource, SimpleRelationalJdbcConfiguration.forDatabaseType(DatabaseType.H2));
    try (InputStream scriptStream = DatabaseType.H2.openInitScriptResource(SCHEMA_VERSION)) {
      datasourceOperations.executeScript(scriptStream);
    }
    return new JdbcDurablePrimitivesImpl(
        new PolarisDefaultDiagServiceImpl(),
        datasourceOperations,
        RANDOM_SECRETS,
        REALM_CONTEXT.getRealmIdentifier(),
        SCHEMA_VERSION);
  }

  @Test
  void commitChangeSetRejectsStaleBaselineInsteadOfSilentlyApplying()
      throws IOException, SQLException {
    JdbcDurablePrimitivesImpl durablePrimitives = newDurablePrimitives();
    PolarisCallContext callCtx = new PolarisCallContext(REALM_CONTEXT, durablePrimitives);

    PolarisBaseEntity grantee =
        new PolarisBaseEntity(
            0L,
            1L,
            PolarisEntityType.PRINCIPAL_ROLE,
            PolarisEntitySubType.NULL_SUBTYPE,
            0L,
            "role");
    durablePrimitives.writeEntity(callCtx, grantee, false, null);

    // The caller reads the entity; this is the baseline it will build its intended commit against.
    PolarisBaseEntity baselineRead =
        durablePrimitives.lookupEntity(
            callCtx, grantee.getCatalogId(), grantee.getId(), grantee.getTypeCode());
    assertThat(baselineRead.getGrantRecordsVersion()).isEqualTo(1);

    // A concurrent writer bumps the same entity's grants version and commits first, out from
    // under the caller that already has `baselineRead` in hand.
    PolarisBaseEntity concurrentlyBumped =
        baselineRead.withGrantRecordsVersion(baselineRead.getGrantRecordsVersion() + 1);
    durablePrimitives.writeEntity(callCtx, concurrentlyBumped, false, baselineRead);

    // The caller now tries to commit its own update using the baseline it read earlier, which is
    // stale: the persisted row has moved from version 1 to version 2 since that read.
    PolarisBaseEntity ourIntendedUpdate =
        baselineRead.withGrantRecordsVersion(baselineRead.getGrantRecordsVersion() + 1);
    PolarisGrantRecord grantRecord = new PolarisGrantRecord(10L, 11L, 0L, grantee.getId(), 21);

    assertThatExceptionOfType(RetryOnConcurrencyException.class)
        .isThrownBy(
            () ->
                durablePrimitives.commitChangeSet(
                    callCtx,
                    List.of(EntityMutation.update(ourIntendedUpdate, baselineRead)),
                    List.of(GrantMutation.create(grantRecord))));

    // The whole commit failed: the grant record must not have been persisted either, and the
    // entity must still show the concurrent writer's version, not ours.
    PolarisBaseEntity afterFailedCommit =
        durablePrimitives.lookupEntity(
            callCtx, grantee.getCatalogId(), grantee.getId(), grantee.getTypeCode());
    assertThat(afterFailedCommit.getGrantRecordsVersion()).isEqualTo(2);
    assertThat(
            durablePrimitives.loadAllGrantRecordsOnGrantee(
                callCtx, grantee.getCatalogId(), grantee.getId()))
        .isEmpty();
  }

  @Test
  void commitChangeSetAppliesEverythingWhenBaselineIsCurrent() throws IOException, SQLException {
    JdbcDurablePrimitivesImpl durablePrimitives = newDurablePrimitives();
    PolarisCallContext callCtx = new PolarisCallContext(REALM_CONTEXT, durablePrimitives);

    PolarisBaseEntity grantee =
        new PolarisBaseEntity(
            0L,
            1L,
            PolarisEntityType.PRINCIPAL_ROLE,
            PolarisEntitySubType.NULL_SUBTYPE,
            0L,
            "role");
    durablePrimitives.writeEntity(callCtx, grantee, false, null);
    PolarisBaseEntity currentRead =
        durablePrimitives.lookupEntity(
            callCtx, grantee.getCatalogId(), grantee.getId(), grantee.getTypeCode());

    PolarisBaseEntity updated = currentRead.withGrantRecordsVersion(2);
    PolarisGrantRecord grantRecord = new PolarisGrantRecord(10L, 11L, 0L, grantee.getId(), 21);

    durablePrimitives.commitChangeSet(
        callCtx,
        List.of(EntityMutation.update(updated, currentRead)),
        List.of(GrantMutation.create(grantRecord)));

    PolarisBaseEntity afterCommit =
        durablePrimitives.lookupEntity(
            callCtx, grantee.getCatalogId(), grantee.getId(), grantee.getTypeCode());
    assertThat(afterCommit.getGrantRecordsVersion()).isEqualTo(2);
    assertThat(
            durablePrimitives.loadAllGrantRecordsOnGrantee(
                callCtx, grantee.getCatalogId(), grantee.getId()))
        .hasSize(1);
  }
}
