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
import java.util.Optional;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.EntityMutation;
import org.apache.polaris.core.persistence.GrantMutation;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.Test;

/**
 * Proves the property the community's own #5035-shaped {@code commitChangeSet} did not enforce
 * on its convenience-builder path: an update mutation's baseline must be exactly what was read
 * before the mutation was built, and a stale baseline must fail the whole commit rather than
 * silently apply.
 */
class JdbcCommitChangeSetBaselineTest {

  private static final RealmContext REALM_CONTEXT = () -> "REALM";
  private static final int SCHEMA_VERSION = 5;

  private JdbcBasePersistenceImpl newBasePersistence() throws IOException, SQLException {
    JdbcConnectionPool dataSource =
        JdbcConnectionPool.create(
            "jdbc:h2:mem:commit_change_set_baseline_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1",
            "sa",
            "");
    DatasourceOperations datasourceOperations =
        new DatasourceOperations(dataSource, new TestJdbcConfiguration());
    try (InputStream scriptStream = DatabaseType.H2.openInitScriptResource(SCHEMA_VERSION)) {
      datasourceOperations.executeScript(scriptStream);
    }
    return new JdbcBasePersistenceImpl(
        new PolarisDefaultDiagServiceImpl(),
        datasourceOperations,
        RANDOM_SECRETS,
        REALM_CONTEXT.getRealmIdentifier(),
        SCHEMA_VERSION);
  }

  @Test
  void supportsAtomicMixedCommitIsTrueOnJdbc() throws IOException, SQLException {
    assertThat(newBasePersistence().supportsAtomicMixedCommit()).isTrue();
  }

  @Test
  void commitChangeSetRejectsStaleBaselineInsteadOfSilentlyApplying()
      throws IOException, SQLException {
    JdbcBasePersistenceImpl basePersistence = newBasePersistence();
    PolarisCallContext callCtx = new PolarisCallContext(REALM_CONTEXT, basePersistence);

    PolarisBaseEntity grantee =
        new PolarisBaseEntity(
            0L, 1L, PolarisEntityType.PRINCIPAL_ROLE, PolarisEntitySubType.NULL_SUBTYPE, 0L, "role");
    basePersistence.writeEntity(callCtx, grantee, false, null);

    // The caller reads the entity, this is the baseline it will build its intended commit against.
    PolarisBaseEntity baselineRead =
        basePersistence.lookupEntity(
            callCtx, grantee.getCatalogId(), grantee.getId(), grantee.getTypeCode());
    assertThat(baselineRead.getGrantRecordsVersion()).isEqualTo(1);

    // A concurrent writer bumps the same entity's grants version and commits first, out from
    // under the caller that already has `baselineRead` in hand.
    PolarisBaseEntity concurrentlyBumped =
        baselineRead.withGrantRecordsVersion(baselineRead.getGrantRecordsVersion() + 1);
    basePersistence.writeEntity(callCtx, concurrentlyBumped, false, baselineRead);

    // The caller now tries to commit its own grant using the baseline it read earlier, which is
    // stale: the persisted row has moved from version 1 to version 2 since that read.
    PolarisBaseEntity ourIntendedUpdate =
        baselineRead.withGrantRecordsVersion(baselineRead.getGrantRecordsVersion() + 1);
    PolarisGrantRecord grantRecord = new PolarisGrantRecord(10L, 11L, 0L, grantee.getId(), 21);

    assertThatExceptionOfType(RetryOnConcurrencyException.class)
        .isThrownBy(
            () ->
                basePersistence.commitChangeSet(
                    callCtx,
                    List.of(EntityMutation.update(ourIntendedUpdate, baselineRead)),
                    List.of(GrantMutation.create(grantRecord))));

    // The whole commit failed: the grant record must not have been persisted either, and the
    // entity must still show the concurrent writer's version, not ours.
    PolarisBaseEntity afterFailedCommit =
        basePersistence.lookupEntity(
            callCtx, grantee.getCatalogId(), grantee.getId(), grantee.getTypeCode());
    assertThat(afterFailedCommit.getGrantRecordsVersion()).isEqualTo(2);
  }

  @Test
  void commitChangeSetAppliesEverythingWhenBaselineIsCurrent() throws IOException, SQLException {
    JdbcBasePersistenceImpl basePersistence = newBasePersistence();
    PolarisCallContext callCtx = new PolarisCallContext(REALM_CONTEXT, basePersistence);

    PolarisBaseEntity grantee =
        new PolarisBaseEntity(
            0L, 1L, PolarisEntityType.PRINCIPAL_ROLE, PolarisEntitySubType.NULL_SUBTYPE, 0L, "role");
    basePersistence.writeEntity(callCtx, grantee, false, null);
    PolarisBaseEntity currentRead =
        basePersistence.lookupEntity(
            callCtx, grantee.getCatalogId(), grantee.getId(), grantee.getTypeCode());

    PolarisBaseEntity updated = currentRead.withGrantRecordsVersion(2);
    PolarisGrantRecord grantRecord = new PolarisGrantRecord(10L, 11L, 0L, grantee.getId(), 21);

    basePersistence.commitChangeSet(
        callCtx,
        List.of(EntityMutation.update(updated, currentRead)),
        List.of(GrantMutation.create(grantRecord)));

    PolarisBaseEntity afterCommit =
        basePersistence.lookupEntity(
            callCtx, grantee.getCatalogId(), grantee.getId(), grantee.getTypeCode());
    assertThat(afterCommit.getGrantRecordsVersion()).isEqualTo(2);
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
