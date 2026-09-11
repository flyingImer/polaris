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
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.durable.conformance.BaseDurableRecordStoreConformanceTest;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.spi.durable.CommitDisruptedException;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.Precondition;
import org.apache.polaris.spi.durable.Read;
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

  /**
   * The folded condition is re-evaluated against the stored row on every statement, so a token that
   * went stale is refused even though the writer never held anything open while it went stale.
   *
   * <p><b>What this does and does not prove.</b> The three phases here are sequential Java: the
   * read returns its connection, then the competitor commits in full, then the loser's write
   * borrows a fresh connection. No transaction is held open across the competitor's commit and no
   * lock is involved, so this is not an SQL-level interleaving of two open transactions. What it
   * does show is the property the fold actually rests on — the condition travels in the write's own
   * WHERE clause and is therefore evaluated against whatever the row contains when that statement
   * runs, on whichever connection runs it, rather than against anything cached from the earlier
   * read. The safety argument under real concurrency is the fold itself plus the isolation level,
   * not this test's ordering.
   *
   * <p>Two stores over ONE pool. {@link #freshPool} mints a distinct in-memory URL per call, so
   * sharing the pool is what makes these two stores see the same rows; the schema script is
   * create-if-not-exists, so running it once per store is harmless.
   *
   * <p>The isolation level is asserted rather than assumed. Nothing in this store sets one, so a
   * borrowed connection runs at the driver's default, and pinning it here means a driver that
   * changed it would report the new value instead of quietly weakening every folded condition.
   */
  @Test
  void aStaleTokenIsReEvaluatedFreshOnASecondConnection() throws SQLException {
    JdbcConnectionPool shared = freshPool();
    DurableRecordStore mine =
        new JdbcDurableRecordStore(operationsOver(shared), REALM, SCHEMA_VERSION);
    DurableRecordStore theirs =
        new JdbcDurableRecordStore(operationsOver(shared), REALM, SCHEMA_VERSION);

    try (Connection probe = shared.getConnection()) {
      assertThat(probe.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_READ_COMMITTED);
    }

    assertThat(mine.commit(List.of(createSecrets(secrets(1L, "two-conn", "one")))).isApplied())
        .isTrue();
    Read<PolarisPrincipalSecrets> read =
        mine.read(secretsRef("two-conn"), PolarisPrincipalSecrets.class).orElseThrow();

    // Committed on the other connection, after the read above came back.
    assertThat(theirs.commit(List.of(overwriteSecrets(secrets(1L, "two-conn", "two")))).isApplied())
        .isTrue();

    CommitResult refused =
        mine.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.PRINCIPAL_SECRETS,
                    Mutation.Op.DELETE,
                    secretsRef("two-conn"),
                    null,
                    List.of(Precondition.unchangedSince(secretsRef("two-conn"), read.token())))));

    assertThat(refused.isApplied()).isFalse();
    assertThat(refused.failure()).contains(CommitResult.Failure.PRECONDITION_FAILED);
    assertThat(refused.failedPreconditions())
        .extracting(Precondition::op)
        .contains(Precondition.Op.UNCHANGED_SINCE);
    assertThat(mine.get(secretsRef("two-conn"), PolarisPrincipalSecrets.class))
        .get()
        .extracting(PolarisPrincipalSecrets::getMainSecretHash)
        .isEqualTo("mainhash-two");
  }

  /**
   * A stale token refuses an UPDATE, not only a DELETE, and the competitor's row survives.
   *
   * <p>This store folds the token into whichever statement carries it, and the UPDATE and DELETE
   * arms fold it separately. Every other token case in the suite issues a DELETE, so dropping the
   * fold from the UPDATE arm alone would go unnoticed. The production path this pins is the secrets
   * rotation, whose only race coverage runs against the in-memory store.
   */
  @Test
  void aStaleTokenOnAnUpdateIsRefusedAndTheCompetingRowSurvives() {
    DurableRecordStore s = new JdbcDurableRecordStore(freshDatasource(), REALM, SCHEMA_VERSION);
    assertThat(s.commit(List.of(createSecrets(secrets(1L, "upd-stale", "one")))).isApplied())
        .isTrue();
    Read<PolarisPrincipalSecrets> read =
        s.read(secretsRef("upd-stale"), PolarisPrincipalSecrets.class).orElseThrow();

    assertThat(s.commit(List.of(overwriteSecrets(secrets(1L, "upd-stale", "two")))).isApplied())
        .isTrue();

    CommitResult refused =
        s.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.PRINCIPAL_SECRETS,
                    Mutation.Op.UPDATE,
                    secretsRef("upd-stale"),
                    secrets(1L, "upd-stale", "three"),
                    List.of(Precondition.unchangedSince(secretsRef("upd-stale"), read.token())))));

    assertThat(refused.isApplied()).isFalse();
    assertThat(refused.failure()).contains(CommitResult.Failure.PRECONDITION_FAILED);
    assertThat(refused.failedPreconditions())
        .extracting(Precondition::op)
        .contains(Precondition.Op.UNCHANGED_SINCE);
    assertThat(s.get(secretsRef("upd-stale"), PolarisPrincipalSecrets.class))
        .get()
        .extracting(PolarisPrincipalSecrets::getMainSecretHash)
        .isEqualTo("mainhash-two");
  }

  /** The same UPDATE applies when the token is the one the row still matches. */
  @Test
  void aFreshTokenOnAnUpdateApplies() {
    DurableRecordStore s = new JdbcDurableRecordStore(freshDatasource(), REALM, SCHEMA_VERSION);
    assertThat(s.commit(List.of(createSecrets(secrets(1L, "upd-fresh", "one")))).isApplied())
        .isTrue();
    Read<PolarisPrincipalSecrets> read =
        s.read(secretsRef("upd-fresh"), PolarisPrincipalSecrets.class).orElseThrow();

    CommitResult applied =
        s.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.PRINCIPAL_SECRETS,
                    Mutation.Op.UPDATE,
                    secretsRef("upd-fresh"),
                    secrets(1L, "upd-fresh", "two"),
                    List.of(Precondition.unchangedSince(secretsRef("upd-fresh"), read.token())))));

    assertThat(applied.isApplied()).isTrue();
    assertThat(s.get(secretsRef("upd-fresh"), PolarisPrincipalSecrets.class))
        .get()
        .extracting(PolarisPrincipalSecrets::getMainSecretHash)
        .isEqualTo("mainhash-two");
  }

  /**
   * A token read from one row, passed under a reference naming another, is caller error rather than
   * a condition to evaluate.
   *
   * <p>Why this is rejected instead of simply failing: the token carries this kind's whole column
   * tuple, key columns included, and the fold writes those columns into the same map the target's
   * own key filled, fold last. Left unchecked, the statement would be re-addressed to the row the
   * token came from while the caller's reference still named the other one — a write to a record
   * the caller never asked to write. A token means something only for the record it was read from.
   */
  @Test
  void aTokenReadFromAnotherRowIsRejectedRatherThanRedirectingTheWrite() {
    DurableRecordStore s = new JdbcDurableRecordStore(freshDatasource(), REALM, SCHEMA_VERSION);
    assertThat(s.commit(List.of(createSecrets(secrets(1L, "f7-a", "one")))).isApplied()).isTrue();
    assertThat(s.commit(List.of(createSecrets(secrets(2L, "f7-b", "one")))).isApplied()).isTrue();
    Read<PolarisPrincipalSecrets> readA =
        s.read(secretsRef("f7-a"), PolarisPrincipalSecrets.class).orElseThrow();

    Throwable thrown =
        catchThrowable(
            () ->
                s.commit(
                    List.of(
                        Mutation.of(
                            PolarisRecordKinds.PRINCIPAL_SECRETS,
                            Mutation.Op.DELETE,
                            secretsRef("f7-b"),
                            null,
                            List.of(
                                Precondition.unchangedSince(secretsRef("f7-b"), readA.token()))))));

    assertThat(thrown).isInstanceOf(IllegalArgumentException.class);
    // Neither row was touched: the rejection happens before any statement runs.
    assertThat(s.get(secretsRef("f7-a"), PolarisPrincipalSecrets.class)).isPresent();
    assertThat(s.get(secretsRef("f7-b"), PolarisPrincipalSecrets.class)).isPresent();
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

  private static final RecordRef DISRUPTION_REF =
      RecordRef.byIdentity(PolarisRecordKinds.ENTITY, List.of(4242L));

  private static PolarisBaseEntity disruptionEntity() {
    return new PolarisBaseEntity.Builder()
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
  }

  private static PolarisBaseEntity missingEntity() {
    return new PolarisBaseEntity.Builder()
        .catalogId(1L)
        .id(9999L)
        .typeCode(PolarisEntityType.NAMESPACE.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .parentId(1L)
        .name("absent")
        .entityVersion(1)
        .propertiesAsMap(Map.of())
        .internalPropertiesAsMap(Map.of())
        .build();
  }

  private static List<Mutation> oneCreate() {
    return List.of(
        Mutation.of(
            PolarisRecordKinds.ENTITY, Mutation.Op.CREATE, DISRUPTION_REF, disruptionEntity()));
  }

  @Test
  void aFailureAcquiringTheConnectionProvesNothingWasApplied() {
    DurableRecordStore store = storeOverInjector();
    injector.arm(FaultInjectingDataSource.Fault.ON_CONNECT);

    assertThatThrownBy(() -> store.commit(oneCreate()))
        .isInstanceOf(CommitDisruptedException.class)
        .extracting(e -> ((CommitDisruptedException) e).durableEffect())
        .isEqualTo(CommitDisruptedException.DurableEffect.NONE);
    assertRowAbsent(store);
  }

  @Test
  void aStatementFailureRolledBackProvesNothingWasApplied() {
    DurableRecordStore store = storeOverInjector();
    injector.arm(FaultInjectingDataSource.Fault.ON_EXECUTE);

    assertThatThrownBy(() -> store.commit(oneCreate()))
        .isInstanceOf(CommitDisruptedException.class)
        .extracting(e -> ((CommitDisruptedException) e).durableEffect())
        .isEqualTo(CommitDisruptedException.DurableEffect.NONE);
    // NONE is a claim about storage, so the claim is checked against storage.
    assertRowAbsent(store);
  }

  @Test
  void aFailureCommittingTheTransactionLeavesTheOutcomeUnknown() {
    DurableRecordStore store = storeOverInjector();
    injector.arm(FaultInjectingDataSource.Fault.ON_COMMIT);

    assertThatThrownBy(() -> store.commit(oneCreate()))
        .isInstanceOf(CommitDisruptedException.class)
        .extracting(e -> ((CommitDisruptedException) e).durableEffect())
        .isEqualTo(CommitDisruptedException.DurableEffect.UNKNOWN);
    // No read-back assertion here on purpose. UNKNOWN claims nothing about storage, and this is
    // the one position where that is the whole point: the statements reached the server and the
    // failure says nothing about what it did with them. Pinning either state would pin this
    // driver's cleanup, not the contract. (The rollback the helper attempts here is why a read
    // finds nothing on H2; a server that had already committed would keep the row, and both are
    // conformant.)
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
    // And they are: the label is checked against the row it is a claim about.
    assertRowPresent(store);
  }

  /** Reads storage back through a working connection, so the fault under test is cleared first. */
  private void assertRowPresent(DurableRecordStore store) {
    injector.disarm();
    assertThat(store.get(DISRUPTION_REF, PolarisBaseEntity.class)).isPresent();
  }

  private void assertRowAbsent(DurableRecordStore store) {
    injector.disarm();
    assertThat(store.get(DISRUPTION_REF, PolarisBaseEntity.class)).isEmpty();
  }

  @Test
  void aFailureStartingTheTransactionProvesNothingWasApplied() {
    DurableRecordStore store = storeOverInjector();
    injector.arm(FaultInjectingDataSource.Fault.ON_TRANSACTION_START);

    assertThatThrownBy(() -> store.commit(oneCreate()))
        .isInstanceOf(CommitDisruptedException.class)
        .extracting(e -> ((CommitDisruptedException) e).durableEffect())
        .isEqualTo(CommitDisruptedException.DurableEffect.NONE);
  }

  @Test
  void aRollbackThatAlsoFailsLeavesTheOutcomeUnknown() {
    DurableRecordStore store = storeOverInjector();
    injector.arm(FaultInjectingDataSource.Fault.ON_EXECUTE_AND_ROLLBACK);

    // Nothing here can establish what the failed cleanup left, so the pessimistic label is the
    // only honest one.
    assertThatThrownBy(() -> store.commit(oneCreate()))
        .isInstanceOf(CommitDisruptedException.class)
        .extracting(e -> ((CommitDisruptedException) e).durableEffect())
        .isEqualTo(CommitDisruptedException.DurableEffect.UNKNOWN);
  }

  @Test
  void aSecondFailureWhileLeavingDoesNotDowngradeAnUnknownCommit() {
    DurableRecordStore store = storeOverInjector();
    injector.arm(FaultInjectingDataSource.Fault.ON_COMMIT_AND_RESTORE);

    // Two failures on one dead connection: the commit, then the cleanup after it. The verdict the
    // commit failure reached must survive the second failure rather than be recomputed from it.
    Throwable thrown = catchThrowable(() -> store.commit(oneCreate()));

    assertThat(thrown)
        .isInstanceOf(CommitDisruptedException.class)
        .extracting(e -> ((CommitDisruptedException) e).durableEffect())
        .isEqualTo(CommitDisruptedException.DurableEffect.UNKNOWN);
    // The verdict and the failure that landed after it travel together, on the object the caller
    // caught, not some number of wraps below it.
    Throwable[] alsoFailed = thrown.getSuppressed();
    assertThat(alsoFailed).hasSize(1);
    assertThat(alsoFailed[0]).hasMessageContaining("restore refused by the fault injector");
  }

  @Test
  void aDeclinedRequestWhoseRollbackFailsLeavesTheOutcomeUnknown() {
    DurableRecordStore store = storeOverInjector();

    // Two mutations, the second one declined: an UPDATE of a row that is not there fails its
    // implied exists condition. The first one's INSERT is issued before that, so the rollback the
    // decline asks for has real statements to undo, and here that rollback fails.
    Mutation created =
        Mutation.of(
            PolarisRecordKinds.ENTITY, Mutation.Op.CREATE, DISRUPTION_REF, disruptionEntity());
    RecordRef missingRef = RecordRef.byIdentity(PolarisRecordKinds.ENTITY, List.of(9999L));
    Mutation declined =
        Mutation.of(PolarisRecordKinds.ENTITY, Mutation.Op.UPDATE, missingRef, missingEntity());
    injector.arm(FaultInjectingDataSource.Fault.ON_DECLINE_ROLLBACK);

    // A decline whose rollback succeeded is a rejection, reported as a result. This one could not
    // be undone, so it stops being a rejection and becomes a disruption, and nothing here can say
    // whether the issued statements survived.
    assertThatThrownBy(() -> store.commit(List.of(created, declined)))
        .isInstanceOf(CommitDisruptedException.class)
        // Classified by the transaction helper, not read pessimistically by the store's fallback:
        // an unclassified SQLException would reach the same label without the guard being there.
        .hasCauseInstanceOf(DisruptedTransactionException.class)
        .extracting(e -> ((CommitDisruptedException) e).durableEffect())
        .isEqualTo(CommitDisruptedException.DurableEffect.UNKNOWN);

    // And the connection went back to the pool in the mode it was borrowed in, rather than with
    // auto-commit still off for the pool's own reset to deal with.
    assertThat(injector.autoCommitRestored()).isTrue();
    // No read-back, for the reason the commit-failure case above gives: UNKNOWN claims nothing
    // about storage, and what a failed rollback leaves is the driver's and the pool's business.
  }

  @Test
  void aRejectedRequestKeepsItsTypeAndLeavesNothingBehind() {
    DurableRecordStore store = storeOverInjector();

    // Two mutations, the second malformed: a version condition against a kind that carries no
    // version. The first one's INSERT is issued before the second is validated, so the transaction
    // is open with a row in it when the request is rejected.
    Mutation good =
        Mutation.of(
            PolarisRecordKinds.ENTITY, Mutation.Op.CREATE, DISRUPTION_REF, disruptionEntity());
    RecordRef grantRef =
        RecordRef.byIdentity(PolarisRecordKinds.GRANT_RECORD, List.of(1L, 2L, 3L, 4L, 5));
    Mutation malformed =
        Mutation.of(
            PolarisRecordKinds.GRANT_RECORD,
            Mutation.Op.UPDATE,
            grantRef,
            new PolarisGrantRecord(1L, 2L, 3L, 4L, 5),
            List.of(
                Precondition.versionEquals(
                    grantRef, Precondition.VersionAttribute.RECORD_VERSION, 1L)));

    assertThatThrownBy(() -> store.commit(List.of(good, malformed)))
        .isInstanceOf(IllegalArgumentException.class);

    // The rejection keeps its own type, and the row the open transaction held is gone: a rejected
    // request is all-or-nothing like any other commit.
    assertRowAbsent(store);
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
