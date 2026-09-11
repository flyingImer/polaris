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
package org.apache.polaris.extension.durable.manager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.exceptions.AlreadyExistsException;
import org.apache.polaris.core.exceptions.CommitConflictException;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.extension.orchestration.DefaultDurableOrchestrator;
import org.apache.polaris.extension.primitives.routing.MappedDurableRecordStoreLocator;
import org.apache.polaris.extension.primitives.routing.RoutingDurableRecordStore;
import org.apache.polaris.persistence.treemap.TreeMapDurableRecordStore;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.LookupPath;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.Precondition;
import org.apache.polaris.spi.durable.Read;
import org.apache.polaris.spi.durable.RecordKind;
import org.apache.polaris.spi.durable.RecordRef;
import org.apache.polaris.spi.durable.RecordVersions;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The two windows the secrets mutations' declared conditions close: a reset that loses the race for
 * a client id, and a delete whose row disappears between its own read and its commit. Both writes
 * used to reach the store unconditioned, so both losers reported an outcome no serial order
 * explains — a create that only the store's own duplicate check refused, and a delete that removed
 * nothing and said so to nobody.
 *
 * <p>Assembly is {@link DefaultGrantDurableManagerTest}'s: TreeMap behind the routing store, one
 * orchestrator, one primitives handle. {@link StagedRaceDurableRecordStore} wraps that handle and
 * performs the winner's write at the start of the loser's commit — after every read the loser
 * makes, before the mutation it built from them reaches the store. That is the whole window, and
 * reaching it needs no second thread.
 *
 * <p>The delete case is decided by behaviour on either shipped store: both evaluate {@code EXISTS}
 * with a real read (TreeMap in its precondition loop, JDBC with a SELECT over the same where-clause
 * its DELETE uses), and without the condition a DELETE of an absent row is a no-op both report as
 * applied. The reset case cannot be decided that way, because both shipped stores refuse a
 * duplicate CREATE whatever the mutation declares — the measurement is in {@code
 * DefaultGrantDurableManager#persistNewGrantRecord}'s javadoc. So it asserts the caller's result
 * and the declared condition: the declaration is what carries the refusal to a store that honours
 * it instead of over-rejecting, and it is the half that goes red if the condition is dropped.
 */
class DefaultSecretsDurableManagerTest {

  private DurableRecordStore routing;
  private StagedRaceDurableRecordStore primitives;
  private DefaultCatalogDurableManager entities;
  private DefaultPrincipalDurableManager principals;
  private DefaultSecretsDurableManager secrets;
  private PolarisCallContext callCtx;

  @BeforeEach
  void setup() {
    DurableRecordStore store = new TreeMapDurableRecordStore(new PolarisDefaultDiagServiceImpl());
    routing =
        new RoutingDurableRecordStore(
            new MappedDurableRecordStoreLocator(
                Map.of(
                    PolarisRecordKinds.ENTITY, "main",
                    PolarisRecordKinds.GRANT_RECORD, "main",
                    PolarisRecordKinds.POLICY_MAPPING, "main",
                    PolarisRecordKinds.PRINCIPAL_SECRETS, "main",
                    PolarisRecordKinds.EVENT, "main"),
                Map.of("main", store)),
            List.of(store),
            store);
    primitives = new StagedRaceDurableRecordStore(routing);
    var diagnostics = new PolarisDefaultDiagServiceImpl();
    var orchestrator = new DefaultDurableOrchestrator(primitives);
    entities =
        new DefaultCatalogDurableManager(Clock.systemUTC(), diagnostics, orchestrator, primitives);
    principals =
        new DefaultPrincipalDurableManager(
            diagnostics, orchestrator, primitives, PrincipalSecretsGenerator.RANDOM_SECRETS);
    secrets = new DefaultSecretsDurableManager(diagnostics, orchestrator, primitives);
    callCtx = new PolarisCallContext(() -> "testRealm", new NeverCallOldPrimitives());
  }

  private PrincipalEntity createPrincipal(String name) {
    PrincipalEntity requested =
        new PrincipalEntity.Builder()
            .setId(entities.generateNewEntityId(callCtx).getId())
            .setName(name)
            .setCreateTimestamp(System.currentTimeMillis())
            .build();
    CreatePrincipalResult created = principals.createPrincipal(callCtx, requested);
    assertThat(created.isSuccess()).isTrue();
    return created.getPrincipal();
  }

  @Test
  void resetLosingTheClientIdRaceIsRefusedByItsOwnDeclaredCondition() {
    PrincipalEntity winner = createPrincipal("raceWinner");
    PrincipalEntity loser = createPrincipal("raceLoser");
    String contendedClientId = "contended-client-id";
    RecordRef contended = RecordRefs.secretsIdentity(contendedClientId);

    // The winner claims the client id after the loser's read of it has already come back empty.
    primitives.stageBeforeNextCommit(
        () ->
            routing.commit(
                List.of(
                    Mutation.of(
                        PolarisRecordKinds.PRINCIPAL_SECRETS,
                        Mutation.Op.CREATE,
                        contended,
                        new PolarisPrincipalSecrets(
                            winner.getId(), contendedClientId, "winner-secret"),
                        List.of(Precondition.notExists(contended))))));

    assertThatThrownBy(
            () -> secrets.resetPrincipalSecrets(callCtx, loser.getId(), contendedClientId, null))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining(contendedClientId)
        .hasMessageContaining("Precondition{NOT_EXISTS");

    assertThat(primitives.mutationsOfLastCommit())
        .singleElement()
        .satisfies(
            mutation -> {
              assertThat(mutation.op()).isEqualTo(Mutation.Op.CREATE);
              assertThat(mutation.preconditions())
                  .anySatisfy(
                      precondition -> {
                        assertThat(precondition.op()).isEqualTo(Precondition.Op.NOT_EXISTS);
                        assertThat(precondition.ref()).contains(contended);
                      });
            });

    assertThat(routing.get(contended, PolarisPrincipalSecrets.class))
        .get()
        .extracting(PolarisPrincipalSecrets::getPrincipalId)
        .isEqualTo(winner.getId());
  }

  @Test
  void deleteLosingTheRaceIsRefusedAsAConflictNotReportedAsAbsent() {
    PrincipalEntity principal = createPrincipal("deletedTwice");
    String clientId = principal.getClientId();
    RecordRef ref = RecordRefs.secretsIdentity(clientId);

    // The winner's delete lands after the loser's reads confirmed the row and its principal id.
    primitives.stageBeforeNextCommit(
        () ->
            routing.commit(
                List.of(
                    Mutation.of(
                        PolarisRecordKinds.PRINCIPAL_SECRETS, Mutation.Op.DELETE, ref, null))));

    // A condition-refused commit that rolled back in full is a REPORTED conflict, not an invariant
    // violation: the in-family type the error mapper renders 409, with the refused condition named.
    assertThatThrownBy(() -> secrets.deletePrincipalSecrets(callCtx, clientId, principal.getId()))
        .isInstanceOf(CommitConflictException.class)
        .hasMessageContaining(clientId)
        .hasMessageContaining("Precondition{EXISTS");

    assertThat(routing.get(ref, PolarisPrincipalSecrets.class)).isEmpty();
  }

  /**
   * Forwards everything to a real handle, with two additions the cases above need: a one-shot
   * action that runs at the start of the next {@code commit}, and the mutations of the last commit
   * that went through. The action is the competing writer, and where it runs is the point: every
   * read the caller under test makes has already returned, and the mutation it built from those
   * reads has not yet reached the store.
   */
  private static final class StagedRaceDurableRecordStore implements DurableRecordStore {
    private final DurableRecordStore delegate;
    private Runnable staged;
    private List<Mutation> lastCommit = List.of();

    StagedRaceDurableRecordStore(DurableRecordStore delegate) {
      this.delegate = delegate;
    }

    /** Runs {@code competingWrite} once, at the start of the next commit through this handle. */
    void stageBeforeNextCommit(Runnable competingWrite) {
      this.staged = competingWrite;
    }

    List<Mutation> mutationsOfLastCommit() {
      return lastCommit;
    }

    @Override
    public @NonNull CommitResult commit(@NonNull List<Mutation> mutations) {
      if (staged != null) {
        Runnable competingWrite = staged;
        staged = null;
        competingWrite.run();
      }
      lastCommit = List.copyOf(mutations);
      return delegate.commit(mutations);
    }

    @Override
    public long generateNewId() {
      return delegate.generateNewId();
    }

    @Override
    public @NonNull <T> Optional<T> get(@NonNull RecordRef ref, @NonNull Class<T> type) {
      return delegate.get(ref, type);
    }

    @Override
    public @NonNull <T> Optional<Read<T>> read(@NonNull RecordRef ref, @NonNull Class<T> type) {
      return delegate.read(ref, type);
    }

    @Override
    public @NonNull <T> List<Optional<T>> getMany(
        @NonNull List<RecordRef> refs, @NonNull Class<T> type) {
      return delegate.getMany(refs, type);
    }

    @Override
    public @NonNull <T> Page<T> list(
        @NonNull RecordKind kind,
        @NonNull LookupPath path,
        @NonNull List<Object> anchors,
        @NonNull PageToken pageToken,
        @NonNull Class<T> type) {
      return delegate.list(kind, path, anchors, pageToken, type);
    }

    @Override
    public @NonNull <T> Page<T> list(
        @NonNull LookupPath path,
        @NonNull List<Object> anchors,
        @NonNull PageToken pageToken,
        @NonNull Class<T> type) {
      return delegate.list(path, anchors, pageToken, type);
    }

    @Override
    public @NonNull List<Optional<RecordVersions>> versionsOf(@NonNull List<RecordRef> refs) {
      return delegate.versionsOf(refs);
    }

    @Override
    public @NonNull Object domainOf(@NonNull RecordRef target) {
      return delegate.domainOf(target);
    }

    @Override
    public int maxItemsPerCommit() {
      return delegate.maxItemsPerCommit();
    }
  }
}
