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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.exceptions.AlreadyExistsException;
import org.apache.polaris.core.exceptions.CommitConflictException;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableOrchestrator;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.OrchestrationResult;
import org.apache.polaris.spi.durable.Precondition;
import org.apache.polaris.spi.durable.Read;
import org.apache.polaris.spi.durable.RecordRef;
import org.apache.polaris.spi.durable.SecretsDurableManager;
import org.jspecify.annotations.NonNull;

/**
 * Default principal-secrets durable manager: load, rotate, reset and delete a principal's secrets.
 *
 * <p>Rotation submits the secrets update and, only when the credential-rotation-required flag has
 * to be set or cleared, an update to the principal entity's internal properties in the same
 * mutation list. Those are two record kinds: the list is atomic only when both resolve to the same
 * atomicity domain, and it becomes more than one commit when they do not. Reset writes the new
 * secrets record alone and never touches the principal entity.
 *
 * <p>Two doors, strictly divided: every write goes through the orchestrator's commit, every read
 * goes to the primitives handle directly. Owns its business rules and knows no storage topology;
 * authorization and request validation live above this layer.
 */
public class DefaultSecretsDurableManager implements SecretsDurableManager {
  private final PolarisDiagnostics diagnostics;
  private final DurableOrchestrator orchestrator;
  private final DurableRecordStore primitives;

  public DefaultSecretsDurableManager(
      @NonNull PolarisDiagnostics diagnostics,
      @NonNull DurableOrchestrator orchestrator,
      @NonNull DurableRecordStore primitives) {
    this.diagnostics = diagnostics;
    this.orchestrator = orchestrator;
    this.primitives = primitives;
  }

  // ---------------------------------------------------------- SecretsDurableManager (ticket 91)

  /**
   * Maps a non-applied secrets-mutation {@link OrchestrationResult}. No old-model precedent, same
   * reasoning as {@code DefaultGrantDurableManager#mapFailedGrantMutation}: the old primitives
   * calls this replaces are raw read-modify-writes with no commit-outcome type to map from.
   */
  private PrincipalSecretsResult mapFailedSecretsMutation(@NonNull OrchestrationResult result) {
    if (result.outcome() == OrchestrationResult.Outcome.ROLLBACK_INCOMPLETE) {
      return new PrincipalSecretsResult(
          BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED,
          "rollback incomplete: "
              + result.uncompensated().size()
              + " mutation(s) require admin reclamation");
    }
    CommitResult.Failure failure = result.groupFailure().orElseThrow().failure().orElseThrow();
    return new PrincipalSecretsResult(
        BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED, failure.toString());
  }

  /**
   * Whether a non-applied commit failed because a declared condition did not hold, which is how
   * both mutation paths below tell "another caller got there first" from an infrastructure failure.
   * A {@code ROLLBACK_INCOMPLETE} outcome is never a lost race: compensation itself did not finish,
   * which {@link #mapFailedSecretsMutation} reports as an admin-reclamation case.
   */
  private static boolean lostRace(@NonNull OrchestrationResult result) {
    return result.outcome() != OrchestrationResult.Outcome.ROLLBACK_INCOMPLETE
        && result
            .groupFailure()
            .flatMap(CommitResult::failure)
            .filter(f -> f == CommitResult.Failure.PRECONDITION_FAILED)
            .isPresent();
  }

  /**
   * The declared conditions the store REPORTED as refused, rendered for a conflict message. {@code
   * CommitResult#failedPreconditions()} may report only a subset — both shipped stores stop at the
   * first failed condition, and the contract's minimum is "at least the one that stopped the
   * commit" — so this names what was reported and never claims to be the complete set.
   */
  private static @NonNull String refusedConditions(@NonNull OrchestrationResult result) {
    List<String> reported =
        result.groupFailure().map(CommitResult::failedPreconditions).orElse(List.of()).stream()
            .map(Precondition::toString)
            .toList();
    return String.join(", ", reported);
  }

  @Override
  public @NonNull PrincipalSecretsResult loadPrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String clientId) {
    return primitives
        .get(RecordRefs.secretsIdentity(clientId), PolarisPrincipalSecrets.class)
        .<PrincipalSecretsResult>map(PrincipalSecretsResult::new)
        .orElseGet(
            () -> new PrincipalSecretsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null));
  }

  /**
   * Ported from {@code TreeMapDurablePrimitivesImpl#rotatePrincipalSecretsInCurrentTxn} for the
   * secret rotation itself, and from both old managers' {@code rotatePrincipalSecrets} for the
   * {@code PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE} bookkeeping:
   *
   * <ul>
   *   <li>One rotation always happens: {@code secondary <- oldSecretHash}, {@code main <- fresh
   *       random}. {@code oldSecretHash} is trusted, not verified against the current main —
   *       neither old primitives implementation checks it either.
   *   <li>{@code doReset} (the caller's {@code reset} flag OR the flag already being set on the
   *       principal) chains a SECOND rotation using the just-generated main as the new secondary.
   *       That is what makes a reset invalidate both the caller's old value and the intermediate
   *       value nobody ever saw, rather than merely rotating once.
   *   <li>The entity write branches on the caller's raw {@code reset}, not {@code doReset}: {@code
   *       reset && !flagPresent} SETS the flag (a caller-requested "next rotation must reset"
   *       mark); {@code flagPresent} (regardless of {@code reset}) CLEARS it (the flag that was
   *       already set has now been honored by this call). Neither branch firing means no entity
   *       write at all for this call.
   * </ul>
   */
  @Override
  public @NonNull PrincipalSecretsResult rotatePrincipalSecrets(
      @NonNull PolarisCallContext callCtx,
      @NonNull String clientId,
      long principalId,
      boolean reset,
      @NonNull String oldSecretHash) {
    Optional<PrincipalEntity> principalOpt = findPrincipalById(callCtx, principalId);
    if (principalOpt.isEmpty()) {
      return new PrincipalSecretsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }
    PrincipalEntity principal = principalOpt.get();
    Map<String, String> internalProps = new HashMap<>(principal.getInternalPropertiesAsMap());
    boolean flagPresent =
        internalProps.containsKey(
            PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE);
    boolean doReset = reset || flagPresent;

    RecordRef secretsRef = RecordRefs.secretsIdentity(clientId);
    Read<PolarisPrincipalSecrets> read =
        primitives.read(secretsRef, PolarisPrincipalSecrets.class).orElse(null);
    diagnostics.checkNotNull(
        read, "cannot_find_secrets", "client_id={} principalId={}", clientId, principalId);
    PolarisPrincipalSecrets current = read.value();
    diagnostics.check(
        principalId == current.getPrincipalId(),
        "principal_id_mismatch",
        "expectedId={} id={}",
        principalId,
        current.getPrincipalId());

    PolarisPrincipalSecrets updated = new PolarisPrincipalSecrets(current);
    updated.rotateSecrets(oldSecretHash);
    if (doReset) {
      updated.rotateSecrets(updated.getMainSecretHash());
    }

    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.of(
            PolarisRecordKinds.PRINCIPAL_SECRETS,
            Mutation.Op.UPDATE,
            secretsRef,
            updated,
            List.of(Precondition.unchangedSince(secretsRef, read.token()))));
    if (reset && !flagPresent) {
      internalProps.put(
          PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE, "true");
      mutations.add(RecordMutations.internalPropertiesMutation(principal, internalProps));
    } else if (flagPresent) {
      internalProps.remove(PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE);
      mutations.add(RecordMutations.internalPropertiesMutation(principal, internalProps));
    }

    OrchestrationResult result = orchestrator.commit(mutations);
    if (result.isApplied()) {
      return new PrincipalSecretsResult(updated);
    }
    if (lostRace(result)) {
      // Lost the race between the read above and this commit: the row this rotation was computed
      // from is gone, or is no longer the row that was read. The commit was refused by its own
      // declared condition and rolled back in full, so nothing this call intended reached storage
      // and this is a reported conflict rather than an invariant violation. Before the condition
      // existed the loser overwrote whatever it found, or resurrected a row a delete had removed.
      throw new CommitConflictException(
          "Cannot rotate principal secrets for client id %s (principal %s): "
              + "commit refused by declared condition(s): %s",
          clientId, principalId, refusedConditions(result));
    }
    return mapFailedSecretsMutation(result);
  }

  /**
   * Ported from {@code IntegrationPersistence#storePrincipalSecrets}: throws {@link
   * AlreadyExistsException} uncaught on ANY existing row for {@code resolvedClientId}, regardless
   * of which principal it belongs to — {@code testResetCredentialsClientIdCollision} exercises
   * exactly this (principal B tries to claim principal A's already-in-use client id). C7: resolve
   * that read before building anything to commit, matching the file's style elsewhere.
   *
   * <p>That read rides into the commit as {@code NOT_EXISTS} on the same identity: the CREATE is
   * refused when a row for {@code resolvedClientId} exists at commit time, so a caller that loses
   * the race between the pre-read and the commit raises the same {@link AlreadyExistsException} the
   * pre-read does. The refusal is the declared condition's rather than a store's. {@code
   * DefaultGrantDurableManager#persistNewGrantRecord}'s javadoc reports a commit-twice measurement
   * against TreeMap, whose CREATE rejects a duplicate whatever the mutation declares, and says the
   * same of the JDBC store without a measurement behind it; what a store that does neither would do
   * with an unconditioned CREATE on an existing identity is not settled here, and this method's
   * outcome no longer depends on it.
   */
  @Override
  public @NonNull PrincipalSecretsResult resetPrincipalSecrets(
      @NonNull PolarisCallContext callCtx,
      long principalId,
      @NonNull String resolvedClientId,
      String customClientSecret) {
    if (findPrincipalById(callCtx, principalId).isEmpty()) {
      return new PrincipalSecretsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    RecordRef ref = RecordRefs.secretsIdentity(resolvedClientId);
    if (primitives.get(ref, PolarisPrincipalSecrets.class).isPresent()) {
      throw new AlreadyExistsException("Client ID already in use: " + resolvedClientId);
    }

    PolarisPrincipalSecrets secrets =
        new PolarisPrincipalSecrets(principalId, resolvedClientId, customClientSecret);
    OrchestrationResult result =
        orchestrator.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.PRINCIPAL_SECRETS,
                    Mutation.Op.CREATE,
                    ref,
                    secrets,
                    List.of(Precondition.notExists(ref)))));
    if (result.isApplied()) {
      return new PrincipalSecretsResult(secrets);
    }
    if (lostRace(result)) {
      // Lost the race between the pre-check above and this commit: someone else claimed
      // resolvedClientId in between. Same exception the pre-check reports, plus the declared
      // condition that refused the commit.
      throw new AlreadyExistsException(
          "Client ID already in use: "
              + resolvedClientId
              + "; commit refused by declared condition(s): "
              + refusedConditions(result));
    }
    return mapFailedSecretsMutation(result);
  }

  /**
   * Ported from {@code TreeMapDurablePrimitivesImpl#deletePrincipalSecretsInCurrentTxn}'s two
   * checks (secrets must exist, principal id must match), then a DELETE conditioned on the same
   * existence those checks just read. Unconditioned — as this method and the old model both were —
   * a DELETE of an already-deleted record is a store-level no-op, so two concurrent callers both
   * returned normally and no serial order explained that pair. With the condition the loser is
   * refused, and that refusal is reported as a conflict (next paragraph). Recorded rather than
   * glossed: a strictly serialized second call would instead report the row ABSENT, so the conflict
   * and the serialized answer are not the same answer; signalling a condition-refused, fully
   * rolled-back commit as a conflict is this manager's own chosen signal for that reported outcome
   * rather than a rule the durable contracts state, and it is the message that carries which
   * condition refused. Same shape as {@code DefaultPolicyDurableManager#detachPolicyFromEntity}'s
   * {@code EXISTS} on the mapping's own identity; {@code
   * DefaultGrantDurableManager#revokeGrantRecord}'s DELETE, which this method used to be paired
   * with, still carries the unconditioned form.
   *
   * <p>The two absences signal differently, deliberately. A row absent on ENTRY is reported by the
   * read-side {@code checkNotNull} as {@code NullPointerException}; losing the race AFTER those
   * reads passed is a commit the store refused by the declared condition and rolled back in full,
   * reported as {@code CommitConflictException} — an in-family {@code PolarisConflictException},
   * which the error mapper renders 409 — naming the refused condition in its message. The method is
   * {@code void} and every failure here is unchecked, so nothing in this surface's contract
   * distinguishes the two; the split is stated rather than left to be discovered.
   */
  @Override
  public void deletePrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String clientId, long principalId) {
    RecordRef ref = RecordRefs.secretsIdentity(clientId);
    Read<PolarisPrincipalSecrets> read =
        primitives.read(ref, PolarisPrincipalSecrets.class).orElse(null);
    diagnostics.checkNotNull(
        read, "cannot_find_secrets", "client_id={} principalId={}", clientId, principalId);
    PolarisPrincipalSecrets secrets = read.value();
    diagnostics.check(
        principalId == secrets.getPrincipalId(),
        "principal_id_mismatch",
        "expectedId={} id={}",
        principalId,
        secrets.getPrincipalId());
    OrchestrationResult result =
        orchestrator.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.PRINCIPAL_SECRETS,
                    Mutation.Op.DELETE,
                    ref,
                    null,
                    List.of(Precondition.unchangedSince(ref, read.token())))));
    if (lostRace(result)) {
      // Lost the race between the reads above and this commit: the row is already gone. The commit
      // was refused by its own declared EXISTS condition and rolled back completely, so this is a
      // REPORTED conflict in the Polaris exception family, not an invariant violation.
      throw new CommitConflictException(
          "Cannot delete principal secrets for client id %s (principal %s): "
              + "commit refused by declared condition(s): %s",
          clientId, principalId, refusedConditions(result));
    }
    diagnostics.check(
        result.isApplied(),
        "failed_to_delete_principal_secrets",
        "clientId={} result={}",
        clientId,
        result);
  }

  /**
   * Entity read by identity with the type and catalog filters of the catalog manager's public read;
   * a private copy so this manager depends on no other manager.
   */
  private @NonNull EntityResult loadEntity(
      @NonNull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      @NonNull PolarisEntityType entityType) {
    Optional<PolarisBaseEntity> found =
        primitives.get(RecordRefs.entityIdentity(entityId), PolarisBaseEntity.class);
    if (found.isPresent()
        && (found.get().getTypeCode() != entityType.getCode()
            || found.get().getCatalogId() != entityCatalogId)) {
      found = Optional.empty();
    }
    return found
        .<EntityResult>map(EntityResult::new)
        .orElseGet(() -> new EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null));
  }

  /**
   * Same lookup the previous single manager offered as a default: a principal by id, if it exists.
   */
  private Optional<PrincipalEntity> findPrincipalById(
      PolarisCallContext polarisCallContext, long principalId) {
    EntityResult loadResult =
        loadEntity(
            polarisCallContext,
            PolarisEntityConstants.getNullId(),
            principalId,
            PolarisEntityType.PRINCIPAL);
    if (!loadResult.isSuccess()) {
      return Optional.empty();
    }
    return Optional.of(loadResult.getEntity()).map(PrincipalEntity::of);
  }
}
