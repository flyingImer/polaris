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

    PolarisPrincipalSecrets current =
        primitives
            .get(RecordRefs.secretsIdentity(clientId), PolarisPrincipalSecrets.class)
            .orElse(null);
    diagnostics.checkNotNull(
        current, "cannot_find_secrets", "client_id={} principalId={}", clientId, principalId);
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
            RecordRefs.secretsIdentity(clientId),
            updated,
            List.of(Precondition.none())));
    if (reset && !flagPresent) {
      internalProps.put(
          PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE, "true");
      mutations.add(RecordMutations.internalPropertiesMutation(principal, internalProps));
    } else if (flagPresent) {
      internalProps.remove(PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE);
      mutations.add(RecordMutations.internalPropertiesMutation(principal, internalProps));
    }

    OrchestrationResult result = orchestrator.commit(mutations);
    return result.isApplied()
        ? new PrincipalSecretsResult(updated)
        : mapFailedSecretsMutation(result);
  }

  /**
   * Ported from {@code IntegrationPersistence#storePrincipalSecrets}: throws {@link
   * AlreadyExistsException} uncaught on ANY existing row for {@code resolvedClientId}, regardless
   * of which principal it belongs to — {@code testResetCredentialsClientIdCollision} exercises
   * exactly this (principal B tries to claim principal A's already-in-use client id). C7: resolve
   * that read before building anything to commit, matching the file's style elsewhere.
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
                    List.of(Precondition.none()))));
    if (result.isApplied()) {
      return new PrincipalSecretsResult(secrets);
    }
    boolean lostRace =
        result.outcome() != OrchestrationResult.Outcome.ROLLBACK_INCOMPLETE
            && result
                .groupFailure()
                .flatMap(CommitResult::failure)
                .filter(f -> f == CommitResult.Failure.PRECONDITION_FAILED)
                .isPresent();
    if (lostRace) {
      // Lost the race between the pre-check above and this commit: someone else claimed
      // resolvedClientId in between. Same exception the pre-check reports.
      throw new AlreadyExistsException("Client ID already in use: " + resolvedClientId);
    }
    return mapFailedSecretsMutation(result);
  }

  /**
   * Ported from {@code TreeMapDurablePrimitivesImpl#deletePrincipalSecretsInCurrentTxn}'s two
   * checks (secrets must exist, principal id must match), then a plain DELETE — same risk profile
   * as {@code DefaultGrantDurableManager#revokeGrantRecord}'s DELETE: existence was just confirmed
   * by this method's own read, and no precondition closes the (equally present in the old model)
   * race between that read and the write.
   */
  @Override
  public void deletePrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String clientId, long principalId) {
    RecordRef ref = RecordRefs.secretsIdentity(clientId);
    PolarisPrincipalSecrets secrets =
        primitives.get(ref, PolarisPrincipalSecrets.class).orElse(null);
    diagnostics.checkNotNull(
        secrets, "cannot_find_secrets", "client_id={} principalId={}", clientId, principalId);
    diagnostics.check(
        principalId == secrets.getPrincipalId(),
        "principal_id_mismatch",
        "expectedId={} id={}",
        principalId,
        secrets.getPrincipalId());
    OrchestrationResult result =
        orchestrator.commit(
            List.of(
                Mutation.of(PolarisRecordKinds.PRINCIPAL_SECRETS, Mutation.Op.DELETE, ref, null)));
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
