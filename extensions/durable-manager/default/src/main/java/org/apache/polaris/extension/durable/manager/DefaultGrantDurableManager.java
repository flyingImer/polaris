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

import java.util.List;
import java.util.Optional;
import java.util.function.ToLongFunction;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.LoadGrantsResult;
import org.apache.polaris.core.persistence.dao.entity.PrivilegeResult;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableOrchestrator;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.GrantDurableManager;
import org.apache.polaris.spi.durable.LookupPath;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.OrchestrationResult;
import org.apache.polaris.spi.durable.Precondition;
import org.apache.polaris.spi.durable.RecordRef;
import org.apache.polaris.spi.durable.RecordVersions;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Default grant durable manager: grant and revoke records between securables and grantees, and the
 * two directional grant reads. Each write also bumps the grant-records version of both counterpart
 * entities.
 *
 * <p>Two doors, strictly divided: every write goes through the orchestrator's commit, every read
 * goes to the primitives handle directly. Owns its business rules and knows no storage topology;
 * authorization and request validation live above this layer.
 */
public class DefaultGrantDurableManager implements GrantDurableManager {
  private final PolarisDiagnostics diagnostics;
  private final DurableOrchestrator orchestrator;
  private final DurableRecordStore primitives;

  public DefaultGrantDurableManager(
      @NonNull PolarisDiagnostics diagnostics,
      @NonNull DurableOrchestrator orchestrator,
      @NonNull DurableRecordStore primitives) {
    this.diagnostics = diagnostics;
    this.orchestrator = orchestrator;
    this.primitives = primitives;
  }

  /**
   * Ported from both old impls' {@code persistNewGrantRecord} (structurally identical in {@code
   * AtomicOperationMetaStoreManager} and {@code TransactionalMetaStoreManagerImpl}): write the
   * grant, then bump the grantee's and the securable's {@code grantRecordsVersion}, in that order.
   * Resolved as one atomic orchestrated commit instead of three independent primitive writes, which
   * as a side effect closes the partial-failure gap both old impls' own {@code TODO: Reorder and/or
   * expose bulk update...} comments name — a version-bump failing after the grant write already
   * landed used to leave the two inconsistent; here the whole group applies or none of it does.
   *
   * <p><b>{@link Precondition#none()} is used per {@link Mutation.Op#CREATE}'s documented contract
   * for a kind whose identity and uniqueness are the same tuple — but empirically, NEITHER shipped
   * store's CREATE handling honors that contract yet.</b> Verified with a throwaway commit-twice
   * test against {@code TreeMapDurableRecordStore}: {@code applyMutation}'s CREATE case checks
   * {@code slice.read(identityKey) != null} and throws unconditionally on any hit, regardless of
   * the mutation's declared preconditions; the second of two identical commits reports {@code
   * PRECONDITION_FAILED} even with {@code Precondition.none()}. No conformance test exercises this
   * combination today ({@code grep Precondition.none()} across every test module returns nothing).
   * {@code Precondition.none()} is kept anyway because it is still the contractually correct
   * declaration for this kind, for whenever that gap closes — but it is NOT what makes this method
   * idempotent today. The pre-read below is: on a repeat grant with identical arguments, this
   * returns the existing record without touching the orchestrator at all, rather than reproducing
   * the old models' "always bump both versions, even on a no-op write" side effect (itself a
   * consequence of {@code DurablePrimitives#writeToGrantRecords} being documented as a silent no-op
   * on a duplicate PK, not a decision either old manager makes). No fixture assertion pins the
   * exact version-bump count on a duplicate grant, so skipping the commit entirely on a confirmed
   * repeat is simpler and strictly less wasteful — a disclosed, new choice, not a ported one.
   */
  private PrivilegeResult persistNewGrantRecord(
      @NonNull PolarisEntityCore securable,
      @NonNull PolarisEntityCore grantee,
      @NonNull PolarisPrivilege priv) {
    diagnostics.checkNotNull(securable, "unexpected_null_securable");
    diagnostics.checkNotNull(grantee, "unexpected_null_grantee");
    diagnostics.checkNotNull(priv, "unexpected_null_priv");
    diagnostics.check(
        grantee.getType().isGrantee(), "entity_must_be_grantee", "entity={}", grantee);

    PolarisGrantRecord grantRecord =
        new PolarisGrantRecord(
            securable.getCatalogId(),
            securable.getId(),
            grantee.getCatalogId(),
            grantee.getId(),
            priv.getCode());
    RecordRef ref = RecordRefs.grantIdentity(grantRecord);

    Optional<PolarisGrantRecord> existing = primitives.get(ref, PolarisGrantRecord.class);
    if (existing.isPresent()) {
      return new PrivilegeResult(existing.get());
    }

    PolarisBaseEntity granteeEntity =
        RecordRefs.mustLoadEntity(primitives, diagnostics, grantee, "grantee_not_found");
    PolarisBaseEntity securableEntity =
        RecordRefs.mustLoadEntity(primitives, diagnostics, securable, "securable_not_found");

    List<Mutation> mutations =
        List.of(
            RecordMutations.createGrantMutation(grantRecord),
            RecordMutations.bumpGrantRecordsVersion(granteeEntity).mutation(),
            RecordMutations.bumpGrantRecordsVersion(securableEntity).mutation());

    OrchestrationResult result = orchestrator.commit(mutations);
    return result.isApplied() ? new PrivilegeResult(grantRecord) : mapFailedGrantMutation(result);
  }

  /**
   * Ported from both old impls' {@code revokeGrantRecord} (structurally identical): delete the
   * grant, then bump the grantee's and securable's {@code grantRecordsVersion}, same order and same
   * one-commit atomicity rationale as {@link #persistNewGrantRecord}. The DELETE carries no payload
   * and no precondition of its own — existence was already confirmed by the caller's own pre-read
   * ({@link #revokeUsageOnRoleFromGrantee}/{@link #revokePrivilegeOnSecurableFromRole} both look
   * the grant up first and return {@code GRANT_NOT_FOUND} before calling this), the same risk
   * profile the old model carries between its own lookup and its own delete call — neither model
   * closes that particular race.
   */
  private PrivilegeResult revokeGrantRecord(
      @NonNull PolarisEntityCore securable,
      @NonNull PolarisEntityCore grantee,
      @NonNull PolarisGrantRecord grantRecord) {
    diagnostics.check(
        securable.getCatalogId() == grantRecord.getSecurableCatalogId()
            && securable.getId() == grantRecord.getSecurableId(),
        "securable_mismatch",
        "securable={} grantRec={}",
        securable,
        grantRecord);
    diagnostics.check(
        grantee.getCatalogId() == grantRecord.getGranteeCatalogId()
            && grantee.getId() == grantRecord.getGranteeId(),
        "grantee_mismatch",
        "grantee={} grantRec={}",
        grantee,
        grantRecord);
    diagnostics.check(grantee.getType().isGrantee(), "not_a_grantee", "grantee={}", grantee);

    PolarisBaseEntity granteeEntity =
        RecordRefs.mustLoadEntity(primitives, diagnostics, grantee, "missing_grantee");
    PolarisBaseEntity securableEntity =
        RecordRefs.mustLoadEntity(primitives, diagnostics, securable, "missing_securable");

    List<Mutation> mutations =
        List.of(
            Mutation.of(
                PolarisRecordKinds.GRANT_RECORD,
                Mutation.Op.DELETE,
                RecordRefs.grantIdentity(grantRecord),
                null),
            RecordMutations.bumpGrantRecordsVersion(granteeEntity).mutation(),
            RecordMutations.bumpGrantRecordsVersion(securableEntity).mutation());

    OrchestrationResult result = orchestrator.commit(mutations);
    return result.isApplied() ? new PrivilegeResult(grantRecord) : mapFailedGrantMutation(result);
  }

  /**
   * Maps a non-applied grant/revoke {@link OrchestrationResult} to a {@link PrivilegeResult}. New
   * mapping, not ported: the old model never fails atomically here at all (each of its three writes
   * is an independent primitive call with no shared transaction across all three), so there is no
   * old-model precedent for what an orchestrated failure means. {@code
   * TARGET_ENTITY_CONCURRENTLY_MODIFIED} is reused from {@code
   * updateEntityPropertiesIfNotChanged}'s existing {@code RetryOnConcurrencyException} mapping as
   * the closest established meaning for "the grantee or securable changed between the read and the
   * commit" — this call path never returned that status before.
   */
  private PrivilegeResult mapFailedGrantMutation(@NonNull OrchestrationResult result) {
    if (result.outcome() == OrchestrationResult.Outcome.ROLLBACK_INCOMPLETE) {
      return new PrivilegeResult(
          BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED,
          "rollback incomplete: "
              + result.uncompensated().size()
              + " mutation(s) require admin reclamation");
    }
    CommitResult.Failure failure = result.groupFailure().orElseThrow().failure().orElseThrow();
    return result.isRefusedAndRolledBack()
        ? new PrivilegeResult(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, null)
        : new PrivilegeResult(
            BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED, failure.toString());
  }

  /**
   * Shared by {@link #loadGrantsOnSecurable} and {@link #loadGrantsToGrantee}: read the anchor
   * entity's {@code grantRecordsVersion} first, treating its absence as {@code ENTITY_NOT_FOUND} —
   * that is how both old impls infer the entity exists at all ({@code
   * lookupEntityGrantRecordsVersion} returning {@code 0}), translated here to this store's cleaner
   * absence signal ({@code Optional.empty()}) rather than a sentinel int, not a separate existence
   * read. Then list the declared path and batch-fetch the distinct counterpart entities, dropping
   * the ones no longer resolvable — a grant referencing a dropped grantee/securable disappears from
   * the resolved view, same as both old impls' {@code entities.stream().filter(Objects::nonNull)}.
   */
  private LoadGrantsResult loadGrants(
      long anchorCatalogId,
      long anchorId,
      @NonNull LookupPath path,
      @NonNull ToLongFunction<PolarisGrantRecord> counterpartId) {
    Optional<RecordVersions> anchorVersions =
        primitives.versionsOf(List.of(RecordRefs.entityIdentity(anchorId))).get(0);
    if (anchorVersions.isEmpty()) {
      return new LoadGrantsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }
    int grantsVersion = (int) anchorVersions.get().grantRecordsVersion();

    List<PolarisGrantRecord> grantRecords =
        primitives
            .list(
                PolarisRecordKinds.GRANT_RECORD,
                path,
                List.of(anchorCatalogId, anchorId),
                PageToken.readEverything(),
                PolarisGrantRecord.class)
            .items();

    List<RecordRef> counterpartRefs =
        grantRecords.stream()
            .mapToLong(counterpartId::applyAsLong)
            .distinct()
            .mapToObj(RecordRefs::entityIdentity)
            .toList();
    List<PolarisBaseEntity> entities =
        primitives.getMany(counterpartRefs, PolarisBaseEntity.class).stream()
            .filter(Optional::isPresent)
            .map(Optional::get)
            .toList();

    return new LoadGrantsResult(grantsVersion, grantRecords, entities);
  }

  @Override
  public @NonNull PrivilegeResult grantUsageOnRoleToGrantee(
      @NonNull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @NonNull PolarisEntityCore role,
      @NonNull PolarisEntityCore grantee) {
    diagnostics.check(grantee.getType().isGrantee(), "not_a_grantee", "grantee={}", grantee);
    // Ported verbatim from AtomicOperationMetaStoreManager: which usage privilege to grant is
    // decided by the GRANTEE's type, not by whether `role` is a catalog role or a principal role.
    PolarisPrivilege usagePriv =
        grantee.getType() == PolarisEntityType.PRINCIPAL_ROLE
            ? PolarisPrivilege.CATALOG_ROLE_USAGE
            : PolarisPrivilege.PRINCIPAL_ROLE_USAGE;
    return persistNewGrantRecord(role, grantee, usagePriv);
  }

  @Override
  public @NonNull PrivilegeResult revokeUsageOnRoleFromGrantee(
      @NonNull PolarisCallContext callCtx,
      @Nullable PolarisEntityCore catalog,
      @NonNull PolarisEntityCore role,
      @NonNull PolarisEntityCore grantee) {
    PolarisPrivilege usagePriv =
        grantee.getType() == PolarisEntityType.PRINCIPAL_ROLE
            ? PolarisPrivilege.CATALOG_ROLE_USAGE
            : PolarisPrivilege.PRINCIPAL_ROLE_USAGE;
    PolarisGrantRecord grantRecord =
        primitives
            .get(
                RecordRef.byIdentity(
                    PolarisRecordKinds.GRANT_RECORD,
                    List.of(
                        role.getCatalogId(),
                        role.getId(),
                        grantee.getCatalogId(),
                        grantee.getId(),
                        usagePriv.getCode())),
                PolarisGrantRecord.class)
            .orElse(null);
    if (grantRecord == null) {
      return new PrivilegeResult(BaseResult.ReturnStatus.GRANT_NOT_FOUND, null);
    }
    return revokeGrantRecord(role, grantee, grantRecord);
  }

  @Override
  public @NonNull PrivilegeResult grantPrivilegeOnSecurableToRole(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityCore securable,
      @NonNull PolarisPrivilege privilege) {
    // catalogPath is accepted but not consulted, same parity choice as createEntityIfNotExists
    // (see catalogIdOf's javadoc): AtomicOperationMetaStoreManager's
    // grantPrivilegeOnSecurableToRole never touches it either.
    return persistNewGrantRecord(securable, grantee, privilege);
  }

  @Override
  public @NonNull PrivilegeResult revokePrivilegeOnSecurableFromRole(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore grantee,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityCore securable,
      @NonNull PolarisPrivilege privilege) {
    PolarisGrantRecord grantRecord =
        primitives
            .get(
                RecordRef.byIdentity(
                    PolarisRecordKinds.GRANT_RECORD,
                    List.of(
                        securable.getCatalogId(),
                        securable.getId(),
                        grantee.getCatalogId(),
                        grantee.getId(),
                        privilege.getCode())),
                PolarisGrantRecord.class)
            .orElse(null);
    if (grantRecord == null) {
      return new PrivilegeResult(BaseResult.ReturnStatus.GRANT_NOT_FOUND, null);
    }
    return revokeGrantRecord(securable, grantee, grantRecord);
  }

  @Override
  public @NonNull LoadGrantsResult loadGrantsOnSecurable(
      @NonNull PolarisCallContext callCtx, PolarisEntityCore securable) {
    return loadGrants(
        securable.getCatalogId(),
        securable.getId(),
        PolarisRecordKinds.GRANT_RECORD_BY_SECURABLE,
        PolarisGrantRecord::getGranteeId);
  }

  @Override
  public @NonNull LoadGrantsResult loadGrantsToGrantee(
      @NonNull PolarisCallContext callCtx, PolarisEntityCore grantee) {
    return loadGrants(
        grantee.getCatalogId(),
        grantee.getId(),
        PolarisRecordKinds.GRANT_RECORD_BY_GRANTEE,
        PolarisGrantRecord::getSecurableId);
  }
}
