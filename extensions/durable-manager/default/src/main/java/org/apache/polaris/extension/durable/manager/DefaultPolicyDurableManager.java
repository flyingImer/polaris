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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.LoadPolicyMappingsResult;
import org.apache.polaris.core.persistence.dao.entity.PolicyAttachmentResult;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableOrchestrator;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.OrchestrationResult;
import org.apache.polaris.spi.durable.PolicyDurableManager;
import org.apache.polaris.spi.durable.Precondition;
import org.apache.polaris.spi.durable.RecordRef;
import org.jspecify.annotations.NonNull;

/**
 * Default policy-mapping durable manager: attach and detach policies to and from target entities,
 * and the policy reads by target. Enforces the one-inheritable-policy-per-type rule as a
 * manager-side pre-read.
 *
 * <p>Two doors, strictly divided: every write goes through the orchestrator's commit, every read
 * goes to the primitives handle directly. Owns its business rules and knows no storage topology;
 * authorization and request validation live above this layer.
 */
public class DefaultPolicyDurableManager implements PolicyDurableManager {
  private final PolarisDiagnostics diagnostics;
  private final DurableOrchestrator orchestrator;
  private final DurableRecordStore primitives;

  public DefaultPolicyDurableManager(
      @NonNull PolarisDiagnostics diagnostics,
      @NonNull DurableOrchestrator orchestrator,
      @NonNull DurableRecordStore primitives) {
    this.diagnostics = diagnostics;
    this.orchestrator = orchestrator;
    this.primitives = primitives;
  }

  /**
   * The policy-entity resolution behind both load methods: each mapping's policy by identity,
   * distinct, in record order, no type filter (matching the old id-only lookup). One DELIBERATE,
   * disclosed deviation from both old impls: their {@code loadPoliciesFromMappingRecords} hands the
   * old {@code lookupEntities} result through UNFILTERED, and that primitive returns a list
   * parallel to its input with {@code null} at unresolved positions (its javadoc and both shipped
   * backends agree) — so an orphaned mapping row surfaces to the caller as a null element, on which
   * the one production consumer ({@code PolicyCatalog#getPolicies}' inheritance walk) throws NPE.
   * This method drops unresolvable ids instead: the orphan-only failure mode becomes "fewer
   * policies returned" rather than a crash. Reachable only through a crash-orphaned mapping row —
   * ordinary drops clean mappings unconditionally in the same commit. (CORRECTION, this ticket's
   * refute pass: this javadoc's first draft claimed the old contract "skips" missing entities,
   * misquoting a javadoc that states the opposite — the old behaviour is null-passthrough, and the
   * skip here is a deviation to disclose, not parity to cite.)
   */
  private List<PolarisBaseEntity> policiesFromMappingRecords(
      @NonNull List<PolarisPolicyMappingRecord> mappingRecords) {
    List<RecordRef> refs =
        mappingRecords.stream()
            .mapToLong(PolarisPolicyMappingRecord::getPolicyId)
            .distinct()
            .mapToObj(RecordRefs::entityIdentity)
            .toList();
    return primitives.getMany(refs, PolarisBaseEntity.class).stream()
        .filter(Optional::isPresent)
        .map(Optional::get)
        .toList();
  }

  /**
   * The manager-owned attach rule (S3), ported from the check both old impls delegate to their
   * backends ({@code AbstractTransactionalPersistence
   * #checkConditionsForWriteToPolicyMappingRecordsInCurrentTxn} and {@code
   * JdbcDurablePrimitivesImpl #handleInheritablePolicy} implement the identical three-way branch):
   * an invalid policy type code is {@code UNEXPECTED_ERROR_SIGNALED "Unknown policy type"}; for an
   * INHERITABLE type, attaching a DIFFERENT policy of the same type as an existing mapping is
   * {@code POLICY_MAPPING_OF_SAME_TYPE_ALREADY_EXISTS}, while re-attaching the SAME policy updates
   * only the mapping's {@code parameters} in place; a non-inheritable type skips the same-type
   * check entirely (no shipped policy type is non-inheritable, so that branch has no old-behaviour
   * oracle — data model 5.1's own note; where old JDBC's raw INSERT would surface a duplicate
   * non-inheritable re-attach as an unchecked SQL-wrapping exception, this branch's
   * get-then-CREATE/UPDATE upserts the parameters cleanly — a dormant, disclosed difference until a
   * non-inheritable type exists).
   *
   * <h2>Disclosed divergence choices (fixture-silent, per ticket 91's precedent)</h2>
   *
   * <p><b>Path and endpoint validation.</b> {@code AtomicOperationMetaStoreManager} ignores both
   * catalogPath arguments and never checks that target or policy exist; {@code
   * TransactionalMetaStoreManagerImpl} re-resolves both paths (leaf entities included) inside its
   * transaction and returns {@code ENTITY_CANNOT_BE_RESOLVED} on failure. This class follows the
   * retrofit convention every OTHER write taking a catalogPath already uses (see {@code
   * RecordRefs#catalogIdOf}): {@code RecordMutations#pathExistsPreconditions} over BOTH paths rides
   * the commit, plus an {@code EXISTS} precondition on the target and the policy identities — the
   * same happens-before guarantee, here closing the leak of a mapping row written under a
   * concurrently-dropped target or policy (the unconditional drop-path cleanup in {@code
   * DefaultCatalogDurableManager#collectDropMutations} deletes mappings when an endpoint drops; a
   * mapping committed AFTER that cleanup read would survive it). Failure mapping: a failed path
   * precondition is {@code CATALOG_PATH_CANNOT_BE_RESOLVED} (matching the other retrofited writes),
   * a failed endpoint precondition is {@code ENTITY_CANNOT_BE_RESOLVED} (Transactional's status for
   * exactly this situation).
   *
   * <p><b>The same-type check is a manager-side pre-read, not a store condition.</b> "At most one
   * inheritable policy of a type per target" is a set-shaped rule the precondition vocabulary
   * deliberately cannot express (no set-emptiness conditions, ADR-0011), and the mapping key cannot
   * enforce it either (data model 5.1). The pre-read-then-commit window this leaves is not new: the
   * old JDBC path is an unguarded read-then-write over the same window, recorded as a live gap by
   * data model 5.1. A lost race on the mapping's own identity (the {@code NOT_EXISTS} below) maps
   * to {@code POLICY_MAPPING_OF_SAME_TYPE_ALREADY_EXISTS} — type-true (the colliding record IS the
   * same type) where old JDBC would propagate a raw uniqueness-violation exception and old TreeMap
   * serializes the race away; a lost race on the UPDATE branch's {@code EXISTS} (mapping detached
   * between pre-read and commit) maps to {@code UNEXPECTED_ERROR_SIGNALED}, since no old status
   * exists for it.
   */
  @Override
  public @NonNull PolicyAttachmentResult attachPolicyToEntity(
      @NonNull PolarisCallContext callCtx,
      @NonNull List<PolarisEntityCore> targetCatalogPath,
      @NonNull PolarisEntityCore target,
      @NonNull List<PolarisEntityCore> policyCatalogPath,
      @NonNull PolicyEntity policy,
      Map<String, String> parameters) {
    diagnostics.checkNotNull(target, "unexpected_null_target");
    diagnostics.checkNotNull(policy, "unexpected_null_policy");

    PolicyType policyType = PolicyType.fromCode(policy.getPolicyTypeCode());
    if (policyType == null) {
      return new PolicyAttachmentResult(
          BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED, "Unknown policy type");
    }

    PolarisPolicyMappingRecord mappingRecord =
        new PolarisPolicyMappingRecord(
            target.getCatalogId(),
            target.getId(),
            policy.getCatalogId(),
            policy.getId(),
            policy.getPolicyTypeCode(),
            parameters);
    RecordRef identity = RecordRefs.policyMappingIdentity(mappingRecord);

    boolean replaceExisting = false;
    if (policyType.isInheritable()) {
      List<PolarisPolicyMappingRecord> existingOfType =
          RecordRefs.policyMappingsOn(
                  primitives,
                  PolarisRecordKinds.POLICY_MAPPING_BY_TARGET,
                  target.getCatalogId(),
                  target.getId())
              .stream()
              .filter(r -> r.getPolicyTypeCode() == policy.getPolicyTypeCode())
              .toList();
      if (existingOfType.size() > 1) {
        return new PolicyAttachmentResult(
            BaseResult.ReturnStatus.POLICY_MAPPING_OF_SAME_TYPE_ALREADY_EXISTS,
            existingOfType.get(0).getPolicyTypeCode());
      }
      if (existingOfType.size() == 1) {
        PolarisPolicyMappingRecord existing = existingOfType.get(0);
        if (existing.getPolicyCatalogId() != policy.getCatalogId()
            || existing.getPolicyId() != policy.getId()) {
          return new PolicyAttachmentResult(
              BaseResult.ReturnStatus.POLICY_MAPPING_OF_SAME_TYPE_ALREADY_EXISTS,
              existing.getPolicyTypeCode());
        }
        replaceExisting = true;
      }
    } else {
      replaceExisting = primitives.get(identity, PolarisPolicyMappingRecord.class).isPresent();
    }

    List<Precondition> preconditions =
        new ArrayList<>(
            RecordMutations.pathExistsPreconditions(targetCatalogPath, policyCatalogPath));
    preconditions.add(Precondition.exists(RecordRefs.entityIdentity(target.getId())));
    preconditions.add(Precondition.exists(RecordRefs.entityIdentity(policy.getId())));
    preconditions.add(
        replaceExisting ? Precondition.exists(identity) : Precondition.notExists(identity));

    OrchestrationResult result =
        orchestrator.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.POLICY_MAPPING,
                    replaceExisting ? Mutation.Op.UPDATE : Mutation.Op.CREATE,
                    identity,
                    mappingRecord,
                    preconditions)));
    if (!result.isApplied()) {
      return mapFailedPolicyMappingWrite(
          result,
          RecordMutations.pathRefs(targetCatalogPath, policyCatalogPath),
          Set.of(
              RecordRefs.entityIdentity(target.getId()), RecordRefs.entityIdentity(policy.getId())),
          replaceExisting
              ? new PolicyAttachmentResult(
                  BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED,
                  "concurrent policy-mapping change")
              : new PolicyAttachmentResult(
                  BaseResult.ReturnStatus.POLICY_MAPPING_OF_SAME_TYPE_ALREADY_EXISTS,
                  mappingRecord.getPolicyTypeCode()));
    }
    return new PolicyAttachmentResult(mappingRecord);
  }

  /**
   * Ported from both old impls' {@code detachPolicyFromEntity}: resolve the mapping first, {@code
   * POLICY_MAPPING_NOT_FOUND} when absent (both agree), then delete it. Same disclosed
   * path-hardening as {@link #attachPolicyToEntity} (both catalogPath arguments ride as {@code
   * EXISTS} preconditions where Atomic ignores them and Transactional re-resolves), but no endpoint
   * preconditions: a mapping whose endpoint vanished concurrently is exactly what the delete
   * removes, and old Atomic happily detaches in that state. The {@code EXISTS} on the mapping's own
   * identity turns a detach that lost a race against another detach into {@code
   * POLICY_MAPPING_NOT_FOUND} — the same status the old, serialized second detach reports.
   */
  @Override
  public @NonNull PolicyAttachmentResult detachPolicyFromEntity(
      @NonNull PolarisCallContext callCtx,
      @NonNull List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityCore target,
      @NonNull List<PolarisEntityCore> policyCatalogPath,
      @NonNull PolicyEntity policy) {
    PolarisPolicyMappingRecord probe =
        new PolarisPolicyMappingRecord(
            target.getCatalogId(),
            target.getId(),
            policy.getCatalogId(),
            policy.getId(),
            policy.getPolicyTypeCode(),
            (Map<String, String>) null);
    RecordRef identity = RecordRefs.policyMappingIdentity(probe);
    PolarisPolicyMappingRecord mappingRecord =
        primitives.get(identity, PolarisPolicyMappingRecord.class).orElse(null);
    if (mappingRecord == null) {
      return new PolicyAttachmentResult(BaseResult.ReturnStatus.POLICY_MAPPING_NOT_FOUND, null);
    }

    List<Precondition> preconditions =
        new ArrayList<>(RecordMutations.pathExistsPreconditions(catalogPath, policyCatalogPath));
    preconditions.add(Precondition.exists(identity));
    OrchestrationResult result =
        orchestrator.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.POLICY_MAPPING,
                    Mutation.Op.DELETE,
                    identity,
                    null,
                    preconditions)));
    if (!result.isApplied()) {
      return mapFailedPolicyMappingWrite(
          result,
          RecordMutations.pathRefs(catalogPath, policyCatalogPath),
          Set.of(),
          new PolicyAttachmentResult(BaseResult.ReturnStatus.POLICY_MAPPING_NOT_FOUND, null));
    }
    return new PolicyAttachmentResult(mappingRecord);
  }

  /**
   * Maps a non-applied policy-mapping {@link OrchestrationResult}. No old-model precedent for the
   * same reason as {@code DefaultCatalogDurableManager#mapFailedCreate}; the per-caller {@code
   * onOwnIdentity} result carries the one mapping that differs between attach's two branches and
   * detach.
   */
  private PolicyAttachmentResult mapFailedPolicyMappingWrite(
      @NonNull OrchestrationResult result,
      @NonNull Set<RecordRef> pathRefs,
      @NonNull Set<RecordRef> endpointRefs,
      @NonNull PolicyAttachmentResult onOwnIdentity) {
    if (result.outcome() == OrchestrationResult.Outcome.ROLLBACK_INCOMPLETE) {
      return new PolicyAttachmentResult(
          BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED,
          "rollback incomplete: "
              + result.uncompensated().size()
              + " mutation(s) require admin reclamation");
    }
    CommitResult.Failure failure = result.groupFailure().orElseThrow().failure().orElseThrow();
    if (failure != CommitResult.Failure.PRECONDITION_FAILED) {
      return new PolicyAttachmentResult(
          BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED, failure.toString());
    }
    if (RecordMutations.failedOnPath(result, pathRefs)) {
      return new PolicyAttachmentResult(
          BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
    }
    if (!endpointRefs.isEmpty() && RecordMutations.failedOnPath(result, endpointRefs)) {
      return new PolicyAttachmentResult(BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RESOLVED, null);
    }
    return onOwnIdentity;
  }

  /**
   * Ported from both old impls' {@code loadPoliciesOnEntity}: {@code ENTITY_NOT_FOUND} when the
   * target does not resolve (by identity AND type, the same filtering {@link #loadEntity} ports),
   * then every mapping on the target with the policy entities resolved.
   */
  @Override
  public @NonNull LoadPolicyMappingsResult loadPoliciesOnEntity(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisEntityCore target) {
    if (!loadEntity(callCtx, target.getCatalogId(), target.getId(), target.getType()).isSuccess()) {
      return new LoadPolicyMappingsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }
    List<PolarisPolicyMappingRecord> mappingRecords =
        RecordRefs.policyMappingsOn(
            primitives,
            PolarisRecordKinds.POLICY_MAPPING_BY_TARGET,
            target.getCatalogId(),
            target.getId());
    return new LoadPolicyMappingsResult(mappingRecords, policiesFromMappingRecords(mappingRecords));
  }

  /**
   * Ported from both old impls' {@code loadPoliciesOnEntityByType}. The type narrowing happens in
   * this method, not at the store: {@code by-target}'s declared anchors are the target address
   * alone (data model 4.3), with no policy-type anchor — the same declaration gap as {@code
   * RecordRefs#listChildEntities}'s entity-type narrowing, and the same disclosure: the store
   * evaluates everything it CAN evaluate, only the undeclared dimension falls through to the
   * manager (old JDBC pushes the type into its WHERE clause through the old interface's dedicated
   * per-type method, which the new declared-path read side deliberately does not carry).
   */
  @Override
  public @NonNull LoadPolicyMappingsResult loadPoliciesOnEntityByType(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore target,
      @NonNull PolicyType policyType) {
    if (!loadEntity(callCtx, target.getCatalogId(), target.getId(), target.getType()).isSuccess()) {
      return new LoadPolicyMappingsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }
    List<PolarisPolicyMappingRecord> mappingRecords =
        RecordRefs.policyMappingsOn(
                primitives,
                PolarisRecordKinds.POLICY_MAPPING_BY_TARGET,
                target.getCatalogId(),
                target.getId())
            .stream()
            .filter(r -> r.getPolicyTypeCode() == policyType.getCode())
            .toList();
    return new LoadPolicyMappingsResult(mappingRecords, policiesFromMappingRecords(mappingRecords));
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
}
