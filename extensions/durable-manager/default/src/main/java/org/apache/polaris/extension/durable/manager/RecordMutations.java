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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.OrchestrationResult;
import org.apache.polaris.spi.durable.Precondition;
import org.apache.polaris.spi.durable.RecordRef;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Mutation and precondition builders shared by the durable manager implementations in this package,
 * plus the mapping from an orchestration failure to the result status the previous model reported.
 * Stateless; carries no business rule.
 */
final class RecordMutations {
  private RecordMutations() {}

  /**
   * EJ's retrofit (2026-08-17): one {@link Precondition#exists} per distinct entity across both
   * path arguments, so the store checks at commit time that every element the caller resolved this
   * write against is still there. Path entities are {@code ENTITY} records like the write target
   * they gate, so they share the atomicity domain and add no extra round trip.
   *
   * @param extraPath a second path to fold in, deduplicated against {@code path} by id — {@code
   *     DefaultCatalogDurableManager#renameEntity} passes the destination path alongside the source
   *     path, and {@code DefaultPolicyDurableManager}'s attach and detach pass the policy's catalog
   *     path alongside the target's
   */
  static List<Precondition> pathExistsPreconditions(
      @Nullable List<PolarisEntityCore> path, @Nullable List<PolarisEntityCore> extraPath) {
    return pathIds(path, extraPath).stream()
        .map(id -> Precondition.exists(RecordRefs.entityIdentity(id)))
        .toList();
  }

  /** The identity refs {@link #pathExistsPreconditions} declared, for mapping a failure back. */
  static Set<RecordRef> pathRefs(
      @Nullable List<PolarisEntityCore> path, @Nullable List<PolarisEntityCore> extraPath) {
    Set<RecordRef> refs = new HashSet<>();
    for (long id : pathIds(path, extraPath)) {
      refs.add(RecordRefs.entityIdentity(id));
    }
    return refs;
  }

  static Set<Long> pathIds(
      @Nullable List<PolarisEntityCore> path, @Nullable List<PolarisEntityCore> extraPath) {
    Set<Long> ids = new HashSet<>();
    if (path != null) {
      path.forEach(e -> ids.add(e.getId()));
    }
    if (extraPath != null) {
      extraPath.forEach(e -> ids.add(e.getId()));
    }
    return ids;
  }

  /**
   * True when {@code result}'s reported failed preconditions include one whose {@link
   * Precondition#ref()} names a path entity — distinguishes a stale {@code catalogPath} from an
   * ordinary uniqueness/version race on the same commit. Relies on {@code
   * CommitResult#failedPreconditions()}'s own disclosure that a store may report only a subset (at
   * least one, per {@code TreeMapDurableRecordStore}'s stop-at-first-failure behavior verified in
   * increment 3): this checks membership rather than counting, so reporting one is enough.
   */
  static boolean failedOnPath(
      @NonNull OrchestrationResult result, @NonNull Set<RecordRef> pathRefs) {
    return result.groupFailure().map(CommitResult::failedPreconditions).orElse(List.of()).stream()
        .anyMatch(p -> p.ref().filter(pathRefs::contains).isPresent());
  }

  /**
   * Maps a non-applied {@code createPrincipal}/{@code createCatalog} {@link OrchestrationResult}.
   * Having already pre-checked the relevant uniqueness before building the mutation list, a failure
   * here can only be a lost race on that same check — the identical collision the pre-check path
   * itself reports — or a genuine bug (TOO_MANY_ITEMS, DOMAIN_MISMATCH, ROLLBACK_INCOMPLETE). No
   * old-model precedent for this mapping exists for the same reason {@code
   * DefaultCatalogDurableManager#mapFailedCreate} has none: the old primitives interface has no
   * multi-outcome commit result, and for {@code createCatalog}/{@code createPrincipal}
   * specifically, neither old impl wraps its several writes in one shared transaction at all (see
   * {@code DefaultCatalogDurableManager#createCatalog}'s and {@code
   * DefaultPrincipalDurableManager#createPrincipal}'s own javadoc for what the one-commit shape
   * closes as a side effect).
   */
  static BaseResult.ReturnStatus classifyFailedCreate(@NonNull OrchestrationResult result) {
    if (result.outcome() == OrchestrationResult.Outcome.ROLLBACK_INCOMPLETE) {
      return BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED;
    }
    CommitResult.Failure failure = result.groupFailure().orElseThrow().failure().orElseThrow();
    return failure == CommitResult.Failure.PRECONDITION_FAILED
        ? BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS
        : BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED;
  }

  /** {@code extraInformation} for a {@link #classifyFailedCreate} mapping, when non-null helps. */
  static @Nullable String failureDetail(@NonNull OrchestrationResult result) {
    if (result.outcome() == OrchestrationResult.Outcome.ROLLBACK_INCOMPLETE) {
      return "rollback incomplete: "
          + result.uncompensated().size()
          + " mutation(s) require admin reclamation";
    }
    CommitResult.Failure failure = result.groupFailure().orElseThrow().failure().orElseThrow();
    return failure == CommitResult.Failure.PRECONDITION_FAILED ? null : failure.toString();
  }

  /**
   * The two-precondition {@code ENTITY} UPDATE shared by {@code
   * DefaultCatalogDurableManager#updateEntityPropertiesIfNotChanged} and its batch form: both
   * halves of the CAS {@code checkConditionsForWriteEntityInCurrentTxn} performs (record version
   * AND grant-records version, both asserted unchanged), gating the write that carries the new
   * {@code properties}/{@code internalProperties} state.
   */
  static Mutation entityPropertiesUpdateMutation(
      @NonNull RecordRef ref,
      @NonNull PolarisBaseEntity current,
      @NonNull PolarisBaseEntity updated) {
    return Mutation.of(
        PolarisRecordKinds.ENTITY,
        Mutation.Op.UPDATE,
        ref,
        updated,
        List.of(
            Precondition.versionEquals(
                ref, Precondition.VersionAttribute.RECORD_VERSION, current.getEntityVersion()),
            Precondition.versionEquals(
                ref,
                Precondition.VersionAttribute.GRANT_RECORDS_VERSION,
                current.getGrantRecordsVersion())));
  }

  /**
   * A grant-record {@code CREATE} mutation, declaring {@link Precondition#none()} per {@link
   * Mutation.Op#CREATE}'s contract for a kind whose identity and uniqueness are the same tuple. See
   * {@code DefaultGrantDurableManager#persistNewGrantRecord}'s javadoc for why that contract is not
   * yet honored by either shipped store's actual {@code CREATE} handling, and why this method still
   * declares it.
   */
  static Mutation createGrantMutation(@NonNull PolarisGrantRecord grantRecord) {
    return Mutation.of(
        PolarisRecordKinds.GRANT_RECORD,
        Mutation.Op.CREATE,
        RecordRefs.grantIdentity(grantRecord),
        grantRecord,
        List.of(Precondition.none()));
  }

  /**
   * One entity's {@code grantRecordsVersion} bump: the mutation to commit, and the resulting entity
   * state. Returning the updated state (rather than just the {@link Mutation}) lets a caller
   * building several grants against the SAME entity within one mutation list — {@code
   * DefaultCatalogDurableManager#createCatalog}'s catalog and admin role, each touched by more than
   * one grant — thread the running version forward between them instead of re-reading the store in
   * between.
   */
  record VersionBump(Mutation mutation, PolarisBaseEntity updated) {}

  /**
   * Gated by both halves of the two-column CAS the relational store's {@code entity_version}/
   * {@code grant_records_version} comparison performs: {@code entityVersion} is asserted unchanged,
   * never bumped here — only {@code grantRecordsVersion} moves, matching both old impls' {@code
   * entity.withGrantRecordsVersion(entity.getGrantRecordsVersion() + 1)}.
   */
  static VersionBump bumpGrantRecordsVersion(@NonNull PolarisBaseEntity entity) {
    RecordRef ref = RecordRefs.entityIdentity(entity.getId());
    PolarisBaseEntity updated = entity.withGrantRecordsVersion(entity.getGrantRecordsVersion() + 1);
    Mutation mutation =
        Mutation.of(
            PolarisRecordKinds.ENTITY,
            Mutation.Op.UPDATE,
            ref,
            updated,
            List.of(
                Precondition.versionEquals(
                    ref, Precondition.VersionAttribute.RECORD_VERSION, entity.getEntityVersion()),
                Precondition.versionEquals(
                    ref,
                    Precondition.VersionAttribute.GRANT_RECORDS_VERSION,
                    entity.getGrantRecordsVersion())));
    return new VersionBump(mutation, updated);
  }

  /**
   * The {@code ENTITY} UPDATE that persists a changed {@code internalPropertiesAsMap}, bumping only
   * {@code entityVersion} — ported from {@code AtomicOperationMetaStoreManager}'s / {@code
   * TransactionalMetaStoreManagerImpl}'s {@code rotatePrincipalSecrets}, which bump entityVersion
   * but never grantRecordsVersion for this write, so only one half of the two-column CAS {@link
   * #bumpGrantRecordsVersion} uses applies here.
   */
  static Mutation internalPropertiesMutation(
      @NonNull PolarisBaseEntity current, @NonNull Map<String, String> internalProperties) {
    RecordRef ref = RecordRefs.entityIdentity(current.getId());
    PolarisBaseEntity updated =
        new PolarisBaseEntity.Builder(current)
            .internalPropertiesAsMap(internalProperties)
            .entityVersion(current.getEntityVersion() + 1)
            .build();
    return Mutation.of(
        PolarisRecordKinds.ENTITY,
        Mutation.Op.UPDATE,
        ref,
        updated,
        List.of(
            Precondition.versionEquals(
                ref, Precondition.VersionAttribute.RECORD_VERSION, current.getEntityVersion())));
  }
}
