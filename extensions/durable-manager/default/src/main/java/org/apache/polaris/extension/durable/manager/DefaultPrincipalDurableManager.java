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
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.spi.durable.DurableOrchestrator;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.OrchestrationResult;
import org.apache.polaris.spi.durable.Precondition;
import org.apache.polaris.spi.durable.PrincipalDurableManager;
import org.apache.polaris.spi.durable.RecordRef;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Default principal durable manager: creates a principal together with its secrets in one durable
 * operation and serves the principal-typed lookups (root principal, principal by id or name,
 * principal role by name). Generating a credential is a business rule and lives here, not in
 * storage.
 *
 * <p>Two doors, strictly divided: every write goes through the orchestrator's commit, every read
 * goes to the primitives handle directly. Owns its business rules and knows no storage topology;
 * authorization and request validation live above this layer.
 */
public class DefaultPrincipalDurableManager implements PrincipalDurableManager {
  private final PolarisDiagnostics diagnostics;
  private final DurableOrchestrator orchestrator;
  private final DurableRecordStore primitives;
  private final PrincipalSecretsGenerator secretsGenerator;

  public DefaultPrincipalDurableManager(
      @NonNull PolarisDiagnostics diagnostics,
      @NonNull DurableOrchestrator orchestrator,
      @NonNull DurableRecordStore primitives,
      @NonNull PrincipalSecretsGenerator secretsGenerator) {
    this.diagnostics = diagnostics;
    this.orchestrator = orchestrator;
    this.primitives = primitives;
    this.secretsGenerator = secretsGenerator;
  }

  /**
   * Ported from {@code AtomicOperationMetaStoreManager#createPrincipal} / {@code
   * TransactionalMetaStoreManagerImpl#createPrincipal}, which diverge and this picks one, disclosed
   * rather than silently: Atomic generates secrets unconditionally, writes the principal, and on an
   * {@code ENTITY_ALREADY_EXISTS} collision compensates with a call to {@code
   * deletePrincipalSecrets} — its own TODO concedes a crash between that write and the compensating
   * delete leaks the secrets row. Transactional checks the name first and only then generates
   * secrets, needing no compensation at all. This matches Transactional's shape: C7 prescribes
   * resolving reads before writes, and it wastes no generated secret on a name that was already
   * taken.
   *
   * <p>The WRITE ORDER inside the one commit still matches BOTH old impls: secrets before the
   * principal. The reason survives the move from two independent primitive writes to one
   * orchestrated commit: in a multi-store deployment where {@code PRINCIPAL_SECRETS} and {@code
   * ENTITY} resolve to different stores, this becomes two orchestrated groups rather than one, and
   * {@link DurableOrchestrator}'s own disclosed, un-closed crash window — the process dying between
   * committing group 1 and compensating a group-2 failure — can leave the first group's effect
   * stranded. Secrets first means that stranded state is an orphan {@code PRINCIPAL_SECRETS} row:
   * inert, nothing references it. Principal first would instead strand an orphan {@code PRINCIPAL}
   * entity whose {@code clientId} resolves to nothing — unusable. Same rationale ADR-0002 records
   * for the "never a principal without secrets" invariant itself.
   */
  @Override
  public @NonNull CreatePrincipalResult createPrincipal(
      @NonNull PolarisCallContext callCtx, @NonNull PrincipalEntity principal) {
    diagnostics.checkNotNull(principal, "unexpected_null_principal");

    Optional<PolarisBaseEntity> existing =
        primitives.get(RecordRefs.entityIdentity(principal.getId()), PolarisBaseEntity.class);
    if (existing.isPresent()) {
      // Same-id idempotent-retry collisions are necessarily sequential (the id was already
      // reserved by generateNewEntityId before this call reached us), so this pre-check needs no
      // atomicity of its own — matches both old impls' own comment to this effect.
      return loadExistingPrincipal(existing.get());
    }

    boolean nameTaken =
        primitives
            .get(
                RecordRefs.entityUniqueness(
                    PolarisEntityConstants.getRootEntityId(),
                    PolarisEntityType.PRINCIPAL.getCode(),
                    principal.getName()),
                PolarisBaseEntity.class)
            .isPresent();
    if (nameTaken) {
      return new CreatePrincipalResult(BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, null);
    }

    PolarisPrincipalSecrets secrets = generateUniqueSecrets(principal.getName(), principal.getId());
    PrincipalEntity updatedPrincipal =
        new PrincipalEntity.Builder(principal).setClientId(secrets.getPrincipalClientId()).build();
    PolarisBaseEntity prepared = prepareNewEntity(updatedPrincipal);
    RecordRef principalUniqueness =
        RecordRefs.entityUniqueness(
            prepared.getParentId(), prepared.getTypeCode(), prepared.getName());

    List<Mutation> mutations =
        List.of(
            Mutation.of(
                PolarisRecordKinds.PRINCIPAL_SECRETS,
                Mutation.Op.CREATE,
                RecordRefs.secretsIdentity(secrets.getPrincipalClientId()),
                secrets,
                List.of(Precondition.none())),
            Mutation.of(
                PolarisRecordKinds.ENTITY,
                Mutation.Op.CREATE,
                RecordRefs.entityIdentity(prepared.getId()),
                prepared,
                List.of(Precondition.notExists(principalUniqueness))));

    OrchestrationResult result = orchestrator.commit(mutations);
    if (result.isApplied()) {
      return new CreatePrincipalResult(prepared, secrets);
    }
    // No compensating delete of the secrets on failure: the orchestrator's own cross-group
    // compensation already rolls back a committed earlier group when a later one fails, which is
    // strictly better than Atomic's manual best-effort cleanup for the ordinary (non-crash)
    // failure case.
    BaseResult.ReturnStatus failureStatus = RecordMutations.classifyFailedCreate(result);
    if (failureStatus == BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS) {
      // Finding 1 (independent review, 2026-08-18): a lost race can mean someone else already
      // committed THIS exact principal (the id this call reserved before it started) rather than
      // a genuine name conflict. Re-reading by identity rather than by principalUniqueness is
      // deliberate and simpler than createEntityIfNotExists's equivalent check: ids are reserved
      // by the caller before this method runs, so a hit here is necessarily this exact id — no
      // separate id-equality comparison is needed the way it is for a uniqueness-keyed read,
      // which could belong to any id.
      Optional<PolarisBaseEntity> winner =
          primitives.get(RecordRefs.entityIdentity(prepared.getId()), PolarisBaseEntity.class);
      if (winner.isPresent()) {
        return loadExistingPrincipal(winner.get());
      }
    }
    return new CreatePrincipalResult(failureStatus, RecordMutations.failureDetail(result));
  }

  /**
   * Loads the existing principal's canonical (entity, secrets) pair by id — shared by {@link
   * #createPrincipal}'s identity pre-check (this id already exists) and its lost-race branch
   * (Finding 1: the SAME rule now applies at both of a create path's collision points).
   */
  private CreatePrincipalResult loadExistingPrincipal(@NonNull PolarisBaseEntity existing) {
    PrincipalEntity refreshPrincipal = PrincipalEntity.of(existing);
    String clientId = refreshPrincipal.getClientId();
    diagnostics.checkNotNull(clientId, "null_client_id", "principal={}", refreshPrincipal);
    diagnostics.check(!clientId.isEmpty(), "empty_client_id", "principal={}", refreshPrincipal);
    PolarisPrincipalSecrets secrets =
        primitives
            .get(RecordRefs.secretsIdentity(clientId), PolarisPrincipalSecrets.class)
            .orElse(null);
    diagnostics.checkNotNull(
        secrets,
        "missing_principal_secrets",
        "clientId={} principal={}",
        clientId,
        refreshPrincipal);
    return new CreatePrincipalResult(existing, secrets);
  }

  /**
   * Ported from {@code TreeMapDurablePrimitivesImpl#generateNewPrincipalSecretsInCurrentTxn}'s
   * collision-avoidance loop: {@link #secretsGenerator} produces a client id that is expected to be
   * unique but not reserved the way {@link DurableRecordStore#generateNewId} reserves an entity id,
   * so this re-checks and retries rather than trusting the generator outright.
   */
  private PolarisPrincipalSecrets generateUniqueSecrets(
      @NonNull String principalName, long principalId) {
    DurableRecordStore store = primitives;
    PolarisPrincipalSecrets candidate;
    do {
      candidate = secretsGenerator.produceSecrets(principalName, principalId);
    } while (store
        .get(
            RecordRefs.secretsIdentity(candidate.getPrincipalClientId()),
            PolarisPrincipalSecrets.class)
        .isPresent());
    return candidate;
  }

  /**
   * Ported from {@code BaseMetaStoreManager#prepareToPersistNewEntity}: validates the invariants a
   * new entity must hold, then stamps the fields the persistence layer owns. No clock usage to port
   * — the source method never calls one, it only validates that the caller already filled in {@code
   * createTimestamp} (which {@link PolarisBaseEntity.Builder#build} backfills to "now" if left at
   * 0, so the check is a defensive invariant rather than a live path).
   */
  private PolarisBaseEntity prepareNewEntity(@NonNull PolarisBaseEntity entity) {
    diagnostics.checkNotNull(entity, "unexpected_null_entity");
    diagnostics.checkNotNull(entity.getName(), "unexpected_null_name", "entity={}", entity);
    PolarisEntityType type = PolarisEntityType.fromCode(entity.getTypeCode());
    diagnostics.checkNotNull(type, "unknown_type", "entity={}", entity);
    PolarisEntitySubType subType = PolarisEntitySubType.fromCode(entity.getSubTypeCode());
    diagnostics.checkNotNull(subType, "unexpected_null_subType", "entity={}", entity);
    diagnostics.check(
        subType.getParentType() == null || subType.getParentType() == type,
        "invalid_subtype",
        "type={} subType={}",
        type,
        subType);
    diagnostics.check(
        !type.isTopLevel() || entity.getParentId() == PolarisEntityConstants.getRootEntityId(),
        "top_level_parent_should_be_account",
        "entity={}",
        entity);
    diagnostics.check(
        entity.getId() != 0 || type == PolarisEntityType.ROOT, "id_not_set", "entity={}", entity);
    diagnostics.check(entity.getCreateTimestamp() != 0, "null_create_timestamp");

    return new PolarisBaseEntity.Builder(entity)
        .lastUpdateTimestamp(entity.getCreateTimestamp())
        .dropTimestamp(0)
        .purgeTimestamp(0)
        .toPurgeTimestamp(0)
        .build();
  }

  // ---------------------------------------------------------- DurableManager (ticket 91)

  private @NonNull EntityResult readEntityByName(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull String name) {
    long parentId = RecordRefs.parentIdOf(catalogPath);
    Optional<PolarisBaseEntity> found =
        primitives.get(
            RecordRefs.entityUniqueness(parentId, entityType.getCode(), name),
            PolarisBaseEntity.class);
    // Shipped rule, ported verbatim from AtomicOperationMetaStoreManager#readEntityByName: a
    // subtype mismatch reads as not-found unless the caller asked for ANY_SUBTYPE. The uniqueness
    // key carries no subtype component, so this check happens after the read, not as part of it.
    if (found.isPresent()
        && entitySubType != PolarisEntitySubType.ANY_SUBTYPE
        && found.get().getSubTypeCode() != entitySubType.getCode()) {
      found = Optional.empty();
    }
    return found
        .<EntityResult>map(EntityResult::new)
        .orElseGet(() -> new EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null));
  }

  /**
   * CORRECTION to increment 2's version of this method (found by {@code testLookup}, which asserts
   * that looking up a real id under the wrong {@code entityType} — a namespace's id looked up as a
   * {@code TABLE_LIKE} — returns not-found): {@code entityType} DOES filter, verified in {@code
   * TreeMapDurablePrimitivesImpl#lookupEntityInCurrentTxn}'s actual body — {@code if (entity !=
   * null && entity.getTypeCode() != typeCode) return null;} — not merely "MAY" as the abstract
   * {@code DurablePrimitives#lookupEntity} javadoc alone would suggest ("The type code parameter is
   * redundant..."). Increment 2 read only the javadoc and picked the weaker of the two documented
   * options; this reads the concrete backend the fixture actually exercises and matches its real
   * behavior. {@code entityCatalogId} remains genuinely unused: the new model's ENTITY identity key
   * is {@code (realm, id)} alone, with no catalog component for identity lookups to filter on at
   * all.
   *
   * <p><b>SECOND CORRECTION (ticket 92, increment 4): the paragraph above's "entityCatalogId
   * remains genuinely unused" was itself the same class of error it corrects.</b> It was written
   * while {@code testEntityCache} was disabled; that case's negative lookup ({@code
   * loadCacheEntryById(N1.getCatalogId() + 1000, ...)} expecting not-found) observes that the old
   * {@code lookupEntity} filters on {@code catalog_id} as well — the shipped query's three filter
   * columns, the same fact the {@code 92c82b995}/{@code e29358a37} row in the chain already
   * recorded for the children query. The identity KEY carries no catalog component, so the store
   * fetch stays by id; the catalog filter is applied here on the fetched row, the same treatment
   * the type filter above already gets.
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

  @Override
  public Optional<PrincipalEntity> findRootPrincipal(PolarisCallContext polarisCallContext) {
    return findPrincipalByName(polarisCallContext, PolarisEntityConstants.getRootPrincipalName());
  }

  @Override
  public Optional<PrincipalEntity> findPrincipalById(
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

  @Override
  public Optional<PrincipalEntity> findPrincipalByName(
      PolarisCallContext polarisCallContext, String principalName) {
    EntityResult entityResult =
        readEntityByName(
            polarisCallContext,
            null,
            PolarisEntityType.PRINCIPAL,
            PolarisEntitySubType.NULL_SUBTYPE,
            principalName);
    if (!entityResult.isSuccess()) {
      return Optional.empty();
    }
    return Optional.of(entityResult.getEntity()).map(PrincipalEntity::of);
  }

  @Override
  public Optional<PrincipalRoleEntity> findPrincipalRoleByName(
      PolarisCallContext polarisCallContext, String principalRoleName) {
    EntityResult entityResult =
        readEntityByName(
            polarisCallContext,
            null,
            PolarisEntityType.PRINCIPAL_ROLE,
            PolarisEntitySubType.NULL_SUBTYPE,
            principalRoleName);
    if (!entityResult.isSuccess()) {
      return Optional.empty();
    }
    return Optional.of(entityResult.getEntity()).map(PrincipalRoleEntity::of);
  }
}
