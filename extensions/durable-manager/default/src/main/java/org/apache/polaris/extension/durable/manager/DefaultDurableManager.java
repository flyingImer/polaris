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

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.ChangeTrackingResult;
import org.apache.polaris.core.persistence.dao.entity.CreateCatalogResult;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.persistence.dao.entity.GenerateEntityIdResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.LoadGrantsResult;
import org.apache.polaris.core.persistence.dao.entity.LoadPolicyMappingsResult;
import org.apache.polaris.core.persistence.dao.entity.PolicyAttachmentResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.persistence.dao.entity.PrivilegeResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableManager;
import org.apache.polaris.spi.durable.DurableOrchestrator;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.GrantManager;
import org.apache.polaris.spi.durable.LookupPath;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.OrchestrationResult;
import org.apache.polaris.spi.durable.PolarisEventManager;
import org.apache.polaris.spi.durable.PolarisPolicyMappingManager;
import org.apache.polaris.spi.durable.Precondition;
import org.apache.polaris.spi.durable.RecordKind;
import org.apache.polaris.spi.durable.RecordRef;
import org.apache.polaris.spi.durable.RecordVersions;
import org.apache.polaris.spi.durable.SecretsManager;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The single new-model durable manager. At ticket 96 this class is a wholesale replacement for both
 * {@code AtomicOperationMetaStoreManager} and the transactional old-model manager it sits alongside
 * today; there is no per-method migration, the old implementations are deleted in one step once
 * this class covers their surface.
 *
 * <p>This manager owns every business rule and knows no storage topology. It implements {@link
 * DurableManager}, {@link GrantManager}, {@link SecretsManager}, {@link
 * PolarisPolicyMappingManager} and {@link PolarisEventManager} on one object because {@code
 * PolarisTestMetaStoreManager} (and, at ticket 96, every other caller of the old managers) narrows
 * the concrete instance it is handed to each of those sibling interfaces with a runtime cast; a
 * class missing one of them fails that cast, not a later call.
 *
 * <h2>Two handles, one floor</h2>
 *
 * <p>This manager legitimately holds two handles into the new model, and the difference between
 * them is a write/read split, not a "manager never sees primitives" rule. {@link #orchestrator} is
 * the WRITE door: it alone knows how to group a mutation list by atomicity domain, commit each
 * group, and compensate across groups on failure, so every write goes through it. {@link
 * #storeForKind} is the READ door and the source of {@link DurableRecordStore#generateNewId} — a
 * read may legitimately be organized by orchestration too (the same-backend read/write optimization
 * allowance recorded 2026-08-10), but it is not required to be, and this class does not use that
 * option: every read here goes straight to the primitives read operations resolved through {@code
 * storeForKind}. What this class may NOT do is go beneath that floor (EJ, 2026-08-17): {@code
 * storeForKind} resolves to a {@link DurableRecordStore} in a possibly multi-store assembly, which
 * is itself the primitives-layer handle in its target shape (see {@link DurableRecordStore}'s own
 * javadoc on the migration); a bare single-store field here would be storage-topology knowledge
 * this class does not have, and a kind-keyed resolver is not.
 *
 * <p>This class never reads the OLD {@link org.apache.polaris.spi.durable.DurablePrimitives} handle
 * carried on {@link PolarisCallContext}. That handle is the old model's write/read door and
 * reaching for it here would silently reintroduce the coupling this class exists to remove.
 */
public class DefaultDurableManager
    implements DurableManager,
        GrantManager,
        SecretsManager,
        PolarisPolicyMappingManager,
        PolarisEventManager {

  private final Clock clock;
  private final PolarisDiagnostics diagnostics;
  private final DurableOrchestrator orchestrator;
  private final Function<RecordKind, DurableRecordStore> storeForKind;

  public DefaultDurableManager(
      @NonNull Clock clock,
      @NonNull PolarisDiagnostics diagnostics,
      @NonNull DurableOrchestrator orchestrator,
      @NonNull Function<RecordKind, DurableRecordStore> storeForKind) {
    this.clock = clock;
    this.diagnostics = diagnostics;
    this.orchestrator = orchestrator;
    this.storeForKind = storeForKind;
  }

  // ---------------------------------------------------------------------------------- helpers

  private DurableRecordStore entityStore() {
    return storeForKind.apply(PolarisRecordKinds.ENTITY);
  }

  /** {@link PolarisRecordKinds#ENTITY}'s identity ref: {@code (realm, id)}, realm implicit. */
  private static RecordRef entityIdentity(long id) {
    return RecordRef.byIdentity(PolarisRecordKinds.ENTITY, List.of(id));
  }

  /**
   * {@link PolarisRecordKinds#ENTITY}'s uniqueness ref: {@code (parent, type, name)}, verified
   * against both shipped stores' bindings ({@code TreeMapDurableRecordStore}, {@code
   * JdbcDurableRecordStore}). Deliberately no catalog-id component: both bindings key uniqueness on
   * {@code (parentId, typeCode, name)} alone, because ids are realm-wide unique so parentId already
   * disambiguates across catalogs.
   */
  private static RecordRef entityUniqueness(long parentId, int typeCode, @NonNull String name) {
    return RecordRef.byUniquenessKey(PolarisRecordKinds.ENTITY, List.of(parentId, typeCode, name));
  }

  /**
   * Deliberate parity choice, matching {@code AtomicOperationMetaStoreManager} and diverging from
   * {@code TransactionalMetaStoreManagerImpl}: {@code catalogPath} is never re-resolved against the
   * store. For reads this derives catalogId/parentId directly from the path the same way Atomic
   * does (raw {@code 0L} there; the named constants here are the same value). For creates, see
   * {@link #createEntityIfNotExists}: catalogPath is not even consulted there, because Atomic
   * itself takes catalogId/parentId from the entity object, not from the path.
   *
   * <p>Consequence, disclosed rather than silently matched: this manager never returns {@code
   * CATALOG_PATH_CANNOT_BE_RESOLVED} or {@code ENTITY_CANNOT_BE_RESOLVED}, which is exactly {@code
   * AtomicOperationMetaStoreManager}'s behavior and diverges from {@code
   * TransactionalMetaStoreManagerImpl}, which re-resolves the path through the package-private
   * {@code PolarisEntityResolver} and can return both. The fixture does not discriminate between
   * the two (Atomic passes it today). Hardening the path check into {@code EXISTS} preconditions on
   * the path entities is available under C7 but is an improvement over parity, not parity itself,
   * and is out of scope here.
   */
  private static long catalogIdOf(@Nullable List<PolarisEntityCore> catalogPath) {
    return catalogPath == null || catalogPath.isEmpty()
        ? PolarisEntityConstants.getNullId()
        : catalogPath.get(0).getId();
  }

  private static long parentIdOf(@Nullable List<PolarisEntityCore> catalogPath) {
    return catalogPath == null || catalogPath.isEmpty()
        ? PolarisEntityConstants.getRootEntityId()
        : catalogPath.get(catalogPath.size() - 1).getId();
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

  /**
   * The children of one parent, narrowed by subtype at the store (a declared anchor) and by type in
   * this method (not a declared anchor).
   *
   * <p>{@link PolarisRecordKinds#ENTITY_BY_PARENT}'s declared anchors are the parent address {@code
   * (catalog, parent)} plus an optional trailing subtype code — verified by reading both shipped
   * stores' {@code PathBinding}s for the path. Neither declares a type-code anchor, so entityType
   * narrowing cannot be pushed to the store the way subtype narrowing can; it happens here, as a
   * plain in-memory filter over whatever the store returns. This is a real gap in the current
   * lookup-path declaration (both stores agree, so it is not an implementation slip), not a
   * caller-side filter of the kind the SPI otherwise forbids — the store still evaluates everything
   * it CAN evaluate, and only the undeclared dimension falls through to the manager.
   */
  private List<PolarisBaseEntity> listChildEntities(
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull PageToken pageToken) {
    long catalogId = catalogIdOf(catalogPath);
    long parentId = parentIdOf(catalogPath);
    List<Object> anchors =
        entitySubType == PolarisEntitySubType.ANY_SUBTYPE
            ? List.of(catalogId, parentId)
            : List.of(catalogId, parentId, entitySubType.getCode());
    Page<PolarisBaseEntity> page =
        entityStore()
            .list(
                PolarisRecordKinds.ENTITY,
                PolarisRecordKinds.ENTITY_BY_PARENT,
                anchors,
                pageToken,
                PolarisBaseEntity.class);
    return page.items().stream().filter(e -> e.getTypeCode() == entityType.getCode()).toList();
  }

  /**
   * Maps an {@link OrchestrationResult} that did not apply to the caller-facing result for a single
   * create, re-reading the uniqueness key on a lost race so the caller learns the winner's subtype
   * the same way the pre-check path reports it. There is no old-model precedent for this mapping —
   * the old primitives interface has no multi-outcome commit result to map from, it either
   * succeeds, throws, or (for a batch) partially applies inside one DB transaction — so this is a
   * new decision, not a ported one, and is named as such for review.
   */
  private EntityResult mapFailedCreate(
      @NonNull DurableRecordStore store,
      @NonNull RecordRef uniqueness,
      @NonNull OrchestrationResult result) {
    if (result.outcome() == OrchestrationResult.Outcome.ROLLBACK_INCOMPLETE) {
      return new EntityResult(
          BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED,
          "rollback incomplete: "
              + result.uncompensated().size()
              + " mutation(s) require admin reclamation");
    }
    CommitResult.Failure failure = result.groupFailure().orElseThrow().failure().orElseThrow();
    if (failure != CommitResult.Failure.PRECONDITION_FAILED) {
      // TOO_MANY_ITEMS / DOMAIN_MISMATCH on a single mutation is a caller or deployment bug, not
      // an ordinary race; surfacing it as ENTITY_ALREADY_EXISTS would misreport the cause.
      return new EntityResult(
          BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED, failure.toString());
    }
    // Lost the race between our pre-check read and the commit: something else created the same
    // (parent, type, name) in between. Re-read to report its subtype, mirroring the pre-check
    // path's own EntityResult(ENTITY_ALREADY_EXISTS, subTypeCode) shape.
    Optional<PolarisBaseEntity> winner = store.get(uniqueness, PolarisBaseEntity.class);
    return new EntityResult(
        BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS,
        winner.map(PolarisBaseEntity::getSubTypeCode).orElse(0));
  }

  // ---------------------------------------------------------- DurableManager (ticket 91)

  @Override
  public @NonNull EntityResult readEntityByName(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull String name) {
    long parentId = parentIdOf(catalogPath);
    Optional<PolarisBaseEntity> found =
        entityStore()
            .get(entityUniqueness(parentId, entityType.getCode(), name), PolarisBaseEntity.class);
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

  @Override
  public @NonNull ListEntitiesResult listEntities(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull PageToken pageToken) {
    List<EntityNameLookupRecord> records =
        listChildEntities(catalogPath, entityType, entitySubType, pageToken).stream()
            .map(EntityNameLookupRecord::new)
            .toList();
    return ListEntitiesResult.fromPage(Page.page(pageToken, records, null));
  }

  @Override
  public @NonNull Page<PolarisBaseEntity> listFullEntities(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull PageToken pageToken) {
    return Page.page(
        pageToken, listChildEntities(catalogPath, entityType, entitySubType, pageToken), null);
  }

  @Override
  public @NonNull GenerateEntityIdResult generateNewEntityId(@NonNull PolarisCallContext callCtx) {
    return new GenerateEntityIdResult(entityStore().generateNewId());
  }

  @Override
  public @NonNull CreatePrincipalResult createPrincipal(
      @NonNull PolarisCallContext callCtx, @NonNull PrincipalEntity principal) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull CreateCatalogResult createCatalog(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity catalog,
      @NonNull List<PolarisEntityCore> principalRoles) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull EntityResult createEntityIfNotExists(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entity) {
    diagnostics.checkNotNull(entity, "unexpected_null_entity");
    diagnostics.checkNotNull(entity.getName(), "unexpected_null_entity_name");

    // catalogPath is accepted but not consulted: AtomicOperationMetaStoreManager's own create path
    // takes catalogId/parentId from the entity object, never from catalogPath. See catalogIdOf's
    // javadoc for the parity choice this follows.
    PolarisBaseEntity prepared = prepareNewEntity(entity);
    DurableRecordStore store = entityStore();
    RecordRef uniqueness =
        entityUniqueness(prepared.getParentId(), prepared.getTypeCode(), prepared.getName());

    Optional<PolarisBaseEntity> existing = store.get(uniqueness, PolarisBaseEntity.class);
    if (existing.isPresent()) {
      // EXPLICIT, PROVISIONAL ASSUMPTION (EJ, 2026-08-17: not certain this is purely a business
      // rule, revisit if it causes trouble): "same id means idempotent create-retry; a different
      // id holding the name is a real conflict." In the old model this lives in the primitives
      // layer, not the manager:
      // AbstractTransactionalPersistence#checkConditionsForWriteEntityInCurrentTxn
      // throws EntityAlreadyExistsException on any name collision, and
      // AtomicOperationMetaStoreManager#persistNewEntity catches it and applies exactly this
      // id-equality test. We do the same test here, against a plain read instead of a caught
      // exception, because the new store's Precondition#notExists is a flat exists/not-exists
      // test with no id-aware exception to catch.
      return existing.get().getId() == prepared.getId()
          // Return the entity we were trying to create, not the stored one — matching
          // persistNewEntity's own comment: the caller should see what it asked to create, even
          // if a concurrent update landed on the stored row first.
          ? new EntityResult(prepared)
          : new EntityResult(
              BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, existing.get().getSubTypeCode());
    }

    OrchestrationResult result =
        orchestrator.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.ENTITY,
                    Mutation.Op.CREATE,
                    entityIdentity(prepared.getId()),
                    prepared,
                    List.of(Precondition.notExists(uniqueness)))));
    return result.isApplied()
        ? new EntityResult(prepared)
        : mapFailedCreate(store, uniqueness, result);
  }

  @Override
  public @NonNull EntitiesResult createEntitiesIfNotExist(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull List<? extends PolarisBaseEntity> entities) {
    DurableRecordStore store = entityStore();
    List<PolarisBaseEntity> resolved = new ArrayList<>(entities.size());
    List<Mutation> mutations = new ArrayList<>();

    // Same provisional assumption as createEntityIfNotExists, applied per entity. In the old
    // model the batch form of this rule lives one layer further down than the single-entity form:
    // AbstractTransactionalPersistence#writeEntities (around lines 254-265) catches
    // EntityAlreadyExistsException per entity inside its own transaction loop and swallows it when
    // the existing entity's id matches, rethrowing (aborting the whole batch) otherwise.
    // TreeMapDurablePrimitivesImpl extends AbstractTransactionalPersistence without overriding
    // writeEntities, so both old managers get the rule "for free" from the primitives layer for the
    // batch case. The new store's commit has no such per-mutation swallow — a failed precondition
    // fails the whole commit — so S3 puts the rule here, in the manager, explicitly and by hand.
    for (PolarisBaseEntity entity : entities) {
      PolarisBaseEntity prepared = prepareNewEntity(entity);
      RecordRef uniqueness =
          entityUniqueness(prepared.getParentId(), prepared.getTypeCode(), prepared.getName());
      Optional<PolarisBaseEntity> existing = store.get(uniqueness, PolarisBaseEntity.class);
      if (existing.isPresent() && existing.get().getId() != prepared.getId()) {
        // One real conflict fails the whole batch before anything is committed, matching
        // AtomicOperationMetaStoreManager#createEntitiesIfNotExist's own catch, which aborts
        // writeEntities() for the whole list regardless of how many other entities would have
        // succeeded (BaseDurableManagerTest#testCreateEntitiesWithConflict).
        return new EntitiesResult(
            BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS,
            String.format(
                "Existing entity id: '%s', type %s subtype %s",
                existing.get().getId(),
                existing.get().getTypeCode(),
                existing.get().getSubTypeCode()));
      }
      resolved.add(prepared);
      if (existing.isEmpty()) {
        mutations.add(
            Mutation.of(
                PolarisRecordKinds.ENTITY,
                Mutation.Op.CREATE,
                entityIdentity(prepared.getId()),
                prepared,
                List.of(Precondition.notExists(uniqueness))));
      }
      // else: idempotent retry, same id — no mutation needed, `resolved` already carries the
      // entity we were trying to create.
    }

    if (mutations.isEmpty()) {
      // Every entity in the batch was already there under a matching id.
      return new EntitiesResult(Page.fromItems(resolved));
    }

    OrchestrationResult result = orchestrator.commit(mutations);
    if (result.isApplied()) {
      return new EntitiesResult(Page.fromItems(resolved));
    }
    if (result.outcome() == OrchestrationResult.Outcome.ROLLBACK_INCOMPLETE) {
      return new EntitiesResult(
          BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED,
          "rollback incomplete: "
              + result.uncompensated().size()
              + " mutation(s) require admin reclamation");
    }
    CommitResult.Failure failure = result.groupFailure().orElseThrow().failure().orElseThrow();
    return failure == CommitResult.Failure.PRECONDITION_FAILED
        ? new EntitiesResult(
            BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS,
            "lost the race against a concurrent create for one or more entities in this batch")
        : new EntitiesResult(BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED, failure.toString());
  }

  @Override
  public @NonNull EntityResult updateEntityPropertiesIfNotChanged(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entity) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @NonNull PolarisCallContext callCtx, @NonNull List<EntityWithPath> entities) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull EntityResult renameEntity(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @NonNull PolarisEntity renamedEntity) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull DropEntityResult dropEntityIfExists(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull EntityResult loadEntity(
      @NonNull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      @NonNull PolarisEntityType entityType) {
    // entityCatalogId and entityType are accepted for interface parity but not used to filter:
    // the new model's ENTITY identity key is (realm, id) alone (see RecordRef's own javadoc), and
    // the shipped DurablePrimitives#lookupEntity already treats both as optimization hints rather
    // than a required filter ("The type code parameter is redundant..."). Both old impls are
    // identical here, so there is nothing else to port.
    Optional<PolarisBaseEntity> found =
        entityStore().get(entityIdentity(entityId), PolarisBaseEntity.class);
    return found
        .<EntityResult>map(EntityResult::new)
        .orElseGet(() -> new EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null));
  }

  @Override
  public @NonNull ChangeTrackingResult loadEntitiesChangeTracking(
      @NonNull PolarisCallContext callCtx, @NonNull List<PolarisEntityId> entityIds) {
    List<RecordRef> refs = entityIds.stream().map(id -> entityIdentity(id.id())).toList();
    List<Optional<RecordVersions>> versions = entityStore().versionsOf(refs);
    List<PolarisChangeTrackingVersions> result = new ArrayList<>(versions.size());
    for (Optional<RecordVersions> v : versions) {
      result.add(
          v.map(
                  rv ->
                      new PolarisChangeTrackingVersions(
                          (int) rv.recordVersion(), (int) rv.grantRecordsVersion()))
              .orElse(null));
    }
    return new ChangeTrackingResult(result);
  }

  @Override
  public @NonNull ResolvedEntityResult loadResolvedEntityById(
      @NonNull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull ResolvedEntitiesResult loadResolvedEntities(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityType entityType,
      @NonNull List<PolarisEntityId> entityIds) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  // ------------------------------------------------------- DurableManager (ticket 92 surfaces)

  @Override
  public @NonNull BaseResult purge(@NonNull PolarisCallContext callCtx) {
    throw new UnsupportedOperationException("ticket 92: purge");
  }

  @Override
  public @NonNull EntitiesResult loadTasks(
      @NonNull PolarisCallContext callCtx, String executorId, PageToken pageToken) {
    throw new UnsupportedOperationException("ticket 92: task leasing (loadTasks)");
  }

  @Override
  public @NonNull ResolvedEntityResult loadResolvedEntityByName(
      @NonNull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @NonNull PolarisEntityType entityType,
      @NonNull String entityName) {
    throw new UnsupportedOperationException(
        "ticket 92: entity cache / resolver refresh (loadResolvedEntityByName)");
  }

  @Override
  public @NonNull ResolvedEntityResult refreshResolvedEntity(
      @NonNull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      @NonNull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId) {
    throw new UnsupportedOperationException(
        "ticket 92: entity cache / resolver refresh (refreshResolvedEntity)");
  }

  // ---------------------------------------------------------- GrantManager (ticket 91)

  /**
   * Grant-record identity ref: {@code (securable-catalog, securable, grantee-catalog, grantee,
   * privilege)} — every field is part of the key, so identity and uniqueness are the same tuple
   * (verified against both shipped stores' bindings, {@code TreeMapDurableRecordStore} and {@code
   * JdbcDurableRecordStore}).
   */
  private static RecordRef grantIdentity(@NonNull PolarisGrantRecord g) {
    return RecordRef.byIdentity(
        PolarisRecordKinds.GRANT_RECORD,
        List.of(
            g.getSecurableCatalogId(),
            g.getSecurableId(),
            g.getGranteeCatalogId(),
            g.getGranteeId(),
            g.getPrivilegeCode()));
  }

  private DurableRecordStore grantStore() {
    return storeForKind.apply(PolarisRecordKinds.GRANT_RECORD);
  }

  /**
   * Loads an entity by identity, throwing uncaught rather than returning a status when it is absent
   * — ported from both old impls' {@code getDiagnostics().checkNotNull(...)} on a
   * concurrently-deleted grantee/securable inside {@code persistNewGrantRecord}/{@code
   * revokeGrantRecord}. Naming both old behaviours rather than silently matching one: {@code
   * AtomicOperationMetaStoreManager} never returns {@code ENTITY_CANNOT_BE_RESOLVED} for grant
   * operations and relies on exactly this uncaught throw; {@code TransactionalMetaStoreManagerImpl}
   * additionally re-resolves through the package-private {@code PolarisEntityResolver} first and
   * CAN return {@code ENTITY_CANNOT_BE_RESOLVED} for the grant/revoke entry points themselves. We
   * match Atomic, the same parity choice {@link #catalogIdOf} documents for entity operations; the
   * fixture does not discriminate between the two.
   */
  private PolarisBaseEntity mustLoadEntity(@NonNull PolarisEntityCore entity, String signature) {
    PolarisBaseEntity loaded =
        entityStore().get(entityIdentity(entity.getId()), PolarisBaseEntity.class).orElse(null);
    diagnostics.checkNotNull(loaded, signature, "entity={}", entity);
    return loaded;
  }

  /**
   * The UPDATE mutation that bumps one entity's {@code grantRecordsVersion}, gated by both halves
   * of the two-column CAS the relational store's {@code entity_version}/{@code
   * grant_records_version} comparison performs: {@code entityVersion} is asserted unchanged, never
   * bumped here — only {@code grantRecordsVersion} moves, matching both old impls' {@code
   * entity.withGrantRecordsVersion(entity.getGrantRecordsVersion() + 1)}.
   */
  private Mutation bumpGrantRecordsVersion(@NonNull PolarisBaseEntity entity) {
    RecordRef ref = entityIdentity(entity.getId());
    return Mutation.of(
        PolarisRecordKinds.ENTITY,
        Mutation.Op.UPDATE,
        ref,
        entity.withGrantRecordsVersion(entity.getGrantRecordsVersion() + 1),
        List.of(
            Precondition.versionEquals(
                ref, Precondition.VersionAttribute.RECORD_VERSION, entity.getEntityVersion()),
            Precondition.versionEquals(
                ref,
                Precondition.VersionAttribute.GRANT_RECORDS_VERSION,
                entity.getGrantRecordsVersion())));
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
    RecordRef ref = grantIdentity(grantRecord);

    Optional<PolarisGrantRecord> existing = grantStore().get(ref, PolarisGrantRecord.class);
    if (existing.isPresent()) {
      return new PrivilegeResult(existing.get());
    }

    PolarisBaseEntity granteeEntity = mustLoadEntity(grantee, "grantee_not_found");
    PolarisBaseEntity securableEntity = mustLoadEntity(securable, "securable_not_found");

    List<Mutation> mutations =
        List.of(
            Mutation.of(
                PolarisRecordKinds.GRANT_RECORD,
                Mutation.Op.CREATE,
                ref,
                grantRecord,
                List.of(Precondition.none())),
            bumpGrantRecordsVersion(granteeEntity),
            bumpGrantRecordsVersion(securableEntity));

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

    PolarisBaseEntity granteeEntity = mustLoadEntity(grantee, "missing_grantee");
    PolarisBaseEntity securableEntity = mustLoadEntity(securable, "missing_securable");

    List<Mutation> mutations =
        List.of(
            Mutation.of(
                PolarisRecordKinds.GRANT_RECORD,
                Mutation.Op.DELETE,
                grantIdentity(grantRecord),
                null),
            bumpGrantRecordsVersion(granteeEntity),
            bumpGrantRecordsVersion(securableEntity));

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
    return failure == CommitResult.Failure.PRECONDITION_FAILED
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
        entityStore().versionsOf(List.of(entityIdentity(anchorId))).get(0);
    if (anchorVersions.isEmpty()) {
      return new LoadGrantsResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }
    int grantsVersion = (int) anchorVersions.get().grantRecordsVersion();

    List<PolarisGrantRecord> grantRecords =
        grantStore()
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
            .mapToObj(DefaultDurableManager::entityIdentity)
            .toList();
    List<PolarisBaseEntity> entities =
        entityStore().getMany(counterpartRefs, PolarisBaseEntity.class).stream()
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
        grantStore()
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
        grantStore()
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

  // ---------------------------------------------------------- SecretsManager (ticket 91)

  @Override
  public @NonNull PrincipalSecretsResult loadPrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String clientId) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull PrincipalSecretsResult rotatePrincipalSecrets(
      @NonNull PolarisCallContext callCtx,
      @NonNull String clientId,
      long principalId,
      boolean reset,
      @NonNull String oldSecretHash) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull PrincipalSecretsResult resetPrincipalSecrets(
      @NonNull PolarisCallContext callCtx,
      long principalId,
      @NonNull String resolvedClientId,
      String customClientSecret) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public void deletePrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String clientId, long principalId) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  // ------------------------------------------------- PolarisPolicyMappingManager (ticket 92)

  @Override
  public @NonNull PolicyAttachmentResult attachPolicyToEntity(
      @NonNull PolarisCallContext callCtx,
      @NonNull List<PolarisEntityCore> targetCatalogPath,
      @NonNull PolarisEntityCore target,
      @NonNull List<PolarisEntityCore> policyCatalogPath,
      @NonNull PolicyEntity policy,
      Map<String, String> parameters) {
    throw new UnsupportedOperationException("ticket 92: policy mapping (attachPolicyToEntity)");
  }

  @Override
  public @NonNull PolicyAttachmentResult detachPolicyFromEntity(
      @NonNull PolarisCallContext callCtx,
      @NonNull List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityCore target,
      @NonNull List<PolarisEntityCore> policyCatalogPath,
      @NonNull PolicyEntity policy) {
    throw new UnsupportedOperationException("ticket 92: policy mapping (detachPolicyFromEntity)");
  }

  @Override
  public @NonNull LoadPolicyMappingsResult loadPoliciesOnEntity(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisEntityCore target) {
    throw new UnsupportedOperationException("ticket 92: policy mapping (loadPoliciesOnEntity)");
  }

  @Override
  public @NonNull LoadPolicyMappingsResult loadPoliciesOnEntityByType(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore target,
      @NonNull PolicyType policyType) {
    throw new UnsupportedOperationException(
        "ticket 92: policy mapping (loadPoliciesOnEntityByType)");
  }

  // ------------------------------------------------------- PolarisEventManager (ticket 92)

  /**
   * Overridden rather than left as the interface default: the default reaches {@link
   * PolarisCallContext#getMetaStore()}, the old primitives handle this class must never touch.
   */
  @Override
  public void writeEvents(
      @NonNull PolarisCallContext callCtx, @NonNull List<EventEntity> polarisEvents) {
    throw new UnsupportedOperationException("ticket 92: events (writeEvents)");
  }
}
