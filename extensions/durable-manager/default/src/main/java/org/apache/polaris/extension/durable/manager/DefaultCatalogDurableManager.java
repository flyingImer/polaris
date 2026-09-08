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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.PolarisObjectMapperUtil;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.CreateCatalogResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.persistence.dao.entity.GenerateEntityIdResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.policy.PolicyMappingUtil;
import org.apache.polaris.spi.durable.CatalogDurableManager;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableOrchestrator;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.OrchestrationResult;
import org.apache.polaris.spi.durable.Precondition;
import org.apache.polaris.spi.durable.RecordRef;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Default catalog durable manager: the entity tree. Catalogs, namespaces, tables, views, roles,
 * policies, tasks and every other entity kind share one record kind and are created, read, renamed
 * and dropped here.
 *
 * <p>This manager owns every business rule of those operations and knows no storage topology:
 * existence checks, name-collision semantics, the id-before-write discipline, and the cleanup a
 * drop composes over the dropped entity's grant records, policy mappings and principal secrets.
 * Authorization and request validation live above this layer.
 *
 * <p>Two doors, strictly divided: every write goes through the orchestrator's commit as a mutation
 * list; every read and every id generation goes to the primitives handle directly. This class never
 * reads the previous persistence handle carried on the call context; reaching for it would silently
 * reintroduce the coupling this class exists to remove.
 *
 * <p>Where a method's behaviour deliberately follows one of the previous managers rather than the
 * other, the method's own javadoc says so.
 */
public class DefaultCatalogDurableManager implements CatalogDurableManager {

  private final Clock clock;

  private final PolarisDiagnostics diagnostics;

  private final DurableOrchestrator orchestrator;

  private final DurableRecordStore primitives;

  public DefaultCatalogDurableManager(
      @NonNull Clock clock,
      @NonNull PolarisDiagnostics diagnostics,
      @NonNull DurableOrchestrator orchestrator,
      @NonNull DurableRecordStore primitives) {
    this.clock = clock;
    this.diagnostics = diagnostics;
    this.orchestrator = orchestrator;
    this.primitives = primitives;
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
   * The id-equality rule every create path's collision point must apply — factored into one helper
   * because it used to be applied at only ONE of a create path's TWO collision points, and having
   * two independent copies of "the rule" is exactly what let them drift (Finding 1, independent
   * review, 2026-08-18). {@code AtomicOperationMetaStoreManager#persistNewEntity} has exactly ONE
   * collision point — a caught {@code EntityAlreadyExistsException} — and applies this comparison
   * there unconditionally, covering both what this class had split into a pre-check branch (which
   * implemented it) and a lost-race branch discovered by a failed commit precondition (which did
   * not, and returned {@code ENTITY_ALREADY_EXISTS} regardless of id). Every create path below now
   * routes both branches through this one method.
   *
   * @param existing whatever is currently stored under the uniqueness key (or identity, for {@link
   *     #createCatalog}/{@code DefaultPrincipalDurableManager#createPrincipal}) that collided
   * @param creatingId the id of the entity THIS call attempted to create — for a batch, the id of
   *     the specific entity whose OWN uniqueness key collided, never an arbitrary member of the
   *     batch (see {@link #createEntitiesIfNotExist}, which tracks this per mutation for exactly
   *     that reason)
   * @return true means an idempotent retry: the caller returns bare success with the entity IT was
   *     attempting to create, no subtype — matching {@code persistNewEntity}'s {@code new
   *     EntityResult(entity)} verbatim. This is NOT the {@code ENTITY_ALREADY_EXISTS} shape a stale
   *     comment on {@link #mapFailedCreate} once claimed the lost-race branch "mirrors"; the
   *     pre-check branch it was supposedly mirroring returns bare success with no subtype in
   *     exactly this case. False means a genuine conflict: the caller returns {@code
   *     ENTITY_ALREADY_EXISTS} carrying {@code existing}'s subtype code.
   */
  // Package-private rather than private: DefaultCatalogDurableManagerTest asserts this rule
  // directly (see its javadoc for why — the branch it gates cannot be driven deterministically
  // through the public API without a test-only hook this class does not have).
  static boolean isIdempotentRetry(@NonNull PolarisBaseEntity existing, long creatingId) {
    return existing.getId() == creatingId;
  }

  /**
   * Maps an {@link OrchestrationResult} that did not apply to the caller-facing result for a single
   * create. There is no old-model precedent for this mapping — the old primitives interface has no
   * multi-outcome commit result to map from, it either succeeds, throws, or (for a batch) partially
   * applies inside one DB transaction — so this is a new decision, not a ported one. {@code
   * creating} is the entity this call attempted to create, needed to apply {@link
   * #isIdempotentRetry} on a lost race (Finding 1).
   */
  private EntityResult mapFailedCreate(
      @NonNull DurableRecordStore store,
      @NonNull RecordRef uniqueness,
      @NonNull PolarisBaseEntity creating,
      @NonNull Set<RecordRef> pathRefs,
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
    if (RecordMutations.failedOnPath(result, pathRefs)) {
      // The retrofit (see catalogIdOf's javadoc): a path entity was gone by commit time. Matches
      // TransactionalMetaStoreManagerImpl's status for exactly this situation.
      return new EntityResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
    }
    // Lost the race between our pre-check read and the commit: something else created a row at
    // this uniqueness key in between. Apply the SAME id-equality rule the pre-check branch
    // applies (see isIdempotentRetry) rather than assuming it is always a conflict.
    Optional<PolarisBaseEntity> winner = store.get(uniqueness, PolarisBaseEntity.class);
    if (winner.isPresent() && isIdempotentRetry(winner.get(), creating.getId())) {
      return new EntityResult(creating);
    }
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

  @Override
  public @NonNull ListEntitiesResult listEntities(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull PageToken pageToken) {
    // Mutable, not Stream#toList()'s unmodifiable result: found by testDropEntities (increment 5),
    // whose dropEntity helper calls children.clear() on this exact return value when exactly one
    // catalog role is left. A real increment-2 bug, only reachable once drop existed to walk that
    // far — fixed here since it blocks this increment's own target, not the type_code anchor gap
    // the tripwire on this method is actually about.
    List<EntityNameLookupRecord> records =
        new ArrayList<>(
            RecordRefs.listChildEntities(
                    primitives, catalogPath, entityType, entitySubType, pageToken)
                .stream()
                .map(EntityNameLookupRecord::new)
                .toList());
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
        pageToken,
        RecordRefs.listChildEntities(primitives, catalogPath, entityType, entitySubType, pageToken),
        null);
  }

  @Override
  public @NonNull GenerateEntityIdResult generateNewEntityId(@NonNull PolarisCallContext callCtx) {
    return new GenerateEntityIdResult(primitives.generateNewId());
  }

  /**
   * ADR-0002's other hard invariant (2026-08-07, grants stay OUTSIDE it): {@code createCatalog}
   * yields BOTH the catalog and its admin {@link
   * org.apache.polaris.core.entity.PolarisEntityType#CATALOG_ROLE}, both durably persisted, or
   * neither. This class does not itself implement that guarantee — there is no special-case
   * atomicity code for "these two entities" anywhere below. It holds because of composition (Issue
   * 47's framing): both are {@code ENTITY} records, {@code ENTITY} resolves to exactly one store
   * under the kind-to-store mapping, and {@link DurableOrchestrator#commit} merges adjacent
   * mutations sharing one atomicity domain into a single commit. Keeping the catalog and admin-role
   * {@code CREATE} mutations ADJACENT in the list below is what lets that composition apply; the
   * conformance suite is what verifies the composition holds, not this method.
   *
   * <p>Grants (both {@code CATALOG_MANAGE_ACCESS}/{@code CATALOG_MANAGE_METADATA} on the catalog to
   * the admin role, and {@code CATALOG_ROLE_USAGE} on the admin role to each assignee) ride in the
   * SAME commit as the catalog/admin-role pair for atomicity's sake where the one-commit shape
   * gives it for free, but are not themselves part of the hard guarantee: a grant failing does not
   * get a weaker catalog/admin-role pair, it fails the whole {@code createCatalog} call, same as
   * any other precondition failure below.
   *
   * <p>Storage integration ({@code IntegrationPersistence#createStorageIntegration}/{@code
   * persistStorageIntegrationIfNeeded}, both old impls' branch for a catalog carrying an inline
   * storage config) is intentionally NOT ported: no fixture catalog carries one (checked — {@code
   * PolarisTestMetaStoreManager} never sets a storage-config internal property on any catalog it
   * builds), and {@code IntegrationPersistence}'s own class note records that every OSS
   * implementation of the hook already returns null / does nothing. Left to the migration tickets
   * rather than writing code no test exercises.
   */
  @Override
  public @NonNull CreateCatalogResult createCatalog(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity catalog,
      @NonNull List<PolarisEntityCore> principalRoles) {
    diagnostics.checkNotNull(catalog, "unexpected_null_catalog");

    Optional<PolarisBaseEntity> existingCatalog =
        primitives.get(RecordRefs.entityIdentity(catalog.getId()), PolarisBaseEntity.class);
    if (existingCatalog.isPresent()) {
      diagnostics.check(
          existingCatalog.get().getTypeCode() == PolarisEntityType.CATALOG.getCode(),
          "not_a_catalog",
          "catalog={}",
          catalog);
      return loadExistingCatalog(existingCatalog.get());
    }

    PolarisBaseEntity preparedCatalog = prepareNewEntity(catalog);
    long adminRoleId = primitives.generateNewId();
    PolarisBaseEntity adminRole =
        prepareNewEntity(
            new PolarisBaseEntity(
                preparedCatalog.getId(),
                adminRoleId,
                PolarisEntityType.CATALOG_ROLE,
                PolarisEntitySubType.NULL_SUBTYPE,
                preparedCatalog.getId(),
                PolarisEntityConstants.getNameOfCatalogAdminRole()));

    List<Mutation> mutations = new ArrayList<>();
    mutations.add(
        Mutation.of(
            PolarisRecordKinds.ENTITY,
            Mutation.Op.CREATE,
            RecordRefs.entityIdentity(preparedCatalog.getId()),
            preparedCatalog,
            List.of(
                Precondition.notExists(
                    RecordRefs.entityUniqueness(
                        preparedCatalog.getParentId(),
                        preparedCatalog.getTypeCode(),
                        preparedCatalog.getName())))));
    mutations.add(
        Mutation.of(
            PolarisRecordKinds.ENTITY,
            Mutation.Op.CREATE,
            RecordRefs.entityIdentity(adminRole.getId()),
            adminRole,
            List.of(
                Precondition.notExists(
                    RecordRefs.entityUniqueness(
                        adminRole.getParentId(), adminRole.getTypeCode(), adminRole.getName())))));

    PolarisBaseEntity catalogState = preparedCatalog;
    PolarisBaseEntity adminRoleState = adminRole;
    for (PolarisPrivilege priv :
        List.of(PolarisPrivilege.CATALOG_MANAGE_ACCESS, PolarisPrivilege.CATALOG_MANAGE_METADATA)) {
      PolarisGrantRecord grantRecord =
          new PolarisGrantRecord(
              catalogState.getCatalogId(),
              catalogState.getId(),
              adminRoleState.getCatalogId(),
              adminRoleState.getId(),
              priv.getCode());
      mutations.add(RecordMutations.createGrantMutation(grantRecord));
      RecordMutations.VersionBump granteeBump =
          RecordMutations.bumpGrantRecordsVersion(adminRoleState);
      mutations.add(granteeBump.mutation());
      adminRoleState = granteeBump.updated();
      RecordMutations.VersionBump securableBump =
          RecordMutations.bumpGrantRecordsVersion(catalogState);
      mutations.add(securableBump.mutation());
      catalogState = securableBump.updated();
    }

    List<PolarisBaseEntity> assignees;
    if (principalRoles.isEmpty()) {
      PrincipalRoleEntity serviceAdminRole =
          findPrincipalRoleByName(
                  callCtx, PolarisEntityConstants.getNameOfPrincipalServiceAdminRole())
              .orElse(null);
      diagnostics.checkNotNull(serviceAdminRole, "missing_service_admin_role");
      assignees = List.of(serviceAdminRole);
    } else {
      assignees = new ArrayList<>(principalRoles.size());
      for (PolarisEntityCore principalRole : principalRoles) {
        diagnostics.checkNotNull(principalRole, "null principal role");
        diagnostics.check(
            principalRole.getTypeCode() == PolarisEntityType.PRINCIPAL_ROLE.getCode(),
            "not_principal_role",
            "type={}",
            principalRole.getType());
        assignees.add(
            RecordRefs.mustLoadEntity(primitives, diagnostics, principalRole, "grantee_not_found"));
      }
    }
    for (PolarisBaseEntity principalRole : assignees) {
      PolarisGrantRecord grantRecord =
          new PolarisGrantRecord(
              adminRoleState.getCatalogId(),
              adminRoleState.getId(),
              principalRole.getCatalogId(),
              principalRole.getId(),
              PolarisPrivilege.CATALOG_ROLE_USAGE.getCode());
      mutations.add(RecordMutations.createGrantMutation(grantRecord));
      mutations.add(RecordMutations.bumpGrantRecordsVersion(principalRole).mutation());
      RecordMutations.VersionBump securableBump =
          RecordMutations.bumpGrantRecordsVersion(adminRoleState);
      mutations.add(securableBump.mutation());
      adminRoleState = securableBump.updated();
    }

    OrchestrationResult result = orchestrator.commit(mutations);
    if (result.isApplied()) {
      // Matches both old impls: returns the pre-grant state of the catalog and admin role, not
      // the state after the grant-driven grantRecordsVersion bumps.
      return new CreateCatalogResult(preparedCatalog, adminRole);
    }
    BaseResult.ReturnStatus failureStatus = RecordMutations.classifyFailedCreate(result);
    if (failureStatus == BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS) {
      // Finding 1 (independent review, 2026-08-18): a lost race can mean someone else already
      // committed THIS exact catalog id (the caller's own reservation, not one this method
      // generates) rather than a genuine name conflict. Reading by identity rather than
      // catalogUniqueness is deliberate: the admin role's own id is generated fresh inside THIS
      // call on every invocation (generateNewId() below, never caller-supplied), so it carries no
      // cross-call identity to compare — only the catalog's caller-reserved id does, and finding
      // a row there already proves the id matches (no separate id-equality comparison needed the
      // way createEntityIfNotExists's uniqueness-keyed read requires one).
      Optional<PolarisBaseEntity> winner =
          primitives.get(
              RecordRefs.entityIdentity(preparedCatalog.getId()), PolarisBaseEntity.class);
      if (winner.isPresent()) {
        return loadExistingCatalog(winner.get());
      }
    }
    return new CreateCatalogResult(failureStatus, RecordMutations.failureDetail(result));
  }

  /**
   * Loads the existing (catalog, admin role) pair by the catalog's id — shared by {@link
   * #createCatalog}'s identity pre-check (this id already exists) and its lost-race branch (Finding
   * 1: the SAME rule now applies at both of a create path's collision points).
   */
  private CreateCatalogResult loadExistingCatalog(@NonNull PolarisBaseEntity existingCatalog) {
    PolarisBaseEntity adminRole =
        primitives
            .get(
                RecordRefs.entityUniqueness(
                    existingCatalog.getId(),
                    PolarisEntityType.CATALOG_ROLE.getCode(),
                    PolarisEntityConstants.getNameOfCatalogAdminRole()),
                PolarisBaseEntity.class)
            .orElse(null);
    diagnostics.checkNotNull(
        adminRole, "catalog_admin_role_not_found", "catalog={}", existingCatalog);
    return new CreateCatalogResult(existingCatalog, adminRole);
  }

  @Override
  public @NonNull EntityResult createEntityIfNotExists(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entity) {
    diagnostics.checkNotNull(entity, "unexpected_null_entity");
    diagnostics.checkNotNull(entity.getName(), "unexpected_null_entity_name");

    // catalogId/parentId are taken from the entity object, never re-derived from catalogPath:
    // AtomicOperationMetaStoreManager's own create path does the same. catalogPath is still
    // consulted below, for the retrofit's EXISTS preconditions — see catalogIdOf's javadoc.
    PolarisBaseEntity prepared = prepareNewEntity(entity);
    DurableRecordStore store = primitives;
    RecordRef uniqueness =
        RecordRefs.entityUniqueness(
            prepared.getParentId(), prepared.getTypeCode(), prepared.getName());

    // EXPLICIT, PROVISIONAL ASSUMPTION (EJ, 2026-08-17: not certain this is purely a business
    // rule, revisit if it causes trouble): "same id means idempotent create-retry; a different
    // id holding the name is a real conflict." In the old model this lives in the primitives
    // layer, not the manager: AbstractTransactionalPersistence#
    // checkConditionsForWriteEntityInCurrentTxn throws EntityAlreadyExistsException on any name
    // collision, and AtomicOperationMetaStoreManager#persistNewEntity catches it and applies
    // exactly this id-equality test — see isIdempotentRetry, which factors it out so this branch
    // and mapFailedCreate's lost-race branch below cannot apply it differently (Finding 1).
    Optional<PolarisBaseEntity> existing = store.get(uniqueness, PolarisBaseEntity.class);
    if (existing.isPresent()) {
      // Return the entity we were trying to create, not the stored one — matching
      // persistNewEntity's own comment: the caller should see what it asked to create, even
      // if a concurrent update landed on the stored row first.
      return isIdempotentRetry(existing.get(), prepared.getId())
          ? new EntityResult(prepared)
          : new EntityResult(
              BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, existing.get().getSubTypeCode());
    }

    List<Precondition> preconditions =
        new ArrayList<>(RecordMutations.pathExistsPreconditions(catalogPath, null));
    preconditions.add(Precondition.notExists(uniqueness));
    OrchestrationResult result =
        orchestrator.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.ENTITY,
                    Mutation.Op.CREATE,
                    RecordRefs.entityIdentity(prepared.getId()),
                    prepared,
                    preconditions)));
    return result.isApplied()
        ? new EntityResult(prepared)
        : mapFailedCreate(
            store, uniqueness, prepared, RecordMutations.pathRefs(catalogPath, null), result);
  }

  /**
   * Batch form of {@link #createEntityIfNotExists}'s collision rule, applied per entity. In the old
   * model the batch form of this rule lives one layer further down than the single-entity form:
   * {@code AbstractTransactionalPersistence#writeEntities} (around lines 254-265) catches {@code
   * EntityAlreadyExistsException} per entity inside its own transaction loop and swallows it when
   * the existing entity's id matches, rethrowing (aborting the whole batch) otherwise. {@code
   * TreeMapDurablePrimitivesImpl} extends {@code AbstractTransactionalPersistence} without
   * overriding {@code writeEntities}, so both old managers get the rule "for free" from the
   * primitives layer for the batch case. The new store's commit has no such per-mutation swallow —
   * a failed precondition fails the WHOLE commit — so S3 puts the rule here, in the manager,
   * explicitly and by hand.
   *
   * <p><b>Finding 1's batch shape (independent review, 2026-08-18):</b> a lost race discovered at
   * commit time used to be treated as a genuine conflict unconditionally, the same defect {@link
   * #mapFailedCreate} had. The fix here is a small retry loop rather than a single re-check,
   * because the batch's commit is genuinely all-or-nothing: if entity B's uniqueness precondition
   * fails because entity B was itself an idempotent retry (id matches what is already stored), the
   * commit still rolled back EVERY OTHER mutation in the list, including entities that had no
   * problem at all. Dropping B's now-redundant mutation and retrying the remaining list is how this
   * reaches the same end state {@code writeEntities}' per-entity swallow-and-continue reaches in
   * one DB transaction — one dropped mutation at a time, since this commit cannot swallow a single
   * row's failure the way a loop over individual writes can. The winner MUST be compared against
   * the specific entity whose uniqueness key collided, tracked in {@code byUniqueness} — never
   * against the first entity in the batch, an easy mistake once several entities are in flight at
   * once.
   */
  @Override
  public @NonNull EntitiesResult createEntitiesIfNotExist(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull List<? extends PolarisBaseEntity> entities) {
    DurableRecordStore store = primitives;
    List<PolarisBaseEntity> resolved = new ArrayList<>(entities.size());
    List<Mutation> mutations = new ArrayList<>();
    Map<RecordRef, PolarisBaseEntity> byUniqueness = new HashMap<>();

    for (PolarisBaseEntity entity : entities) {
      PolarisBaseEntity prepared = prepareNewEntity(entity);
      RecordRef uniqueness =
          RecordRefs.entityUniqueness(
              prepared.getParentId(), prepared.getTypeCode(), prepared.getName());
      Optional<PolarisBaseEntity> existing = store.get(uniqueness, PolarisBaseEntity.class);
      if (existing.isPresent() && !isIdempotentRetry(existing.get(), prepared.getId())) {
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
        List<Precondition> preconditions =
            new ArrayList<>(RecordMutations.pathExistsPreconditions(catalogPath, null));
        preconditions.add(Precondition.notExists(uniqueness));
        mutations.add(
            Mutation.of(
                PolarisRecordKinds.ENTITY,
                Mutation.Op.CREATE,
                RecordRefs.entityIdentity(prepared.getId()),
                prepared,
                preconditions));
        byUniqueness.put(uniqueness, prepared);
      }
      // else: idempotent retry, same id — no mutation needed, `resolved` already carries the
      // entity we were trying to create.
    }

    Set<RecordRef> pathRefs = RecordMutations.pathRefs(catalogPath, null);
    while (!mutations.isEmpty()) {
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
      CommitResult groupFailure = result.groupFailure().orElseThrow();
      CommitResult.Failure failure = groupFailure.failure().orElseThrow();
      if (failure != CommitResult.Failure.PRECONDITION_FAILED) {
        return new EntitiesResult(
            BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED, failure.toString());
      }
      if (RecordMutations.failedOnPath(result, pathRefs)) {
        return new EntitiesResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
      }
      // Which entity's uniqueness key collided — never assumed to be the first of the batch.
      Optional<RecordRef> failedUniqueness =
          groupFailure.failedPreconditions().stream()
              .map(Precondition::ref)
              .flatMap(Optional::stream)
              .filter(byUniqueness::containsKey)
              .findFirst();
      if (failedUniqueness.isEmpty()) {
        // A PRECONDITION_FAILED that is neither the path retrofit nor one of this batch's own
        // uniqueness checks — should not happen given every precondition on these mutations is
        // one of the two, but reported rather than guessed at if it does.
        return new EntitiesResult(
            BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED,
            "precondition failed on an unrecognized reference");
      }
      RecordRef ref = failedUniqueness.get();
      PolarisBaseEntity creating = byUniqueness.remove(ref);
      Optional<PolarisBaseEntity> winner = store.get(ref, PolarisBaseEntity.class);
      if (winner.isEmpty() || !isIdempotentRetry(winner.get(), creating.getId())) {
        return new EntitiesResult(
            BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS,
            winner
                .map(
                    w ->
                        String.format(
                            "Existing entity id: '%s', type %s subtype %s",
                            w.getId(), w.getTypeCode(), w.getSubTypeCode()))
                .orElse("entity vanished between the failed commit and the re-read"));
      }
      // Idempotent retry: someone else already committed this exact entity (matching id) between
      // our pre-check and this commit. Its mutation is now redundant — drop it and retry the
      // remaining list.
      RecordRef resolvedTarget = RecordRefs.entityIdentity(creating.getId());
      List<Mutation> remaining = new ArrayList<>(mutations.size() - 1);
      for (Mutation m : mutations) {
        if (!m.target().equals(resolvedTarget)) {
          remaining.add(m);
        }
      }
      mutations = remaining;
    }
    // Every remaining mutation resolved as an idempotent retry.
    return new EntitiesResult(Page.fromItems(resolved));
  }

  /**
   * Rebuilds from a fresh read, overlaying ONLY {@code properties}/{@code internalProperties} —
   * {@code TransactionalMetaStoreManagerImpl}'s shape (its {@code
   * updateEntityPropertiesIfNotChanged} re-reads and copies across just those two fields), not
   * {@code AtomicOperationMetaStoreManager}'s, which persists the caller's {@code entity} argument
   * verbatim and so silently writes back whatever stale {@code parentId}/{@code name}/timestamps
   * the caller's copy happened to carry. The brief calls for Transactional's shape here; this
   * follows it.
   *
   * <p>{@code catalogPath} is accepted but not consulted, same parity choice {@code
   * RecordRefs#catalogIdOf} documents elsewhere — but for a different reason than usual: it is not
   * merely unconsulted by the old impl this follows for parity, it is genuinely irrelevant to the
   * write. Neither old implementation resolves the entity being updated THROUGH its path; both go
   * straight to it by {@code catalogId}+{@code id}. {@code TransactionalMetaStoreManagerImpl} DOES
   * additionally re-resolve {@code catalogPath} via the package-private {@code
   * PolarisEntityResolver} and can return {@code CATALOG_PATH_CANNOT_BE_RESOLVED} for a stale one —
   * found while reading it for this increment, and NOT reproduced here: the retrofit's target
   * failure mode is a write that SUCCEEDS underneath a deleted path (see {@code
   * RecordRefs#catalogIdOf}'s javadoc), and an update's own version precondition below already
   * fails a concurrently-changed entity regardless of what happened to its ancestors, so there is
   * no equivalent hole for the retrofit to close. Flagging this rather than silently applying the
   * retrofit here anyway, since the brief's own retrofit list names only the create paths plus this
   * increment's rename/drop.
   *
   * <p>Not-found and stale-version COLLAPSE into the same {@code
   * TARGET_ENTITY_CONCURRENTLY_MODIFIED} signal, matching {@code AtomicOperationMetaStoreManager}'s
   * actually observed behavior rather than the weaker two-branch reading its own javadoc comment
   * suggests: its write goes through {@code
   * AbstractTransactionalPersistence#checkConditionsForWriteEntityInCurrentTxn}, whose update-path
   * check is {@code if (refreshedEntity == null || refreshedEntity.getEntityVersion() !=
   * originalEntity.getEntityVersion() || refreshedEntity.getGrantRecordsVersion() !=
   * originalEntity.getGrantRecordsVersion()) throw RetryOnConcurrencyException} — absence and a
   * stale version throw the identical exception, caught by {@code
   * AtomicOperationMetaStoreManager#updateEntityPropertiesIfNotChanged} into one status. {@code
   * TransactionalMetaStoreManagerImpl} diverges here too (its own not-found path is an uncaught
   * {@code checkNotNull}, a crash rather than a status) but is not one of the fixture's five tested
   * bindings, and the fixture's own {@code testUpdateEntities} — "update an entity which does not
   * exist" — exercises exactly this and expects a graceful null, which only Atomic's shape
   * delivers. Realized here as two {@code VERSION_EQUALS} preconditions mirroring {@code
   * RecordMutations#bumpGrantRecordsVersion}'s own two-column CAS, since a {@code VERSION_EQUALS}
   * precondition against an absent record already evaluates false (see {@code Precondition}'s
   * {@code holds()}), so absence and staleness fail the same way without a separate branch.
   */
  @Override
  public @NonNull EntityResult updateEntityPropertiesIfNotChanged(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entity) {
    diagnostics.checkNotNull(entity, "unexpected_null_entity");

    RecordRef ref = RecordRefs.entityIdentity(entity.getId());
    Optional<PolarisBaseEntity> current = primitives.get(ref, PolarisBaseEntity.class);
    if (current.isEmpty()
        || current.get().getEntityVersion() != entity.getEntityVersion()
        || current.get().getGrantRecordsVersion() != entity.getGrantRecordsVersion()) {
      return new EntityResult(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, null);
    }
    PolarisBaseEntity currentEntity = current.get();

    PolarisBaseEntity updated =
        new PolarisBaseEntity.Builder(currentEntity)
            .properties(entity.getProperties())
            .internalProperties(entity.getInternalProperties())
            .entityVersion(currentEntity.getEntityVersion() + 1)
            // System.currentTimeMillis(), not clock.millis(): PolarisBaseEntity.Builder#build()'s
            // own createTimestamp default is real wall-clock time, decoupled from any injected
            // clock, and the fixture's testStartTime is captured the same way. This class's clock
            // field is real in production; the fixture's own MutableClock is fixed at construction
            // and only advances via explicit clock.add(...) (for the task-leasing tests) —
            // using it here made every update's timestamp read as BEFORE the entity's own
            // real-time createTimestamp. Found by testUpdateEntities/testRename failing on exactly
            // that ordering.
            .lastUpdateTimestamp(System.currentTimeMillis())
            .build();

    OrchestrationResult result =
        orchestrator.commit(
            List.of(RecordMutations.entityPropertiesUpdateMutation(ref, currentEntity, updated)));
    return result.isApplied()
        ? new EntityResult(updated)
        : new EntityResult(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, null);
  }

  /**
   * Same per-entity rule as the single form, applied across one atomic mutation list so the batch
   * stays atomic — matching both old impls' batch shape (Atomic's single {@code ms.writeEntities}
   * call, Transactional's one wrapping DB transaction): one entity failing its pre-check aborts the
   * WHOLE batch before anything commits, rather than partially applying. {@code
   * EntityWithPath#catalogPath()} is accepted (it rides on the record) but not consulted, for the
   * same reason given in the single form's javadoc.
   */
  @Override
  public @NonNull EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @NonNull PolarisCallContext callCtx, @NonNull List<EntityWithPath> entities) {
    diagnostics.checkNotNull(entities, "unexpected_null_entities");

    List<PolarisBaseEntity> updated = new ArrayList<>(entities.size());
    List<Mutation> mutations = new ArrayList<>(entities.size());

    for (EntityWithPath entityWithPath : entities) {
      PolarisBaseEntity entity = entityWithPath.entity();
      RecordRef ref = RecordRefs.entityIdentity(entity.getId());
      Optional<PolarisBaseEntity> current = primitives.get(ref, PolarisBaseEntity.class);
      if (current.isEmpty()
          || current.get().getEntityVersion() != entity.getEntityVersion()
          || current.get().getGrantRecordsVersion() != entity.getGrantRecordsVersion()) {
        return new EntitiesResult(
            BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, null);
      }
      PolarisBaseEntity currentEntity = current.get();
      PolarisBaseEntity updatedEntity =
          new PolarisBaseEntity.Builder(currentEntity)
              .properties(entity.getProperties())
              .internalProperties(entity.getInternalProperties())
              .entityVersion(currentEntity.getEntityVersion() + 1)
              // See the single form's javadoc: real wall-clock time, not this class's clock field.
              .lastUpdateTimestamp(System.currentTimeMillis())
              .build();
      updated.add(updatedEntity);
      mutations.add(
          RecordMutations.entityPropertiesUpdateMutation(ref, currentEntity, updatedEntity));
    }

    if (mutations.isEmpty()) {
      return new EntitiesResult(Page.fromItems(updated));
    }

    OrchestrationResult result = orchestrator.commit(mutations);
    if (result.isApplied()) {
      return new EntitiesResult(Page.fromItems(updated));
    }
    if (result.outcome() == OrchestrationResult.Outcome.ROLLBACK_INCOMPLETE) {
      return new EntitiesResult(
          BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED,
          "rollback incomplete: "
              + result.uncompensated().size()
              + " mutation(s) require admin reclamation");
    }
    return new EntitiesResult(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, null);
  }

  /**
   * One {@code UPDATE} mutation carrying {@link Mutation}'s own worked rename example verbatim: the
   * source still at the version read, the destination name free, the destination parent still
   * present — plus the retrofit's path-EXISTS hardening over both {@code catalogPath} and {@code
   * newCatalogPath} (their union covers "destination parent still present" as one instance of the
   * broader check, so it is not declared a second time). Ported structurally from both old impls,
   * which are identical here except for {@code TransactionalMetaStoreManagerImpl}'s additional
   * {@code PolarisEntityResolver} path re-check — which the retrofit's {@code EXISTS} preconditions
   * now subsume with a real happens-before guarantee instead of a same-transaction coincidence (see
   * {@code RecordRefs#catalogIdOf}'s javadoc).
   *
   * <p>{@code cannotBeDroppedOrRenamed()} → {@code ENTITY_CANNOT_BE_RENAMED}; a taken destination
   * name → {@code ENTITY_ALREADY_EXISTS} carrying the existing entity's subtype code; a missing
   * source → {@code ENTITY_NOT_FOUND}; a stale source version → {@code
   * TARGET_ENTITY_CONCURRENTLY_MODIFIED} — all four read directly off both old impls' own {@code
   * renameEntity}, which agree on every status here.
   */
  @Override
  public @NonNull EntityResult renameEntity(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @NonNull PolarisEntity renamedEntity) {
    diagnostics.checkNotNull(entityToRename, "unexpected_null_entityToRename");
    diagnostics.checkNotNull(renamedEntity, "unexpected_null_renamedEntity");
    diagnostics.check(
        newCatalogPath == null || catalogPath != null,
        "newCatalogPath_specified_without_catalogPath");

    // null newCatalogPath is shorthand for "the path isn't changing" (both old impls' own comment).
    List<PolarisEntityCore> effectiveNewPath =
        newCatalogPath == null ? catalogPath : newCatalogPath;

    Optional<PolarisBaseEntity> found =
        primitives.get(RecordRefs.entityIdentity(entityToRename.getId()), PolarisBaseEntity.class);
    if (found.isEmpty()) {
      return new EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }
    PolarisBaseEntity current = found.get();

    if (current.getEntityVersion() != renamedEntity.getEntityVersion()) {
      return new EntityResult(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, null);
    }
    if (current.cannotBeDroppedOrRenamed()) {
      return new EntityResult(BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RENAMED, null);
    }

    long newParentId = RecordRefs.parentIdOf(effectiveNewPath);
    RecordRef destinationUniqueness =
        RecordRefs.entityUniqueness(newParentId, current.getTypeCode(), renamedEntity.getName());
    Optional<PolarisBaseEntity> destinationTaken =
        primitives.get(destinationUniqueness, PolarisBaseEntity.class);
    if (destinationTaken.isPresent()) {
      return new EntityResult(
          BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, destinationTaken.get().getSubTypeCode());
    }

    PolarisBaseEntity.Builder updatedBuilder =
        new PolarisBaseEntity.Builder(current)
            .name(renamedEntity.getName())
            .properties(renamedEntity.getProperties())
            .internalProperties(renamedEntity.getInternalProperties())
            .entityVersion(current.getEntityVersion() + 1)
            // Real wall-clock time, not this class's clock field — see
            // updateEntityPropertiesIfNotChanged's javadoc for why.
            .lastUpdateTimestamp(System.currentTimeMillis());
    if (newCatalogPath != null) {
      updatedBuilder.parentId(newParentId);
    }
    PolarisBaseEntity updated = updatedBuilder.build();

    RecordRef sourceRef = RecordRefs.entityIdentity(current.getId());
    List<Precondition> preconditions =
        new ArrayList<>(RecordMutations.pathExistsPreconditions(catalogPath, newCatalogPath));
    preconditions.add(
        Precondition.versionEquals(
            sourceRef, Precondition.VersionAttribute.RECORD_VERSION, current.getEntityVersion()));
    preconditions.add(Precondition.notExists(destinationUniqueness));

    OrchestrationResult result =
        orchestrator.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.ENTITY,
                    Mutation.Op.UPDATE,
                    sourceRef,
                    updated,
                    preconditions)));
    return result.isApplied()
        ? new EntityResult(updated)
        : mapFailedRename(
            result,
            sourceRef,
            destinationUniqueness,
            RecordMutations.pathRefs(catalogPath, newCatalogPath));
  }

  /**
   * Maps a non-applied rename {@link OrchestrationResult}, distinguishing which of the mutation's
   * several declared preconditions failed by checking {@link CommitResult#failedPreconditions()}'s
   * reported ref/attribute against each candidate. No old-model precedent: neither old impl commits
   * rename as one atomic write with several declared conditions at once (each checks its conditions
   * as separate reads before a single unconditioned persist), so there is no commit-outcome type to
   * map from — this is a new decision, not a ported one.
   */
  private EntityResult mapFailedRename(
      @NonNull OrchestrationResult result,
      @NonNull RecordRef sourceRef,
      @NonNull RecordRef destinationUniqueness,
      @NonNull Set<RecordRef> pathRefs) {
    if (result.outcome() == OrchestrationResult.Outcome.ROLLBACK_INCOMPLETE) {
      return new EntityResult(
          BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED,
          "rollback incomplete: "
              + result.uncompensated().size()
              + " mutation(s) require admin reclamation");
    }
    CommitResult groupFailure = result.groupFailure().orElseThrow();
    CommitResult.Failure failure = groupFailure.failure().orElseThrow();
    if (failure != CommitResult.Failure.PRECONDITION_FAILED) {
      return new EntityResult(
          BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED, failure.toString());
    }
    if (RecordMutations.failedOnPath(result, pathRefs)) {
      return new EntityResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
    }
    boolean sourceStale =
        groupFailure.failedPreconditions().stream()
            .anyMatch(
                p ->
                    p.op() == Precondition.Op.VERSION_EQUALS
                        && p.ref().filter(sourceRef::equals).isPresent());
    if (sourceStale) {
      return new EntityResult(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, null);
    }
    // Lost the race on the destination name: something else claimed it between our pre-check and
    // the commit. Re-read to report its subtype, mirroring the pre-check path's own shape.
    Optional<PolarisBaseEntity> winner =
        primitives.get(destinationUniqueness, PolarisBaseEntity.class);
    return new EntityResult(
        BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS,
        winner.map(PolarisBaseEntity::getSubTypeCode).orElse(0));
  }

  /**
   * The full removal of one entity: an atomic mutation list combining the entity {@code DELETE},
   * both old impls' private {@code dropEntity} helper (grant-record cleanup, counterpart {@code
   * grantRecordsVersion} bumps, principal-secrets delete), and — when requested — a cleanup {@code
   * TASK} entity, all as ONE commit rather than the old impls' separate calls.
   *
   * <h2>Cleanup-order divergence: dissolved, not resolved</h2>
   *
   * <p>{@code TransactionalMetaStoreManagerImpl}'s private {@code dropEntity} deletes the entity
   * LAST — grant cleanup, then counterpart version bumps, then best-effort policy-mapping cleanup,
   * then {@code ms.deleteEntityInCurrentTxn}. {@code AtomicOperationMetaStoreManager}'s deletes the
   * entity FIRST, and swaps the other two: grant cleanup, then policy-mapping cleanup, THEN the
   * version bumps. Inside one commit, where every mutation applies or none do, the difference
   * between those two orders is unobservable, so the divergence dissolves rather than being
   * resolved: this method builds the mutations in whichever order is simplest. List order is not
   * discarded, though — orchestration merges adjacent mutations of equal domain into groups
   * strictly in list order, and each group is exactly one commit — which is why this method takes
   * care never to emit two writes on one identity (see the counterpart dedup below, whose comment
   * rests on the same rule).
   *
   * <h2>Policy-mapping cleanup (the obligation ticket 92 pays)</h2>
   *
   * <p>Both old impls' private {@code dropEntity} helper runs an UNCONDITIONAL best-effort
   * policy-mapping delete on every drop of a {@code POLICY} or valid policy-target entity (Atomic
   * ~212-235, Transactional ~219-242, identical shape), gated only by {@code entity.getType() ==
   * POLICY || PolicyMappingUtil.isValidTargetEntityType(entity.getType(), entity.getSubType())} —
   * NOT by {@code dropEntityIfExists}'s {@code cleanup} flag, which governs only the {@code
   * POLICY_HAS_MAPPINGS} pre-check (see the correction history in ticket 91's completion record).
   * This method ports it as {@code DELETE} mutations folded into the same single commit: for a
   * dropped {@code POLICY}, every mapping on its {@code by-policy} anchor; for a dropped valid
   * target, every mapping on its {@code by-target} anchor. The old impls' {@code catch
   * (UnsupportedOperationException)} best-effort wrapper dissolves rather than being ported: it
   * existed for backends that never implemented policy-mapping persistence, and both new-model
   * stores serve the kind — a store that does not would reject the whole commit loudly, which is
   * the new model's documented refusal, not a case to swallow. Mapping deletes bump no entity
   * version (neither old impl bumps any for policy mappings) and are deduplicated by identity for
   * headroom, the same reason the grant deletes above are.
   *
   * <p>{@code PolarisBaseEntity} instances in {@code droppedEntities} beyond the first are the
   * catalog's own recursively-dropped admin {@code CATALOG_ROLE} (see the caller): a grant between
   * two entities that are BOTH being removed in this same call (e.g. {@code CATALOG_MANAGE_ACCESS}
   * with securable=catalog, grantee=admin-role) must delete the grant but must NOT bump either
   * side's {@code grantRecordsVersion} — both sides are about to be deleted by this SAME commit,
   * and an {@code UPDATE} mutation on an identity the SAME commit also {@code DELETE}s would either
   * resurrect the row or race against mutation order, neither of which any old-model equivalent
   * needs to consider (their two calls are genuinely separate writes). {@code droppedIds} is what
   * lets this method recognize and skip that case.
   */
  private void collectDropMutations(
      @NonNull List<PolarisBaseEntity> droppedEntities,
      @NonNull Set<Long> droppedIds,
      @NonNull List<Precondition> topLevelPreconditions,
      @NonNull List<Mutation> mutations) {
    List<PolarisGrantRecord> allGrants = new ArrayList<>();
    List<PolarisPolicyMappingRecord> allMappings = new ArrayList<>();
    boolean first = true;
    for (PolarisBaseEntity dropped : droppedEntities) {
      mutations.add(
          Mutation.of(
              PolarisRecordKinds.ENTITY,
              Mutation.Op.DELETE,
              RecordRefs.entityIdentity(dropped.getId()),
              null,
              first ? topLevelPreconditions : List.of()));
      first = false;
      allGrants.addAll(
          primitives
              .list(
                  PolarisRecordKinds.GRANT_RECORD,
                  PolarisRecordKinds.GRANT_RECORD_BY_SECURABLE,
                  List.of(dropped.getCatalogId(), dropped.getId()),
                  PageToken.readEverything(),
                  PolarisGrantRecord.class)
              .items());
      allGrants.addAll(
          primitives
              .list(
                  PolarisRecordKinds.GRANT_RECORD,
                  PolarisRecordKinds.GRANT_RECORD_BY_GRANTEE,
                  List.of(dropped.getCatalogId(), dropped.getId()),
                  PageToken.readEverything(),
                  PolarisGrantRecord.class)
              .items());
      if (dropped.getType() == PolarisEntityType.PRINCIPAL) {
        String clientId = PrincipalEntity.of(dropped).getClientId();
        if (clientId != null && !clientId.isEmpty()) {
          mutations.add(
              Mutation.of(
                  PolarisRecordKinds.PRINCIPAL_SECRETS,
                  Mutation.Op.DELETE,
                  RecordRefs.secretsIdentity(clientId),
                  null));
        }
      }
      if (dropped.getType() == PolarisEntityType.POLICY) {
        allMappings.addAll(
            RecordRefs.policyMappingsOn(
                primitives,
                PolarisRecordKinds.POLICY_MAPPING_BY_POLICY,
                dropped.getCatalogId(),
                dropped.getId()));
      } else if (PolicyMappingUtil.isValidTargetEntityType(
          dropped.getType(), dropped.getSubType())) {
        allMappings.addAll(
            RecordRefs.policyMappingsOn(
                primitives,
                PolarisRecordKinds.POLICY_MAPPING_BY_TARGET,
                dropped.getCatalogId(),
                dropped.getId()));
      }
    }

    Map<RecordRef, PolarisPolicyMappingRecord> distinctMappings = new LinkedHashMap<>();
    for (PolarisPolicyMappingRecord m : allMappings) {
      distinctMappings.putIfAbsent(RecordRefs.policyMappingIdentity(m), m);
    }
    for (RecordRef mappingRef : distinctMappings.keySet()) {
      mutations.add(
          Mutation.of(PolarisRecordKinds.POLICY_MAPPING, Mutation.Op.DELETE, mappingRef, null));
    }

    // Deduplicated by target ref, first-seen order preserved (Finding 3, independent review,
    // 2026-08-18): dropping a catalog together with its sole remaining admin role collects the
    // SAME catalog<->adminRole grant twice — once through the catalog's own
    // GRANT_RECORD_BY_SECURABLE query, once through the admin role's own
    // GRANT_RECORD_BY_GRANTEE query — since droppedEntities walks both entities' grants
    // independently. Both stores treat a DELETE of an already-deleted record as a no-op (verified
    // in TreeMapDurableRecordStore#applyMutation's DELETE case), so a duplicate here was never a
    // correctness bug, only wasted headroom against maxItemsPerCommit.
    Map<RecordRef, PolarisGrantRecord> distinctGrants = new LinkedHashMap<>();
    for (PolarisGrantRecord g : allGrants) {
      distinctGrants.putIfAbsent(RecordRefs.grantIdentity(g), g);
    }

    // One combined counterpart set across every dropped entity, not one per entity: a counterpart
    // reached from two different grants (e.g. the same principal role usage-granted on both the
    // catalog admin role AND some unrelated role) must be bumped exactly once. Two UPDATE
    // mutations on the same identity in one commit would have the second's version precondition
    // fail against the first's already-applied bump, because orchestration preserves list order
    // into the commit, spuriously failing the whole drop.
    Set<Long> counterpartIds = new HashSet<>();
    for (PolarisGrantRecord g : distinctGrants.values()) {
      mutations.add(
          Mutation.of(
              PolarisRecordKinds.GRANT_RECORD,
              Mutation.Op.DELETE,
              RecordRefs.grantIdentity(g),
              null));
      if (!droppedIds.contains(g.getGranteeId())) {
        counterpartIds.add(g.getGranteeId());
      }
      if (!droppedIds.contains(g.getSecurableId())) {
        counterpartIds.add(g.getSecurableId());
      }
    }
    if (!counterpartIds.isEmpty()) {
      List<RecordRef> counterpartRefs =
          counterpartIds.stream().map(RecordRefs::entityIdentity).toList();
      primitives.getMany(counterpartRefs, PolarisBaseEntity.class).stream()
          .filter(Optional::isPresent)
          .map(Optional::get)
          .forEach(
              counterpart ->
                  mutations.add(RecordMutations.bumpGrantRecordsVersion(counterpart).mutation()));
    }
  }

  /**
   * Ported from both old impls' {@code dropEntityIfExists}, which agree on every check here except
   * for the retrofit's path hardening (neither re-checks {@code catalogPath} at all, {@code
   * AtomicOperationMetaStoreManager}'s own choice this generally matches — but the brief calls for
   * the retrofit natively here, so this diverges from that parity choice on this one point,
   * disclosed rather than silent) and for the one-commit cleanup-task atomicity described on {@link
   * #collectDropMutations}. {@code ENTITY_NOT_FOUND}, {@code ENTITY_UNDROPPABLE}, the
   * passthrough-facade branch, {@code NAMESPACE_NOT_EMPTY}/{@code CATALOG_NOT_EMPTY} with
   * single-admin-role recursion — all read directly off both old impls, which agree on every status
   * and every threshold here.
   */
  @Override
  public @NonNull DropEntityResult dropEntityIfExists(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup) {
    diagnostics.checkNotNull(entityToDrop, "unexpected_null_entity");

    Optional<PolarisBaseEntity> found =
        primitives.get(RecordRefs.entityIdentity(entityToDrop.getId()), PolarisBaseEntity.class);
    if (found.isEmpty()) {
      return new DropEntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }
    PolarisBaseEntity current = found.get();

    if (current.cannotBeDroppedOrRenamed()) {
      return new DropEntityResult(BaseResult.ReturnStatus.ENTITY_UNDROPPABLE, null);
    }

    List<PolarisBaseEntity> recurseCatalogRoles = List.of();
    if (current.getType() == PolarisEntityType.CATALOG) {
      long catalogId = current.getId();
      CatalogEntity catalogEntity = CatalogEntity.of(current);
      List<PolarisBaseEntity> children =
          RecordRefs.rawChildEntities(primitives, catalogId, catalogId);
      // Passthrough-facade catalogs may carry passthrough entities that are not source-of-truth;
      // both old impls temporarily allow dropping over them when the feature config says so.
      boolean allowNonEmptyPassthrough =
          catalogEntity.isPassthroughFacade()
              && callCtx
                  .getRealmConfig()
                  .getConfig(
                      FeatureConfiguration.ALLOW_DROPPING_NON_EMPTY_PASSTHROUGH_FACADE_CATALOG,
                      catalogEntity);
      if (!allowNonEmptyPassthrough
          && children.stream()
              .anyMatch(e -> e.getTypeCode() == PolarisEntityType.NAMESPACE.getCode())) {
        return new DropEntityResult(
            BaseResult.ReturnStatus.NAMESPACE_NOT_EMPTY,
            catalogEntity.isPassthroughFacade()
                ? String.format(
                    "Set %s to true to drop non-empty passthrough facade catalogs",
                    FeatureConfiguration.ALLOW_DROPPING_NON_EMPTY_PASSTHROUGH_FACADE_CATALOG.key())
                : null);
      }
      List<PolarisBaseEntity> catalogRoles =
          children.stream()
              .filter(e -> e.getTypeCode() == PolarisEntityType.CATALOG_ROLE.getCode())
              .toList();
      if (catalogRoles.size() > 1) {
        return new DropEntityResult(BaseResult.ReturnStatus.CATALOG_NOT_EMPTY, null);
      }
      // If exactly one role is left, it should be the admin role (not validated, same as both old
      // impls) — drop it too, in the SAME commit as the catalog.
      recurseCatalogRoles = catalogRoles;
    } else if (current.getType() == PolarisEntityType.NAMESPACE
        && !RecordRefs.rawChildEntities(primitives, current.getCatalogId(), current.getId())
            .isEmpty()) {
      return new DropEntityResult(BaseResult.ReturnStatus.NAMESPACE_NOT_EMPTY, null);
    } else if (current.getType() == PolarisEntityType.POLICY
        && !cleanup
        && !RecordRefs.policyMappingsOn(
                primitives,
                PolarisRecordKinds.POLICY_MAPPING_BY_POLICY,
                current.getCatalogId(),
                current.getId())
            .isEmpty()) {
      // Ported from both old impls: dropping a still-attached POLICY without cleanup is refused.
      // Their catch(UnsupportedOperationException) wrapper dissolves for the same reason
      // collectDropMutations's javadoc gives for the cleanup itself. Unexercised by the fixture
      // (its policy drops pass cleanup=true), implemented for drop-surface parity now that
      // attachPolicyToEntity makes the state reachable.
      return new DropEntityResult(BaseResult.ReturnStatus.POLICY_HAS_MAPPINGS, null);
    }

    List<PolarisBaseEntity> droppedEntities = new ArrayList<>();
    droppedEntities.add(current);
    droppedEntities.addAll(recurseCatalogRoles);
    Set<Long> droppedIds =
        Set.copyOf(droppedEntities.stream().map(PolarisBaseEntity::getId).toList());

    List<Mutation> mutations = new ArrayList<>();
    collectDropMutations(
        droppedEntities,
        droppedIds,
        RecordMutations.pathExistsPreconditions(catalogPath, null),
        mutations);

    Long cleanupTaskId = null;
    if (cleanup && current.getType() != PolarisEntityType.POLICY) {
      // Cleanup-task creation rides in the SAME mutation list as the drop, unlike both old impls:
      // Transactional gets this atomicity for free from its wrapping DB transaction, Atomic's own
      // TODO concedes a crash between its two separate calls can drop the entity with no task
      // persisted at all. Folding it into one commit closes that gap rather than reproducing it.
      Map<String, String> properties = new HashMap<>();
      properties.put(
          PolarisTaskConstants.TASK_TYPE,
          String.valueOf(AsyncTaskType.ENTITY_CLEANUP_SCHEDULER.typeCode()));
      properties.put(PolarisTaskConstants.TASK_DATA, PolarisObjectMapperUtil.serialize(current));
      PolarisBaseEntity.Builder taskBuilder =
          new PolarisBaseEntity.Builder()
              .id(primitives.generateNewId())
              .catalogId(0L)
              .name("entityCleanup_" + entityToDrop.getId())
              .typeCode(PolarisEntityType.TASK.getCode())
              .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
              // The INJECTED clock here, unlike the entity-timestamp sites, because this is the one
              // place the old model uses it too: AtomicOperationMetaStoreManager stamps the cleanup
              // task with clock.millis() while BaseMetaStoreManager stamps entities with
              // System.currentTimeMillis(). The split is not an inconsistency to tidy up — task
              // leasing reads the same injected clock to decide whether a lease has expired, so a
              // task stamped from real time while the lease check runs on a test clock would mix
              // two
              // time bases. Entity timestamps have no such reader.
              .createTimestamp(clock.millis())
              .propertiesAsMap(properties);
      if (cleanupProperties != null) {
        taskBuilder.internalPropertiesAsMap(cleanupProperties);
      }
      PolarisBaseEntity taskEntity = prepareNewEntity(taskBuilder.build());
      cleanupTaskId = taskEntity.getId();
      mutations.add(
          Mutation.of(
              PolarisRecordKinds.ENTITY,
              Mutation.Op.CREATE,
              RecordRefs.entityIdentity(taskEntity.getId()),
              taskEntity,
              List.of(
                  Precondition.notExists(
                      RecordRefs.entityUniqueness(
                          taskEntity.getParentId(),
                          taskEntity.getTypeCode(),
                          taskEntity.getName())))));
    }

    OrchestrationResult result = orchestrator.commit(mutations);
    if (!result.isApplied()) {
      return mapFailedDrop(result, RecordMutations.pathRefs(catalogPath, null));
    }
    return cleanupTaskId != null ? new DropEntityResult(cleanupTaskId) : new DropEntityResult();
  }

  /**
   * Maps a non-applied drop {@link OrchestrationResult}. No old-model precedent — same reasoning as
   * {@link #mapFailedRename}: neither old impl commits its whole cleanup sequence as one
   * conditioned write, so there is no commit-outcome type to map from.
   */
  private DropEntityResult mapFailedDrop(
      @NonNull OrchestrationResult result, @NonNull Set<RecordRef> pathRefs) {
    if (result.outcome() == OrchestrationResult.Outcome.ROLLBACK_INCOMPLETE) {
      return new DropEntityResult(
          BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED,
          "rollback incomplete: "
              + result.uncompensated().size()
              + " mutation(s) require admin reclamation");
    }
    CommitResult.Failure failure = result.groupFailure().orElseThrow().failure().orElseThrow();
    if (failure != CommitResult.Failure.PRECONDITION_FAILED) {
      return new DropEntityResult(
          BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED, failure.toString());
    }
    return RecordMutations.failedOnPath(result, pathRefs)
        ? new DropEntityResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null)
        : new DropEntityResult(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, null);
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
  @Override
  public @NonNull EntityResult loadEntity(
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

  /** The principal-role lookup the previous single manager offered as a default; body unchanged. */
  private Optional<PrincipalRoleEntity> findPrincipalRoleByName(
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
