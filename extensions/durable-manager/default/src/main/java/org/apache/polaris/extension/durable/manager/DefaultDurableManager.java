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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.ToLongFunction;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.exceptions.AlreadyExistsException;
import org.apache.polaris.core.persistence.PolarisObjectMapperUtil;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
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
import org.apache.polaris.core.persistence.resolver.ResolvedEntityReads;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.PolicyMappingUtil;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.spi.durable.CatalogDurableManager;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableManager;
import org.apache.polaris.spi.durable.DurableOrchestrator;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.EventDurableManager;
import org.apache.polaris.spi.durable.GrantDurableManager;
import org.apache.polaris.spi.durable.LookupPath;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.OrchestrationResult;
import org.apache.polaris.spi.durable.PolicyDurableManager;
import org.apache.polaris.spi.durable.Precondition;
import org.apache.polaris.spi.durable.PrincipalDurableManager;
import org.apache.polaris.spi.durable.RecordRef;
import org.apache.polaris.spi.durable.RecordVersions;
import org.apache.polaris.spi.durable.SecretsDurableManager;
import org.apache.polaris.spi.durable.TaskDurableManager;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The single new-model durable manager. At ticket 96 this class is a wholesale replacement for both
 * {@code AtomicOperationMetaStoreManager} and the transactional old-model manager it sits alongside
 * today; there is no per-method migration, the old implementations are deleted in one step once
 * this class covers their surface.
 *
 * <p>This manager owns every business rule and knows no storage topology. It implements {@link
 * DurableManager}, {@link GrantDurableManager}, {@link SecretsDurableManager}, {@link
 * PolicyDurableManager} and {@link EventDurableManager} on one object because {@code
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
 * #primitives} is the READ door and the source of {@link DurableRecordStore#generateNewId} — a read
 * may legitimately be organized by orchestration too (the same-backend read/write optimization
 * allowance recorded 2026-08-10), but it is not required to be, and this class does not use that
 * option: every read here goes straight to the primitives handle. That handle IS the floor (EJ,
 * 2026-08-17/18): in a multi-store deployment it is the routing implementation, whose kind-to-store
 * mapping hides BEHIND the primitives SPI, so holding one handle carries no storage-topology
 * knowledge — this class cannot tell one backend from five, and the kind-keyed resolver it used to
 * hold (a reachable routing table) is dissolved (ticket 111, 2026-08-18).
 *
 * <p>This class never reads the OLD {@link org.apache.polaris.spi.durable.DurablePrimitives} handle
 * carried on {@link PolarisCallContext}. That handle is the old model's write/read door and
 * reaching for it here would silently reintroduce the coupling this class exists to remove.
 *
 * <h2>A third collaborator that is not a data-access door</h2>
 *
 * <p>{@link #secretsGenerator} produces a principal's client id and secret. In the old model this
 * lived one layer down: the old bindings hand {@code PrincipalSecretsGenerator.RANDOM_SECRETS} to
 * the primitives implementation itself (e.g. {@code new TreeMapDurablePrimitivesImpl(diag, store,
 * RANDOM_SECRETS)}), so the OLD primitives layer generates secrets. The new {@link
 * DurableRecordStore} carries no such parameter and must not gain one: generating a credential is a
 * business rule, not a storage concern, so S3 puts it here, one layer up from where it used to
 * live. {@link PrincipalSecretsGenerator} is an existing type — this coins no new term, it only
 * moves an existing collaborator to its correct layer.
 */
public class DefaultDurableManager
    implements DurableManager,
        CatalogDurableManager,
        PrincipalDurableManager,
        TaskDurableManager,
        ResolvedEntityReads,
        GrantDurableManager,
        SecretsDurableManager,
        PolicyDurableManager,
        EventDurableManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDurableManager.class);

  private final Clock clock;
  private final PolarisDiagnostics diagnostics;
  private final DurableOrchestrator orchestrator;
  private final DurableRecordStore primitives;
  private final PrincipalSecretsGenerator secretsGenerator;

  public DefaultDurableManager(
      @NonNull Clock clock,
      @NonNull PolarisDiagnostics diagnostics,
      @NonNull DurableOrchestrator orchestrator,
      @NonNull DurableRecordStore primitives,
      @NonNull PrincipalSecretsGenerator secretsGenerator) {
    this.clock = clock;
    this.diagnostics = diagnostics;
    this.orchestrator = orchestrator;
    this.primitives = primitives;
    this.secretsGenerator = secretsGenerator;
  }

  // ---------------------------------------------------------------------------------- helpers

  private DurableRecordStore entityStore() {
    return primitives;
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
   * {@code TransactionalMetaStoreManagerImpl}: for plain reads and for {@link
   * #createEntityIfNotExists}'s id/name derivation, {@code catalogPath} is never re-resolved
   * against the store — it derives catalogId/parentId directly from the path the same way Atomic
   * does (raw {@code 0L} there; the named constants here are the same value).
   *
   * <p><b>CORRECTION to this method's increment-2 disclosure</b> (EJ's retrofit, 2026-08-17): that
   * text claimed this manager never returns {@code CATALOG_PATH_CANNOT_BE_RESOLVED}, matching only
   * Atomic. It now does, for {@link #createEntityIfNotExists}, {@link #createEntitiesIfNotExist},
   * {@link #renameEntity} and {@link #dropEntityIfExists}: each attaches an {@code EXISTS}
   * precondition per {@code catalogPath} entity to its mutation (see {@link
   * #pathExistsPreconditions}), so a path entity deleted between the read below and the commit
   * fails the write instead of silently succeeding underneath it — the concrete failure mode this
   * closes is a table left hanging under a concurrently-dropped namespace, which Atomic's own
   * unconditional derivation cannot detect. This is a REAL happens-before guarantee neither old
   * implementation has: Atomic never re-checks the path at all, and Transactional's re-check (via
   * the package-private {@code PolarisEntityResolver}) is safe only because it runs inside the same
   * DB transaction as the write — nothing states that as a condition, a wrapping transaction just
   * happens to serialize against the concurrent delete. {@code updateEntityPropertiesIfNotChanged}
   * and its batch form deliberately do NOT get this treatment: neither old implementation's update
   * path uses {@code catalogPath} to reach the entity being updated (it is resolved directly by
   * catalogId+id), so there is no "hanging under a deleted path" failure mode for update to close.
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
   * EJ's retrofit (2026-08-17): one {@link Precondition#exists} per distinct entity across both
   * path arguments, so the store checks at commit time that every element the caller resolved this
   * write against is still there. Path entities are {@code ENTITY} records like the write target
   * they gate, so they share the atomicity domain and add no extra round trip.
   *
   * @param extraPath a second path to fold in, deduplicated against {@code path} by id — {@link
   *     #renameEntity} is the only caller that passes one, for the destination path alongside the
   *     source path
   */
  private static List<Precondition> pathExistsPreconditions(
      @Nullable List<PolarisEntityCore> path, @Nullable List<PolarisEntityCore> extraPath) {
    return pathIds(path, extraPath).stream()
        .map(id -> Precondition.exists(entityIdentity(id)))
        .toList();
  }

  /** The identity refs {@link #pathExistsPreconditions} declared, for mapping a failure back. */
  private static Set<RecordRef> pathRefs(
      @Nullable List<PolarisEntityCore> path, @Nullable List<PolarisEntityCore> extraPath) {
    Set<RecordRef> refs = new HashSet<>();
    for (long id : pathIds(path, extraPath)) {
      refs.add(entityIdentity(id));
    }
    return refs;
  }

  private static Set<Long> pathIds(
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
  private static boolean failedOnPath(
      @NonNull OrchestrationResult result, @NonNull Set<RecordRef> pathRefs) {
    return result.groupFailure().map(CommitResult::failedPreconditions).orElse(List.of()).stream()
        .anyMatch(p -> p.ref().filter(pathRefs::contains).isPresent());
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
   *     #createCatalog}/{@link #createPrincipal}) that collided
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
  // Package-private rather than private: DefaultDurableManagerEntityOpsTest asserts this rule
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
    if (failedOnPath(result, pathRefs)) {
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

  /**
   * Maps a non-applied {@code createPrincipal}/{@code createCatalog} {@link OrchestrationResult}.
   * Having already pre-checked the relevant uniqueness before building the mutation list, a failure
   * here can only be a lost race on that same check — the identical collision the pre-check path
   * itself reports — or a genuine bug (TOO_MANY_ITEMS, DOMAIN_MISMATCH, ROLLBACK_INCOMPLETE). No
   * old-model precedent for this mapping exists for the same reason {@link #mapFailedCreate} has
   * none: the old primitives interface has no multi-outcome commit result, and for {@code
   * createCatalog}/{@code createPrincipal} specifically, neither old impl wraps its several writes
   * in one shared transaction at all (see {@link #createCatalog}'s and {@link #createPrincipal}'s
   * own javadoc for what the one-commit shape closes as a side effect).
   */
  private BaseResult.ReturnStatus classifyFailedCreate(@NonNull OrchestrationResult result) {
    if (result.outcome() == OrchestrationResult.Outcome.ROLLBACK_INCOMPLETE) {
      return BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED;
    }
    CommitResult.Failure failure = result.groupFailure().orElseThrow().failure().orElseThrow();
    return failure == CommitResult.Failure.PRECONDITION_FAILED
        ? BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS
        : BaseResult.ReturnStatus.UNEXPECTED_ERROR_SIGNALED;
  }

  /** {@code extraInformation} for a {@link #classifyFailedCreate} mapping, when non-null helps. */
  private @Nullable String failureDetail(@NonNull OrchestrationResult result) {
    if (result.outcome() == OrchestrationResult.Outcome.ROLLBACK_INCOMPLETE) {
      return "rollback incomplete: "
          + result.uncompensated().size()
          + " mutation(s) require admin reclamation";
    }
    CommitResult.Failure failure = result.groupFailure().orElseThrow().failure().orElseThrow();
    return failure == CommitResult.Failure.PRECONDITION_FAILED ? null : failure.toString();
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
    // Mutable, not Stream#toList()'s unmodifiable result: found by testDropEntities (increment 5),
    // whose dropEntity helper calls children.clear() on this exact return value when exactly one
    // catalog role is left. A real increment-2 bug, only reachable once drop existed to walk that
    // far — fixed here since it blocks this increment's own target, not the type_code anchor gap
    // the tripwire on this method is actually about.
    List<EntityNameLookupRecord> records =
        new ArrayList<>(
            listChildEntities(catalogPath, entityType, entitySubType, pageToken).stream()
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
        pageToken, listChildEntities(catalogPath, entityType, entitySubType, pageToken), null);
  }

  @Override
  public @NonNull GenerateEntityIdResult generateNewEntityId(@NonNull PolarisCallContext callCtx) {
    return new GenerateEntityIdResult(entityStore().generateNewId());
  }

  /**
   * {@code PRINCIPAL_SECRETS}'s identity ref: {@code (realm, client-id)} — every field is part of
   * the key, the same shape as {@link #grantIdentity}.
   */
  private static RecordRef secretsIdentity(@NonNull String clientId) {
    return RecordRef.byIdentity(PolarisRecordKinds.PRINCIPAL_SECRETS, List.of(clientId));
  }

  private DurableRecordStore secretsStore() {
    return primitives;
  }

  /**
   * Ported from {@code TreeMapDurablePrimitivesImpl#generateNewPrincipalSecretsInCurrentTxn}'s
   * collision-avoidance loop: {@link #secretsGenerator} produces a client id that is expected to be
   * unique but not reserved the way {@link DurableRecordStore#generateNewId} reserves an entity id,
   * so this re-checks and retries rather than trusting the generator outright.
   */
  private PolarisPrincipalSecrets generateUniqueSecrets(
      @NonNull String principalName, long principalId) {
    DurableRecordStore store = secretsStore();
    PolarisPrincipalSecrets candidate;
    do {
      candidate = secretsGenerator.produceSecrets(principalName, principalId);
    } while (store
        .get(secretsIdentity(candidate.getPrincipalClientId()), PolarisPrincipalSecrets.class)
        .isPresent());
    return candidate;
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
        entityStore().get(entityIdentity(principal.getId()), PolarisBaseEntity.class);
    if (existing.isPresent()) {
      // Same-id idempotent-retry collisions are necessarily sequential (the id was already
      // reserved by generateNewEntityId before this call reached us), so this pre-check needs no
      // atomicity of its own — matches both old impls' own comment to this effect.
      return loadExistingPrincipal(existing.get());
    }

    boolean nameTaken =
        entityStore()
            .get(
                entityUniqueness(
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
        entityUniqueness(prepared.getParentId(), prepared.getTypeCode(), prepared.getName());

    List<Mutation> mutations =
        List.of(
            Mutation.of(
                PolarisRecordKinds.PRINCIPAL_SECRETS,
                Mutation.Op.CREATE,
                secretsIdentity(secrets.getPrincipalClientId()),
                secrets,
                List.of(Precondition.none())),
            Mutation.of(
                PolarisRecordKinds.ENTITY,
                Mutation.Op.CREATE,
                entityIdentity(prepared.getId()),
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
    BaseResult.ReturnStatus failureStatus = classifyFailedCreate(result);
    if (failureStatus == BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS) {
      // Finding 1 (independent review, 2026-08-18): a lost race can mean someone else already
      // committed THIS exact principal (the id this call reserved before it started) rather than
      // a genuine name conflict. Re-reading by identity rather than by principalUniqueness is
      // deliberate and simpler than createEntityIfNotExists's equivalent check: ids are reserved
      // by the caller before this method runs, so a hit here is necessarily this exact id — no
      // separate id-equality comparison is needed the way it is for a uniqueness-keyed read,
      // which could belong to any id.
      Optional<PolarisBaseEntity> winner =
          entityStore().get(entityIdentity(prepared.getId()), PolarisBaseEntity.class);
      if (winner.isPresent()) {
        return loadExistingPrincipal(winner.get());
      }
    }
    return new CreatePrincipalResult(failureStatus, failureDetail(result));
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
        secretsStore().get(secretsIdentity(clientId), PolarisPrincipalSecrets.class).orElse(null);
    diagnostics.checkNotNull(
        secrets,
        "missing_principal_secrets",
        "clientId={} principal={}",
        clientId,
        refreshPrincipal);
    return new CreatePrincipalResult(existing, secrets);
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
        entityStore().get(entityIdentity(catalog.getId()), PolarisBaseEntity.class);
    if (existingCatalog.isPresent()) {
      diagnostics.check(
          existingCatalog.get().getTypeCode() == PolarisEntityType.CATALOG.getCode(),
          "not_a_catalog",
          "catalog={}",
          catalog);
      return loadExistingCatalog(existingCatalog.get());
    }

    PolarisBaseEntity preparedCatalog = prepareNewEntity(catalog);
    long adminRoleId = entityStore().generateNewId();
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
            entityIdentity(preparedCatalog.getId()),
            preparedCatalog,
            List.of(
                Precondition.notExists(
                    entityUniqueness(
                        preparedCatalog.getParentId(),
                        preparedCatalog.getTypeCode(),
                        preparedCatalog.getName())))));
    mutations.add(
        Mutation.of(
            PolarisRecordKinds.ENTITY,
            Mutation.Op.CREATE,
            entityIdentity(adminRole.getId()),
            adminRole,
            List.of(
                Precondition.notExists(
                    entityUniqueness(
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
      mutations.add(createGrantMutation(grantRecord));
      VersionBump granteeBump = bumpGrantRecordsVersion(adminRoleState);
      mutations.add(granteeBump.mutation());
      adminRoleState = granteeBump.updated();
      VersionBump securableBump = bumpGrantRecordsVersion(catalogState);
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
        assignees.add(mustLoadEntity(principalRole, "grantee_not_found"));
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
      mutations.add(createGrantMutation(grantRecord));
      mutations.add(bumpGrantRecordsVersion(principalRole).mutation());
      VersionBump securableBump = bumpGrantRecordsVersion(adminRoleState);
      mutations.add(securableBump.mutation());
      adminRoleState = securableBump.updated();
    }

    OrchestrationResult result = orchestrator.commit(mutations);
    if (result.isApplied()) {
      // Matches both old impls: returns the pre-grant state of the catalog and admin role, not
      // the state after the grant-driven grantRecordsVersion bumps.
      return new CreateCatalogResult(preparedCatalog, adminRole);
    }
    BaseResult.ReturnStatus failureStatus = classifyFailedCreate(result);
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
          entityStore().get(entityIdentity(preparedCatalog.getId()), PolarisBaseEntity.class);
      if (winner.isPresent()) {
        return loadExistingCatalog(winner.get());
      }
    }
    return new CreateCatalogResult(failureStatus, failureDetail(result));
  }

  /**
   * Loads the existing (catalog, admin role) pair by the catalog's id — shared by {@link
   * #createCatalog}'s identity pre-check (this id already exists) and its lost-race branch (Finding
   * 1: the SAME rule now applies at both of a create path's collision points).
   */
  private CreateCatalogResult loadExistingCatalog(@NonNull PolarisBaseEntity existingCatalog) {
    PolarisBaseEntity adminRole =
        entityStore()
            .get(
                entityUniqueness(
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
    DurableRecordStore store = entityStore();
    RecordRef uniqueness =
        entityUniqueness(prepared.getParentId(), prepared.getTypeCode(), prepared.getName());

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

    List<Precondition> preconditions = new ArrayList<>(pathExistsPreconditions(catalogPath, null));
    preconditions.add(Precondition.notExists(uniqueness));
    OrchestrationResult result =
        orchestrator.commit(
            List.of(
                Mutation.of(
                    PolarisRecordKinds.ENTITY,
                    Mutation.Op.CREATE,
                    entityIdentity(prepared.getId()),
                    prepared,
                    preconditions)));
    return result.isApplied()
        ? new EntityResult(prepared)
        : mapFailedCreate(store, uniqueness, prepared, pathRefs(catalogPath, null), result);
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
    DurableRecordStore store = entityStore();
    List<PolarisBaseEntity> resolved = new ArrayList<>(entities.size());
    List<Mutation> mutations = new ArrayList<>();
    Map<RecordRef, PolarisBaseEntity> byUniqueness = new HashMap<>();

    for (PolarisBaseEntity entity : entities) {
      PolarisBaseEntity prepared = prepareNewEntity(entity);
      RecordRef uniqueness =
          entityUniqueness(prepared.getParentId(), prepared.getTypeCode(), prepared.getName());
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
            new ArrayList<>(pathExistsPreconditions(catalogPath, null));
        preconditions.add(Precondition.notExists(uniqueness));
        mutations.add(
            Mutation.of(
                PolarisRecordKinds.ENTITY,
                Mutation.Op.CREATE,
                entityIdentity(prepared.getId()),
                prepared,
                preconditions));
        byUniqueness.put(uniqueness, prepared);
      }
      // else: idempotent retry, same id — no mutation needed, `resolved` already carries the
      // entity we were trying to create.
    }

    Set<RecordRef> pathRefs = pathRefs(catalogPath, null);
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
      if (failedOnPath(result, pathRefs)) {
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
      RecordRef resolvedTarget = entityIdentity(creating.getId());
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
   * <p>{@code catalogPath} is accepted but not consulted, same parity choice {@link #catalogIdOf}
   * documents elsewhere — but for a different reason than usual: it is not merely unconsulted by
   * the old impl this follows for parity, it is genuinely irrelevant to the write. Neither old
   * implementation resolves the entity being updated THROUGH its path; both go straight to it by
   * {@code catalogId}+{@code id}. {@code TransactionalMetaStoreManagerImpl} DOES additionally
   * re-resolve {@code catalogPath} via the package-private {@code PolarisEntityResolver} and can
   * return {@code CATALOG_PATH_CANNOT_BE_RESOLVED} for a stale one — found while reading it for
   * this increment, and NOT reproduced here: the retrofit's target failure mode is a write that
   * SUCCEEDS underneath a deleted path (see {@link #catalogIdOf}'s javadoc), and an update's own
   * version precondition below already fails a concurrently-changed entity regardless of what
   * happened to its ancestors, so there is no equivalent hole for the retrofit to close. Flagging
   * this rather than silently applying the retrofit here anyway, since the brief's own retrofit
   * list names only the create paths plus this increment's rename/drop.
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
   * delivers. Realized here as two {@code VERSION_EQUALS} preconditions mirroring {@link
   * #bumpGrantRecordsVersion}'s own two-column CAS, since a {@code VERSION_EQUALS} precondition
   * against an absent record already evaluates false (see {@code Precondition}'s {@code holds()}),
   * so absence and staleness fail the same way without a separate branch.
   */
  @Override
  public @NonNull EntityResult updateEntityPropertiesIfNotChanged(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entity) {
    diagnostics.checkNotNull(entity, "unexpected_null_entity");

    RecordRef ref = entityIdentity(entity.getId());
    Optional<PolarisBaseEntity> current = entityStore().get(ref, PolarisBaseEntity.class);
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
            // and only advances via explicit clock.add(...) (for ticket 92's task-leasing tests) —
            // using it here made every update's timestamp read as BEFORE the entity's own
            // real-time createTimestamp. Found by testUpdateEntities/testRename failing on exactly
            // that ordering.
            .lastUpdateTimestamp(System.currentTimeMillis())
            .build();

    OrchestrationResult result =
        orchestrator.commit(List.of(entityPropertiesUpdateMutation(ref, currentEntity, updated)));
    return result.isApplied()
        ? new EntityResult(updated)
        : new EntityResult(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, null);
  }

  /**
   * The two-precondition {@code ENTITY} UPDATE shared by {@link
   * #updateEntityPropertiesIfNotChanged} and its batch form: both halves of the CAS {@code
   * checkConditionsForWriteEntityInCurrentTxn} performs (record version AND grant-records version,
   * both asserted unchanged), gating the write that carries the new {@code properties}/{@code
   * internalProperties} state.
   */
  private static Mutation entityPropertiesUpdateMutation(
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
      RecordRef ref = entityIdentity(entity.getId());
      Optional<PolarisBaseEntity> current = entityStore().get(ref, PolarisBaseEntity.class);
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
      mutations.add(entityPropertiesUpdateMutation(ref, currentEntity, updatedEntity));
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
   * {@link #catalogIdOf}'s javadoc).
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
        entityStore().get(entityIdentity(entityToRename.getId()), PolarisBaseEntity.class);
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

    long newParentId = parentIdOf(effectiveNewPath);
    RecordRef destinationUniqueness =
        entityUniqueness(newParentId, current.getTypeCode(), renamedEntity.getName());
    Optional<PolarisBaseEntity> destinationTaken =
        entityStore().get(destinationUniqueness, PolarisBaseEntity.class);
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

    RecordRef sourceRef = entityIdentity(current.getId());
    List<Precondition> preconditions =
        new ArrayList<>(pathExistsPreconditions(catalogPath, newCatalogPath));
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
            result, sourceRef, destinationUniqueness, pathRefs(catalogPath, newCatalogPath));
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
    if (failedOnPath(result, pathRefs)) {
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
        entityStore().get(destinationUniqueness, PolarisBaseEntity.class);
    return new EntityResult(
        BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS,
        winner.map(PolarisBaseEntity::getSubTypeCode).orElse(0));
  }

  /**
   * All children checks below are READS, not preconditions: {@code Precondition} declares no
   * set-emptiness operator (see its own "Deliberately absent" section), so "no children under this
   * parent" cannot ride into the commit the way the retrofit's path checks do. This leaves the
   * identical TOCTOU window both old impls already carry between this read and the write — {@code
   * AtomicOperationMetaStoreManager}'s own five TODOs concede the same gap for the same reason, so
   * this is parity, not a regression introduced here.
   */
  private List<PolarisBaseEntity> rawChildEntities(long catalogId, long parentId) {
    return entityStore()
        .list(
            PolarisRecordKinds.ENTITY,
            PolarisRecordKinds.ENTITY_BY_PARENT,
            List.of(catalogId, parentId),
            PageToken.readEverything(),
            PolarisBaseEntity.class)
        .items();
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
   * version bumps. Inside one commit, where every mutation applies or none do, that order is
   * unobservable — this method builds the mutations in whichever order is simplest, and the store
   * applies them as one unordered set. The divergence dissolves rather than being resolved.
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
              entityIdentity(dropped.getId()),
              null,
              first ? topLevelPreconditions : List.of()));
      first = false;
      allGrants.addAll(
          grantStore()
              .list(
                  PolarisRecordKinds.GRANT_RECORD,
                  PolarisRecordKinds.GRANT_RECORD_BY_SECURABLE,
                  List.of(dropped.getCatalogId(), dropped.getId()),
                  PageToken.readEverything(),
                  PolarisGrantRecord.class)
              .items());
      allGrants.addAll(
          grantStore()
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
                  secretsIdentity(clientId),
                  null));
        }
      }
      if (dropped.getType() == PolarisEntityType.POLICY) {
        allMappings.addAll(
            policyMappingsOn(
                PolarisRecordKinds.POLICY_MAPPING_BY_POLICY,
                dropped.getCatalogId(),
                dropped.getId()));
      } else if (PolicyMappingUtil.isValidTargetEntityType(
          dropped.getType(), dropped.getSubType())) {
        allMappings.addAll(
            policyMappingsOn(
                PolarisRecordKinds.POLICY_MAPPING_BY_TARGET,
                dropped.getCatalogId(),
                dropped.getId()));
      }
    }

    Map<RecordRef, PolarisPolicyMappingRecord> distinctMappings = new LinkedHashMap<>();
    for (PolarisPolicyMappingRecord m : allMappings) {
      distinctMappings.putIfAbsent(policyMappingIdentity(m), m);
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
      distinctGrants.putIfAbsent(grantIdentity(g), g);
    }

    // One combined counterpart set across every dropped entity, not one per entity: a counterpart
    // reached from two different grants (e.g. the same principal role usage-granted on both the
    // catalog admin role AND some unrelated role) must be bumped exactly once. Two UPDATE
    // mutations on the same identity in one commit would have the second's version precondition
    // fail against the first's already-applied bump (mutations in one commit apply in list order
    // within the same transaction), spuriously failing the whole drop.
    Set<Long> counterpartIds = new HashSet<>();
    for (PolarisGrantRecord g : distinctGrants.values()) {
      mutations.add(
          Mutation.of(PolarisRecordKinds.GRANT_RECORD, Mutation.Op.DELETE, grantIdentity(g), null));
      if (!droppedIds.contains(g.getGranteeId())) {
        counterpartIds.add(g.getGranteeId());
      }
      if (!droppedIds.contains(g.getSecurableId())) {
        counterpartIds.add(g.getSecurableId());
      }
    }
    if (!counterpartIds.isEmpty()) {
      List<RecordRef> counterpartRefs =
          counterpartIds.stream().map(DefaultDurableManager::entityIdentity).toList();
      entityStore().getMany(counterpartRefs, PolarisBaseEntity.class).stream()
          .filter(Optional::isPresent)
          .map(Optional::get)
          .forEach(counterpart -> mutations.add(bumpGrantRecordsVersion(counterpart).mutation()));
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
        entityStore().get(entityIdentity(entityToDrop.getId()), PolarisBaseEntity.class);
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
      List<PolarisBaseEntity> children = rawChildEntities(catalogId, catalogId);
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
        && !rawChildEntities(current.getCatalogId(), current.getId()).isEmpty()) {
      return new DropEntityResult(BaseResult.ReturnStatus.NAMESPACE_NOT_EMPTY, null);
    } else if (current.getType() == PolarisEntityType.POLICY
        && !cleanup
        && !policyMappingsOn(
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
        droppedEntities, droppedIds, pathExistsPreconditions(catalogPath, null), mutations);

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
              .id(entityStore().generateNewId())
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
              entityIdentity(taskEntity.getId()),
              taskEntity,
              List.of(
                  Precondition.notExists(
                      entityUniqueness(
                          taskEntity.getParentId(),
                          taskEntity.getTypeCode(),
                          taskEntity.getName())))));
    }

    OrchestrationResult result = orchestrator.commit(mutations);
    if (!result.isApplied()) {
      return mapFailedDrop(result, pathRefs(catalogPath, null));
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
    return failedOnPath(result, pathRefs)
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
        entityStore().get(entityIdentity(entityId), PolarisBaseEntity.class);
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

  /**
   * The grant records on which {@code entity} is the securable — the anchor every entity gets,
   * grantee or not. Shared by {@link #loadResolvedEntityById}, {@link #loadResolvedEntities} and
   * their {@code toResolvedPolarisEntity} helper below.
   */
  private List<PolarisGrantRecord> grantsAsSecurable(@NonNull PolarisEntityCore entity) {
    return grantsAsSecurable(entity.getCatalogId(), entity.getId());
  }

  private List<PolarisGrantRecord> grantsAsSecurable(long catalogId, long id) {
    return grantStore()
        .list(
            PolarisRecordKinds.GRANT_RECORD,
            PolarisRecordKinds.GRANT_RECORD_BY_SECURABLE,
            List.of(catalogId, id),
            PageToken.readEverything(),
            PolarisGrantRecord.class)
        .items();
  }

  /** The grant records where {@code entity} is the grantee — only meaningful when it is one. */
  private List<PolarisGrantRecord> grantsAsGrantee(@NonNull PolarisEntityCore entity) {
    return grantsAsGrantee(entity.getCatalogId(), entity.getId());
  }

  private List<PolarisGrantRecord> grantsAsGrantee(long catalogId, long id) {
    return grantStore()
        .list(
            PolarisRecordKinds.GRANT_RECORD,
            PolarisRecordKinds.GRANT_RECORD_BY_GRANTEE,
            List.of(catalogId, id),
            PageToken.readEverything(),
            PolarisGrantRecord.class)
        .items();
  }

  /**
   * Ported from both old impls' {@code loadResolvedEntityById}, identical apart from the {@code
   * InCurrentTxn} suffix and the read-transaction wrapper (confirmed by reading both). {@code
   * entityType} filters the lookup the same way {@link #loadEntity} does — this reuses it rather
   * than re-deriving the type check, since both old impls resolve through the SAME {@code
   * lookupEntity}/{@code lookupEntityInCurrentTxn} call {@link #loadEntity} already ports. No
   * counterpart-entity fetch here: both old impls return the raw {@link PolarisGrantRecord} list
   * unenriched, which is why a grant referencing a dropped counterpart is already absent — the
   * counterpart's own drop deleted the grant record itself (increment 5's {@code
   * collectDropMutations}), not a filter this method applies.
   */
  @Override
  public @NonNull ResolvedEntityResult loadResolvedEntityById(
      @NonNull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType) {
    EntityResult found = loadEntity(callCtx, entityCatalogId, entityId, entityType);
    if (!found.isSuccess()) {
      return new ResolvedEntityResult(found.getReturnStatus(), found.getExtraInformation());
    }
    PolarisBaseEntity entity = found.getEntity();

    List<PolarisGrantRecord> grantRecords;
    if (entity.getType().isGrantee()) {
      grantRecords = new ArrayList<>(grantsAsGrantee(entity));
      grantRecords.addAll(grantsAsSecurable(entity));
    } else {
      grantRecords = grantsAsSecurable(entity);
    }
    return new ResolvedEntityResult(entity, entity.getGrantRecordsVersion(), grantRecords);
  }

  /**
   * A single position's resolved view, or {@code null} when the entity is absent or the wrong type
   * — ported from both old impls' shared {@code toResolvedPolarisEntity}/{@code
   * getResolvedEntitiesResult}. Unlike {@link #loadResolvedEntityById}'s combined list, {@code
   * ResolvedPolarisEntity}'s constructor here takes the grantee/securable lists pre-split (the
   * OTHER constructor, the one with a {@code PolarisDiagnostics} parameter, is what does the
   * splitting from a combined list — neither old impl uses that one here).
   */
  private @Nullable ResolvedPolarisEntity toResolvedPolarisEntity(
      @Nullable PolarisBaseEntity entity) {
    if (entity == null) {
      return null;
    }
    List<PolarisGrantRecord> asSecurable = grantsAsSecurable(entity);
    List<PolarisGrantRecord> asGrantee =
        entity.getType().isGrantee() ? grantsAsGrantee(entity) : List.of();
    return new ResolvedPolarisEntity(PolarisEntity.of(entity), asGrantee, asSecurable);
  }

  /**
   * Ported from both old impls' shared {@code getResolvedEntitiesResult}: batch-fetch by identity
   * (positional, per {@link DurableRecordStore#getMany}'s own contract), filter each position by
   * {@code entityType}, and resolve grants for whichever positions survive. A missing or wrong-type
   * position becomes a {@code null} entry in the returned list — the call itself still succeeds,
   * matching {@code testLoadResolvedEntitiesById}'s own assertion that a batch mixing real, absent
   * and wrong-type ids returns {@code SUCCESS} with nulls at the losing positions.
   */
  @Override
  public @NonNull ResolvedEntitiesResult loadResolvedEntities(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityType entityType,
      @NonNull List<PolarisEntityId> entityIds) {
    List<RecordRef> refs = entityIds.stream().map(id -> entityIdentity(id.id())).toList();
    List<Optional<PolarisBaseEntity>> found = entityStore().getMany(refs, PolarisBaseEntity.class);

    List<ResolvedPolarisEntity> resolved = new ArrayList<>(entityIds.size());
    for (Optional<PolarisBaseEntity> maybeEntity : found) {
      PolarisBaseEntity entity =
          maybeEntity.filter(e -> e.getTypeCode() == entityType.getCode()).orElse(null);
      resolved.add(toResolvedPolarisEntity(entity));
    }
    return new ResolvedEntitiesResult(resolved);
  }

  // ------------------------------------------------------- DurableManager (ticket 92 surfaces)

  /**
   * Today's observable behaviour, ported per Issue 68's verified shape (byte-equivalent in both old
   * impls): a WARN, a realm wipe, a WARN, an unconditional {@code SUCCESS} — no coded failure path;
   * an underlying error propagates as an unchecked exception, exactly as the old impls let their
   * store exceptions through. The log messages are the old impls' own, verbatim. Whether this
   * manager-level method should exist at all stays Issue 68's ready-for-human question;
   * implementing parity does not prejudge it.
   *
   * <p>The old wipe is ONE old-primitives call ({@code deleteAll}), a realm-scoped per-table bulk
   * delete. The new SPI is deliberately closed at one write and four reads with no realm-wipe
   * operation, so the wipe is COMPOSED: walk every entity from the root anchor through {@code
   * by-parent}, collect each entity's grant records (both directions), policy mappings (both
   * directions where they apply) and — for principals — the secrets row named by the principal's
   * client id, then DELETE everything in chunked commits through the orchestrator. The scope
   * matches the old wipe's actual table list, read from {@code JdbcDurablePrimitivesImpl#deleteAll}
   * before building this: ENTITIES, GRANT_RECORDS, PRINCIPAL_AUTHENTICATION_DATA,
   * POLICY_MAPPING_RECORD — and NOT the events table, whose rows carry no realm column, so the old
   * realm-scoped wipe never touched them either (the proving case pins their survival).
   *
   * <p>Disclosed narrowings vs the old single-call wipe. <b>Crash window:</b> old JDBC wipes in one
   * transaction; this walk is several commits, so a crash mid-purge leaves a partial wipe. What
   * makes re-running purge actually complete it is the DELETE ORDER, not merely the deletes being
   * unconditioned: mutations run leaf-ward — secrets, then mappings, then grants (each reachable
   * only through an entity anchor, so their anchors must still exist when a re-run looks), then
   * entities CHILDREN-BEFORE-PARENTS (reverse breadth-first order). Any crash prefix therefore
   * leaves every surviving record still reachable by a fresh walk: no parent dies before its
   * subtree, no anchor entity dies before the records anchored on it. (This ticket's refute pass
   * caught the original entity-first order manufacturing permanently unreachable subtrees on a
   * mid-purge crash while the javadoc claimed idempotency — the ordering above is the fix, not a
   * restatement.) <b>Reachability:</b> a PRE-EXISTING crash-orphaned secrets row with no surviving
   * principal entity is unreachable (the by-principal/enumeration path is the data model's own
   * recorded gap, §4.4 / open question 2), likewise a pre-existing orphaned mapping row both of
   * whose endpoints are gone, and likewise an entity subtree whose parent chain was already broken
   * before purge began; the old whole-table deletes covered such orphans, a walk cannot.
   */
  @Override
  public @NonNull BaseResult purge(@NonNull PolarisCallContext callCtx) {
    LOGGER.warn("Deleting all metadata in the metastore...");

    List<PolarisBaseEntity> entities = walkAllEntities();

    Map<RecordRef, PolarisGrantRecord> grants = new LinkedHashMap<>();
    Map<RecordRef, PolarisPolicyMappingRecord> mappings = new LinkedHashMap<>();
    for (PolarisBaseEntity entity : entities) {
      for (PolarisGrantRecord g : grantsAsSecurable(entity)) {
        grants.putIfAbsent(grantIdentity(g), g);
      }
      for (PolarisGrantRecord g : grantsAsGrantee(entity)) {
        grants.putIfAbsent(grantIdentity(g), g);
      }
      for (PolarisPolicyMappingRecord m :
          policyMappingsOn(
              PolarisRecordKinds.POLICY_MAPPING_BY_TARGET, entity.getCatalogId(), entity.getId())) {
        mappings.putIfAbsent(policyMappingIdentity(m), m);
      }
      if (entity.getType() == PolarisEntityType.POLICY) {
        for (PolarisPolicyMappingRecord m :
            policyMappingsOn(
                PolarisRecordKinds.POLICY_MAPPING_BY_POLICY,
                entity.getCatalogId(),
                entity.getId())) {
          mappings.putIfAbsent(policyMappingIdentity(m), m);
        }
      }
    }

    // Leaf-ward delete order — the invariant the crash-window disclosure above rests on.
    List<Mutation> mutations = new ArrayList<>();
    for (PolarisBaseEntity entity : entities) {
      if (entity.getType() == PolarisEntityType.PRINCIPAL) {
        String clientId = PrincipalEntity.of(entity).getClientId();
        if (clientId != null && !clientId.isEmpty()) {
          mutations.add(
              Mutation.of(
                  PolarisRecordKinds.PRINCIPAL_SECRETS,
                  Mutation.Op.DELETE,
                  secretsIdentity(clientId),
                  null));
        }
      }
    }
    for (RecordRef mappingRef : mappings.keySet()) {
      mutations.add(
          Mutation.of(PolarisRecordKinds.POLICY_MAPPING, Mutation.Op.DELETE, mappingRef, null));
    }
    for (RecordRef grantRef : grants.keySet()) {
      mutations.add(
          Mutation.of(PolarisRecordKinds.GRANT_RECORD, Mutation.Op.DELETE, grantRef, null));
    }
    for (int i = entities.size() - 1; i >= 0; i--) {
      // Reverse breadth-first = children before parents: a parent's anchor survives until its
      // whole subtree's deletes have committed.
      mutations.add(
          Mutation.of(
              PolarisRecordKinds.ENTITY,
              Mutation.Op.DELETE,
              entityIdentity(entities.get(i).getId()),
              null));
    }

    int cap = primitives.maxItemsPerCommit();
    for (int from = 0; from < mutations.size(); from += cap) {
      OrchestrationResult result =
          orchestrator.commit(mutations.subList(from, Math.min(from + cap, mutations.size())));
      if (!result.isApplied()) {
        // No preconditions ride these deletes, so a non-applied outcome is a store/deployment
        // problem, not a race; failure-is-loud matches the old impls' uncaught store exceptions.
        throw new IllegalStateException(
            "purge commit not applied: "
                + result
                    .groupFailure()
                    .flatMap(CommitResult::failure)
                    .map(Enum::toString)
                    .orElse(result.outcome().toString()));
      }
    }

    LOGGER.warn("Finished deleting all metadata in the metastore");
    return new BaseResult(BaseResult.ReturnStatus.SUCCESS);
  }

  /**
   * Every entity in the realm, breadth-first from the root anchor {@code (null-catalog, root)}. A
   * CATALOG's children anchor on {@code (catalog, catalog)}; every other entity's children anchor
   * on {@code (its catalog, its id)}. The root container is self-parented (id 0 under parent 0),
   * which is why anchors and ids are both dedup-guarded.
   */
  private List<PolarisBaseEntity> walkAllEntities() {
    List<PolarisBaseEntity> out = new ArrayList<>();
    Set<Long> seenIds = new HashSet<>();
    Set<List<Long>> seenAnchors = new HashSet<>();
    Deque<long[]> anchors = new ArrayDeque<>();
    anchors.add(
        new long[] {PolarisEntityConstants.getNullId(), PolarisEntityConstants.getRootEntityId()});
    seenAnchors.add(
        List.of(PolarisEntityConstants.getNullId(), PolarisEntityConstants.getRootEntityId()));
    while (!anchors.isEmpty()) {
      long[] anchor = anchors.poll();
      for (PolarisBaseEntity entity : rawChildEntities(anchor[0], anchor[1])) {
        if (!seenIds.add(entity.getId())) {
          continue;
        }
        out.add(entity);
        long childCatalog =
            entity.getTypeCode() == PolarisEntityType.CATALOG.getCode()
                ? entity.getId()
                : entity.getCatalogId();
        if (seenAnchors.add(List.of(childCatalog, entity.getId()))) {
          anchors.add(new long[] {childCatalog, entity.getId()});
        }
      }
    }
    return out;
  }

  /**
   * Ported from both old impls' {@code loadTasks}, whose availability predicate is verbatim
   * identical in the two: a TASK under root is leasable when its parsed state is null (never
   * attempted, or unparseable — {@code parseTaskState} logs and returns null on bad JSON), its
   * executor is null, or its last attempt is older than {@code POLARIS_TASK_TIMEOUT_MILLIS} (realm
   * config, default 300s) against the INJECTED clock. Taking a lease stamps {@code
   * lastAttemptExecutorId}/{@code lastAttemptStartTime}/{@code attemptCount} and persists through
   * {@link #updateEntityPropertiesIfNotChanged}'s version CAS, exactly as both old impls do.
   *
   * <p>The read is {@link #listChildEntities} over root (the same in-memory entity-type narrowing
   * that method already discloses), with the availability predicate evaluated HERE and the page
   * limit applied AFTER it — matching the old primitives' predicate-then-limit order (the fixture's
   * second limit-5 call must return the NEXT five unleased tasks, not an empty page of
   * already-leased ones). The old interface pushed this predicate INTO the store as a callback; the
   * new SPI's own javadoc records task leasing as a missing operation rather than a filter to
   * relocate, and reshaping it is the read-side record's noted follow-up, not this ticket's — so
   * the whole candidate set crosses to the manager and is filtered in memory, the disclosed interim
   * cost. Part of the same interim shape: the caller's continuation CURSOR, if its page token ever
   * carried one, is not honored — only the page SIZE is read (the old impls thread the whole token
   * into the store scan). No caller in the tree passes a continuation-bearing token, and loadTasks
   * never returns one to chain from (old and new both return a token-less {@code Page.fromItems}),
   * so the gap has no live trigger; named by this ticket's refute pass, owned by the same read-side
   * follow-up.
   *
   * <p><b>Disclosed old-impl divergence, Atomic's form matched:</b> individual failed leases are
   * skipped, and only a batch where EVERY attempted lease failed throws {@link
   * RetryOnConcurrencyException} ({@code AtomicOperationMetaStoreManager}'s partial-success form,
   * which one-commit-per-lease natively is). {@code TransactionalMetaStoreManagerImpl} instead
   * rolls its whole batch back and throws on the FIRST failed lease; that all-or-nothing form has
   * no counterpart here because each lease is its own commit. The fixture accepts either (its
   * parallel executors catch the exception and retry; exactly-once claiming rests on the CAS, not
   * on the batch shape).
   */
  @Override
  public @NonNull EntitiesResult loadTasks(
      @NonNull PolarisCallContext callCtx, String executorId, PageToken pageToken) {
    long taskAgeTimeout =
        callCtx.getRealmConfig().getConfig(FeatureConfiguration.POLARIS_TASK_TIMEOUT_MILLIS);
    List<PolarisBaseEntity> availableTasks =
        listChildEntities(
                null,
                PolarisEntityType.TASK,
                PolarisEntitySubType.ANY_SUBTYPE,
                PageToken.readEverything())
            .stream()
            .filter(
                entity -> {
                  PolarisObjectMapperUtil.TaskExecutionState taskState =
                      PolarisObjectMapperUtil.parseTaskState(entity);
                  return taskState == null
                      || taskState.executor == null
                      || clock.millis() - taskState.lastAttemptStartTime > taskAgeTimeout;
                })
            .limit(
                pageToken.pageSize().isPresent() ? pageToken.pageSize().getAsInt() : Long.MAX_VALUE)
            .toList();

    int failedLeaseCount = 0;
    List<PolarisBaseEntity> loadedTasks = new ArrayList<>(availableTasks.size());
    for (PolarisBaseEntity task : availableTasks) {
      PolarisBaseEntity.Builder updatedTaskBuilder = new PolarisBaseEntity.Builder(task);
      Map<String, String> properties = task.getPropertiesAsMap();
      properties.put(PolarisTaskConstants.LAST_ATTEMPT_EXECUTOR_ID, executorId);
      properties.put(PolarisTaskConstants.LAST_ATTEMPT_START_TIME, String.valueOf(clock.millis()));
      properties.put(
          PolarisTaskConstants.ATTEMPT_COUNT,
          String.valueOf(
              Integer.parseInt(properties.getOrDefault(PolarisTaskConstants.ATTEMPT_COUNT, "0"))
                  + 1));
      updatedTaskBuilder.propertiesAsMap(properties);
      EntityResult result =
          updateEntityPropertiesIfNotChanged(callCtx, null, updatedTaskBuilder.build());
      if (result.getReturnStatus() == BaseResult.ReturnStatus.SUCCESS) {
        loadedTasks.add(result.getEntity());
      } else {
        failedLeaseCount++;
      }
    }
    if (loadedTasks.isEmpty() && failedLeaseCount > 0) {
      throw new RetryOnConcurrencyException(
          "Failed to lease any of %s tasks due to concurrent leases", failedLeaseCount);
    }
    return EntitiesResult.fromPage(Page.fromItems(loadedTasks));
  }

  /**
   * Ported from both old impls' {@code loadResolvedEntityByName}, including the root-container
   * backfill special case both carry verbatim (a holdover from before bootstrap created the root
   * container; the old code's own TODO doubts it is still reachable, and it is ported rather than
   * judged). The name lookup goes through the same uniqueness key {@link #readEntityByName} uses;
   * the STORE fetch carries no catalog component ({@link #entityUniqueness}'s disclosure), and the
   * old lookup's {@code catalog_id} filter — both old stores apply it in the physical by-name
   * lookup, so an untruthful {@code entityCatalogId} is {@code ENTITY_NOT_FOUND} there — is applied
   * HERE on the fetched row, the same treatment {@link #loadEntity} gives its identity lookups.
   * (This ticket's refute pass caught the first draft silently returning SUCCESS for that case and
   * its javadoc understating the divergence as a grant-anchor nuance; the check below restores
   * exact old behaviour, and makes the grant anchors — the entity's own {@code (catalogId, id)} —
   * provably equal to the old code's argument-anchored loads.)
   */
  @Override
  public @NonNull ResolvedEntityResult loadResolvedEntityByName(
      @NonNull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @NonNull PolarisEntityType entityType,
      @NonNull String entityName) {
    Optional<PolarisBaseEntity> found =
        entityStore()
            .get(
                entityUniqueness(parentId, entityType.getCode(), entityName),
                PolarisBaseEntity.class);
    if (found.isPresent() && found.get().getCatalogId() != entityCatalogId) {
      found = Optional.empty();
    }

    ResolvedEntityResult result;
    if (found.isEmpty()) {
      result = new ResolvedEntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    } else {
      PolarisBaseEntity entity = found.get();
      List<PolarisGrantRecord> grantRecords;
      if (entity.getType().isGrantee()) {
        grantRecords = new ArrayList<>(grantsAsGrantee(entity));
        grantRecords.addAll(grantsAsSecurable(entity));
      } else {
        grantRecords = grantsAsSecurable(entity);
      }
      result = new ResolvedEntityResult(entity, entity.getGrantRecordsVersion(), grantRecords);
    }

    if (PolarisEntityConstants.getRootContainerName().equals(entityName)
        && entityType == PolarisEntityType.ROOT
        && !result.isSuccess()) {
      // Backfill rootContainer if needed, ported verbatim from both old impls (Atomic quoted):
      // create the root container idempotently, grant SERVICE_MANAGE_ACCESS to the service admin
      // role when it exists, then redo the lookup.
      PolarisBaseEntity rootContainer =
          new PolarisBaseEntity(
              PolarisEntityConstants.getNullId(),
              PolarisEntityConstants.getRootEntityId(),
              PolarisEntityType.ROOT,
              PolarisEntitySubType.NULL_SUBTYPE,
              PolarisEntityConstants.getRootEntityId(),
              PolarisEntityConstants.getRootContainerName());
      EntityResult backfillResult = this.createEntityIfNotExists(callCtx, null, rootContainer);
      if (backfillResult.isSuccess()) {
        PolarisBaseEntity serviceAdminRole =
            entityStore()
                .get(
                    entityUniqueness(
                        PolarisEntityConstants.getRootEntityId(),
                        PolarisEntityType.PRINCIPAL_ROLE.getCode(),
                        PolarisEntityConstants.getNameOfPrincipalServiceAdminRole()),
                    PolarisBaseEntity.class)
                .orElse(null);
        if (serviceAdminRole != null) {
          this.persistNewGrantRecord(
              rootContainer, serviceAdminRole, PolarisPrivilege.SERVICE_MANAGE_ACCESS);
        }
      }
      result =
          this.loadResolvedEntityByName(callCtx, entityCatalogId, parentId, entityType, entityName);
    }
    return result;
  }

  /**
   * Ported from {@code AtomicOperationMetaStoreManager#refreshResolvedEntity}, with the old shape's
   * TWO reads collapsed into ONE full fetch, disclosed rather than silent:
   *
   * <p>The old shape probes {@code lookupEntityVersions} (a narrow, catalog-filtered projection)
   * and reloads the full row only when the entity version moved. The new {@code versionsOf} keys on
   * {@code (realm, id)} alone with no catalog dimension, so the old probe's catalog filter — which
   * {@code testEntityCache}'s wrong-catalog refresh observes — cannot be expressed through the
   * narrow read; a full identity fetch here carries the catalog column and IS filterable. The cost
   * is a full row where the old no-change path shipped four version columns; the read-side record's
   * follow-up owns whether versionsOf should carry the catalog dimension (kin of the by-parent
   * type-code declaration gap).
   *
   * <p>The old filter split is preserved exactly: the probe filters by catalog only (a wrong-TYPE
   * refresh whose versions are unchanged still reports success — the old versions lookup takes no
   * type code), while the reload branch additionally filters by type, exactly as {@code
   * lookupEntity} does. One read also supersedes the two-read race Atomic's own comment corrects
   * for — the returned {@code (entity, grantRecordsVersion)} pair comes from one snapshot, the
   * internally-consistent outcome that race-corrected form exists to approximate ({@code
   * TransactionalMetaStoreManagerImpl} reports the earlier snapshot instead; Atomic's form is this
   * class's disclosed convention for the resolved-entity reads). Version short-circuits are the
   * contract the cache relies on: an unchanged half comes back {@code null} inside a SUCCESS
   * result, meaning "keep your copy".
   */
  @Override
  public @NonNull ResolvedEntityResult refreshResolvedEntity(
      @NonNull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      @NonNull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId) {
    Optional<PolarisBaseEntity> found =
        entityStore().get(entityIdentity(entityId), PolarisBaseEntity.class);
    if (found.isEmpty() || found.get().getCatalogId() != entityCatalogId) {
      // purged, or the old probe's catalog filter says this is not the row the caller cached
      return new ResolvedEntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }
    PolarisBaseEntity current = found.get();

    final PolarisBaseEntity entity;
    if (entityVersion != current.getEntityVersion()) {
      // the reload branch is where the old shape's TYPE filter lives
      if (current.getTypeCode() != entityType.getCode()) {
        return new ResolvedEntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
      }
      entity = current;
    } else {
      // entity has not changed, no need to reload it
      entity = null;
    }

    int reportedGrantRecordsVersion = current.getGrantRecordsVersion();

    final List<PolarisGrantRecord> grantRecords;
    if (reportedGrantRecordsVersion != entityGrantRecordsVersion) {
      if (entityType.isGrantee()) {
        grantRecords = new ArrayList<>(grantsAsGrantee(entityCatalogId, entityId));
        grantRecords.addAll(grantsAsSecurable(entityCatalogId, entityId));
      } else {
        grantRecords = grantsAsSecurable(entityCatalogId, entityId);
      }
    } else {
      grantRecords = null;
    }

    return new ResolvedEntityResult(entity, reportedGrantRecordsVersion, grantRecords);
  }

  // ---------------------------------------------------------- GrantDurableManager (ticket 91)

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
    return primitives;
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
   * A grant-record {@code CREATE} mutation, declaring {@link Precondition#none()} per {@link
   * Mutation.Op#CREATE}'s contract for a kind whose identity and uniqueness are the same tuple. See
   * {@link #persistNewGrantRecord}'s javadoc for why that contract is not yet honored by either
   * shipped store's actual {@code CREATE} handling, and why this method still declares it.
   */
  private static Mutation createGrantMutation(@NonNull PolarisGrantRecord grantRecord) {
    return Mutation.of(
        PolarisRecordKinds.GRANT_RECORD,
        Mutation.Op.CREATE,
        grantIdentity(grantRecord),
        grantRecord,
        List.of(Precondition.none()));
  }

  /**
   * One entity's {@code grantRecordsVersion} bump: the mutation to commit, and the resulting entity
   * state. Returning the updated state (rather than just the {@link Mutation}) lets a caller
   * building several grants against the SAME entity within one mutation list — {@link
   * #createCatalog}'s catalog and admin role, each touched by more than one grant — thread the
   * running version forward between them instead of re-reading the store in between.
   */
  private record VersionBump(Mutation mutation, PolarisBaseEntity updated) {}

  /**
   * Gated by both halves of the two-column CAS the relational store's {@code entity_version}/
   * {@code grant_records_version} comparison performs: {@code entityVersion} is asserted unchanged,
   * never bumped here — only {@code grantRecordsVersion} moves, matching both old impls' {@code
   * entity.withGrantRecordsVersion(entity.getGrantRecordsVersion() + 1)}.
   */
  private VersionBump bumpGrantRecordsVersion(@NonNull PolarisBaseEntity entity) {
    RecordRef ref = entityIdentity(entity.getId());
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
            createGrantMutation(grantRecord),
            bumpGrantRecordsVersion(granteeEntity).mutation(),
            bumpGrantRecordsVersion(securableEntity).mutation());

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
            bumpGrantRecordsVersion(granteeEntity).mutation(),
            bumpGrantRecordsVersion(securableEntity).mutation());

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

  // ---------------------------------------------------------- SecretsDurableManager (ticket 91)

  /**
   * Maps a non-applied secrets-mutation {@link OrchestrationResult}. No old-model precedent, same
   * reasoning as {@link #mapFailedGrantMutation}: the old primitives calls this replaces are raw
   * read-modify-writes with no commit-outcome type to map from.
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
   * The {@code ENTITY} UPDATE that persists a changed {@code internalPropertiesAsMap}, bumping only
   * {@code entityVersion} — ported from {@code AtomicOperationMetaStoreManager}'s / {@code
   * TransactionalMetaStoreManagerImpl}'s {@code rotatePrincipalSecrets}, which bump entityVersion
   * but never grantRecordsVersion for this write, so only one half of the two-column CAS {@link
   * #bumpGrantRecordsVersion} uses applies here.
   */
  private Mutation internalPropertiesMutation(
      @NonNull PolarisBaseEntity current, @NonNull Map<String, String> internalProperties) {
    RecordRef ref = entityIdentity(current.getId());
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

  @Override
  public @NonNull PrincipalSecretsResult loadPrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String clientId) {
    return secretsStore()
        .get(secretsIdentity(clientId), PolarisPrincipalSecrets.class)
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
        secretsStore().get(secretsIdentity(clientId), PolarisPrincipalSecrets.class).orElse(null);
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
            secretsIdentity(clientId),
            updated,
            List.of(Precondition.none())));
    if (reset && !flagPresent) {
      internalProps.put(
          PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE, "true");
      mutations.add(internalPropertiesMutation(principal, internalProps));
    } else if (flagPresent) {
      internalProps.remove(PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE);
      mutations.add(internalPropertiesMutation(principal, internalProps));
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

    RecordRef ref = secretsIdentity(resolvedClientId);
    if (secretsStore().get(ref, PolarisPrincipalSecrets.class).isPresent()) {
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
   * as {@link #revokeGrantRecord}'s DELETE: existence was just confirmed by this method's own read,
   * and no precondition closes the (equally present in the old model) race between that read and
   * the write.
   */
  @Override
  public void deletePrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String clientId, long principalId) {
    RecordRef ref = secretsIdentity(clientId);
    PolarisPrincipalSecrets secrets =
        secretsStore().get(ref, PolarisPrincipalSecrets.class).orElse(null);
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

  // ------------------------------------------------- PolicyDurableManager (ticket 92)

  /**
   * Policy-mapping identity ref: {@code (target-catalog, target, policy-type, policy-catalog,
   * policy)} — the whole tuple is the key ({@code parameters} is not part of it), so identity and
   * uniqueness coincide, verified against both shipped stores' bindings ({@code
   * TreeMapDurableRecordStore#policyKey}, {@code JdbcDurableRecordStore}'s identity column list —
   * both state exactly this order).
   */
  private static RecordRef policyMappingIdentity(@NonNull PolarisPolicyMappingRecord record) {
    return RecordRef.byIdentity(
        PolarisRecordKinds.POLICY_MAPPING,
        List.of(
            record.getTargetCatalogId(),
            record.getTargetId(),
            record.getPolicyTypeCode(),
            record.getPolicyCatalogId(),
            record.getPolicyId()));
  }

  private DurableRecordStore policyMappingStore() {
    return primitives;
  }

  /** Every mapping record on one anchor of {@code path} — the policy-side twin of loadGrants. */
  private List<PolarisPolicyMappingRecord> policyMappingsOn(
      @NonNull LookupPath path, long anchorCatalogId, long anchorId) {
    return policyMappingStore()
        .list(
            PolarisRecordKinds.POLICY_MAPPING,
            path,
            List.of(anchorCatalogId, anchorId),
            PageToken.readEverything(),
            PolarisPolicyMappingRecord.class)
        .items();
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
            .mapToObj(DefaultDurableManager::entityIdentity)
            .toList();
    return entityStore().getMany(refs, PolarisBaseEntity.class).stream()
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
   * retrofit convention every OTHER write taking a catalogPath already uses (see {@link
   * #catalogIdOf}): {@link #pathExistsPreconditions} over BOTH paths rides the commit, plus an
   * {@code EXISTS} precondition on the target and the policy identities — the same happens-before
   * guarantee, here closing the leak of a mapping row written under a concurrently-dropped target
   * or policy (the unconditional drop-path cleanup in {@link #collectDropMutations} deletes
   * mappings when an endpoint drops; a mapping committed AFTER that cleanup read would survive it).
   * Failure mapping: a failed path precondition is {@code CATALOG_PATH_CANNOT_BE_RESOLVED}
   * (matching the other retrofited writes), a failed endpoint precondition is {@code
   * ENTITY_CANNOT_BE_RESOLVED} (Transactional's status for exactly this situation).
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
    RecordRef identity = policyMappingIdentity(mappingRecord);

    boolean replaceExisting = false;
    if (policyType.isInheritable()) {
      List<PolarisPolicyMappingRecord> existingOfType =
          policyMappingsOn(
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
      replaceExisting =
          policyMappingStore().get(identity, PolarisPolicyMappingRecord.class).isPresent();
    }

    List<Precondition> preconditions =
        new ArrayList<>(pathExistsPreconditions(targetCatalogPath, policyCatalogPath));
    preconditions.add(Precondition.exists(entityIdentity(target.getId())));
    preconditions.add(Precondition.exists(entityIdentity(policy.getId())));
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
          pathRefs(targetCatalogPath, policyCatalogPath),
          Set.of(entityIdentity(target.getId()), entityIdentity(policy.getId())),
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
    RecordRef identity = policyMappingIdentity(probe);
    PolarisPolicyMappingRecord mappingRecord =
        policyMappingStore().get(identity, PolarisPolicyMappingRecord.class).orElse(null);
    if (mappingRecord == null) {
      return new PolicyAttachmentResult(BaseResult.ReturnStatus.POLICY_MAPPING_NOT_FOUND, null);
    }

    List<Precondition> preconditions =
        new ArrayList<>(pathExistsPreconditions(catalogPath, policyCatalogPath));
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
          pathRefs(catalogPath, policyCatalogPath),
          Set.of(),
          new PolicyAttachmentResult(BaseResult.ReturnStatus.POLICY_MAPPING_NOT_FOUND, null));
    }
    return new PolicyAttachmentResult(mappingRecord);
  }

  /**
   * Maps a non-applied policy-mapping {@link OrchestrationResult}. No old-model precedent for the
   * same reason as {@link #mapFailedCreate}; the per-caller {@code onOwnIdentity} result carries
   * the one mapping that differs between attach's two branches and detach.
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
    if (failedOnPath(result, pathRefs)) {
      return new PolicyAttachmentResult(
          BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
    }
    if (!endpointRefs.isEmpty() && failedOnPath(result, endpointRefs)) {
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
        policyMappingsOn(
            PolarisRecordKinds.POLICY_MAPPING_BY_TARGET, target.getCatalogId(), target.getId());
    return new LoadPolicyMappingsResult(mappingRecords, policiesFromMappingRecords(mappingRecords));
  }

  /**
   * Ported from both old impls' {@code loadPoliciesOnEntityByType}. The type narrowing happens in
   * this method, not at the store: {@code by-target}'s declared anchors are the target address
   * alone (data model 4.3), with no policy-type anchor — the same declaration gap as {@link
   * #listChildEntities}'s entity-type narrowing, and the same disclosure: the store evaluates
   * everything it CAN evaluate, only the undeclared dimension falls through to the manager (old
   * JDBC pushes the type into its WHERE clause through the old interface's dedicated per-type
   * method, which the new declared-path read side deliberately does not carry).
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
        policyMappingsOn(
                PolarisRecordKinds.POLICY_MAPPING_BY_TARGET, target.getCatalogId(), target.getId())
            .stream()
            .filter(r -> r.getPolicyTypeCode() == policyType.getCode())
            .toList();
    return new LoadPolicyMappingsResult(mappingRecords, policiesFromMappingRecords(mappingRecords));
  }

  // ------------------------------------------------------- EventDurableManager (ticket 92)

  /** {@code EVENT}'s identity ref: {@code (event-id)} — both stores' bindings key on it alone. */
  private static RecordRef eventIdentity(@NonNull EventEntity event) {
    return RecordRef.byIdentity(PolarisRecordKinds.EVENT, List.of(event.getId()));
  }

  /**
   * Overridden rather than left as the interface default: the default reaches {@link
   * PolarisCallContext#getMetaStore()}, the old primitives handle this class must never touch.
   *
   * <p>Neither old manager overrides this — the interface default is one unconditional {@code
   * ms.writeEvents(events)} with no manager-level catch, so the manager layer adds no policy and
   * the store decides: old JDBC batch-inserts, old TreeMap throws {@code
   * UnsupportedOperationException} (events are optional — data model 4.5; the one production
   * caller, the in-memory buffer listener, retries then logs and drops). Same contract here:
   * append-only {@code CREATE} mutations through the orchestrator, no catch — a store that does not
   * serve the events kind rejects loudly (the routing implementation names the kind), which IS the
   * documented refusal; on this branch both new-model stores serve it, so both fixture bindings are
   * functional where the old TreeMap pairing threw.
   *
   * <p>Two disclosed shape notes with no old-status vocabulary to map onto (the method is void): a
   * batch larger than {@link DurableRecordStore#maxItemsPerCommit} is CHUNKED into consecutive
   * commits — events are independent append-only rows with no cross-event atomicity promise, so
   * S12's no-silent-splitting rule for promised-atomic batches does not bite, and the only
   * difference from old JDBC's single INSERT transaction is the crash window between chunks; and a
   * non-applied commit (e.g. a duplicate event id — old JDBC propagates the raw uniqueness
   * violation there) surfaces as an unchecked exception naming the failure, preserving
   * failure-is-loud.
   */
  @Override
  public void writeEvents(
      @NonNull PolarisCallContext callCtx, @NonNull List<EventEntity> polarisEvents) {
    int cap = primitives.maxItemsPerCommit();
    for (int from = 0; from < polarisEvents.size(); from += cap) {
      List<EventEntity> chunk =
          polarisEvents.subList(from, Math.min(from + cap, polarisEvents.size()));
      List<Mutation> mutations =
          chunk.stream()
              .map(
                  event ->
                      Mutation.of(
                          PolarisRecordKinds.EVENT,
                          Mutation.Op.CREATE,
                          eventIdentity(event),
                          event,
                          List.of(Precondition.none())))
              .toList();
      OrchestrationResult result = orchestrator.commit(mutations);
      if (!result.isApplied()) {
        throw new IllegalStateException(
            "writeEvents commit not applied: "
                + result
                    .groupFailure()
                    .flatMap(CommitResult::failure)
                    .map(Enum::toString)
                    .orElse(result.outcome().toString()));
      }
    }
  }

  // The previous model's manager also satisfies the per-domain contracts. Java requires an explicit
  // choice where a default method is inherited from two unrelated interfaces; keep the existing
  // one.
  @Override
  public @NonNull List<PolarisBaseEntity> listFullEntitiesAll(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType) {
    return DurableManager.super.listFullEntitiesAll(
        callCtx, catalogPath, entityType, entitySubType);
  }

  @Override
  public <T extends PolarisEntity & LocationBasedEntity>
      Optional<Optional<String>> hasOverlappingSiblings(
          @NonNull PolarisCallContext callContext, T entity) {
    return DurableManager.super.hasOverlappingSiblings(callContext, entity);
  }

  @Override
  public boolean requiresEntityReload() {
    return DurableManager.super.requiresEntityReload();
  }

  @Override
  public Optional<PrincipalEntity> findRootPrincipal(PolarisCallContext polarisCallContext) {
    return DurableManager.super.findRootPrincipal(polarisCallContext);
  }

  @Override
  public Optional<PrincipalEntity> findPrincipalByName(
      PolarisCallContext polarisCallContext, String principalName) {
    return DurableManager.super.findPrincipalByName(polarisCallContext, principalName);
  }

  @Override
  public Optional<PrincipalEntity> findPrincipalById(
      PolarisCallContext polarisCallContext, long principalId) {
    return DurableManager.super.findPrincipalById(polarisCallContext, principalId);
  }

  @Override
  public Optional<PrincipalRoleEntity> findPrincipalRoleByName(
      PolarisCallContext polarisCallContext, String principalRoleName) {
    return DurableManager.super.findPrincipalRoleByName(polarisCallContext, principalRoleName);
  }
}
