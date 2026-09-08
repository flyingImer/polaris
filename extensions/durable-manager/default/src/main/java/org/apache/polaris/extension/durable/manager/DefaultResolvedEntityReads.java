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
import java.util.Optional;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.ChangeTrackingResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;
import org.apache.polaris.core.persistence.resolver.ResolvedEntityReads;
import org.apache.polaris.spi.durable.CatalogDurableManager;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.GrantDurableManager;
import org.apache.polaris.spi.durable.RecordRef;
import org.apache.polaris.spi.durable.RecordVersions;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Transitional home of the resolved-entity and change-tracking reads on the new persistence stack.
 * The read bodies are unchanged from the single manager they came from: they read the primitives
 * handle directly, one through the catalog manager's entity read. The one write on this surface,
 * the root-container backfill, goes through the catalog and grant managers. Replaced when the
 * resolver's internals are re-pointed onto the per-domain managers.
 */
final class DefaultResolvedEntityReads implements ResolvedEntityReads {
  private final DurableRecordStore primitives;
  private final CatalogDurableManager catalog;
  private final GrantDurableManager grants;

  DefaultResolvedEntityReads(
      @NonNull DurableRecordStore primitives,
      @NonNull CatalogDurableManager catalog,
      @NonNull GrantDurableManager grants) {
    this.primitives = primitives;
    this.catalog = catalog;
    this.grants = grants;
  }

  @Override
  public @NonNull ChangeTrackingResult loadEntitiesChangeTracking(
      @NonNull PolarisCallContext callCtx, @NonNull List<PolarisEntityId> entityIds) {
    List<RecordRef> refs =
        entityIds.stream().map(id -> RecordRefs.entityIdentity(id.id())).toList();
    List<Optional<RecordVersions>> versions = primitives.versionsOf(refs);
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
    EntityResult found = catalog.loadEntity(callCtx, entityCatalogId, entityId, entityType);
    if (!found.isSuccess()) {
      return new ResolvedEntityResult(found.getReturnStatus(), found.getExtraInformation());
    }
    PolarisBaseEntity entity = found.getEntity();

    List<PolarisGrantRecord> grantRecords;
    if (entity.getType().isGrantee()) {
      grantRecords = new ArrayList<>(RecordRefs.grantsAsGrantee(primitives, entity));
      grantRecords.addAll(RecordRefs.grantsAsSecurable(primitives, entity));
    } else {
      grantRecords = RecordRefs.grantsAsSecurable(primitives, entity);
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
    List<PolarisGrantRecord> asSecurable = RecordRefs.grantsAsSecurable(primitives, entity);
    List<PolarisGrantRecord> asGrantee =
        entity.getType().isGrantee() ? RecordRefs.grantsAsGrantee(primitives, entity) : List.of();
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
    List<RecordRef> refs =
        entityIds.stream().map(id -> RecordRefs.entityIdentity(id.id())).toList();
    List<Optional<PolarisBaseEntity>> found = primitives.getMany(refs, PolarisBaseEntity.class);

    List<ResolvedPolarisEntity> resolved = new ArrayList<>(entityIds.size());
    for (Optional<PolarisBaseEntity> maybeEntity : found) {
      PolarisBaseEntity entity =
          maybeEntity.filter(e -> e.getTypeCode() == entityType.getCode()).orElse(null);
      resolved.add(toResolvedPolarisEntity(entity));
    }
    return new ResolvedEntitiesResult(resolved);
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
        primitives.get(
            RecordRefs.entityUniqueness(parentId, entityType.getCode(), entityName),
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
        grantRecords = new ArrayList<>(RecordRefs.grantsAsGrantee(primitives, entity));
        grantRecords.addAll(RecordRefs.grantsAsSecurable(primitives, entity));
      } else {
        grantRecords = RecordRefs.grantsAsSecurable(primitives, entity);
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
      EntityResult backfillResult = catalog.createEntityIfNotExists(callCtx, null, rootContainer);
      if (backfillResult.isSuccess()) {
        PolarisBaseEntity serviceAdminRole =
            primitives
                .get(
                    RecordRefs.entityUniqueness(
                        PolarisEntityConstants.getRootEntityId(),
                        PolarisEntityType.PRINCIPAL_ROLE.getCode(),
                        PolarisEntityConstants.getNameOfPrincipalServiceAdminRole()),
                    PolarisBaseEntity.class)
                .orElse(null);
        if (serviceAdminRole != null) {
          grants.grantPrivilegeOnSecurableToRole(
              callCtx,
              serviceAdminRole,
              null,
              rootContainer,
              PolarisPrivilege.SERVICE_MANAGE_ACCESS);
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
        primitives.get(RecordRefs.entityIdentity(entityId), PolarisBaseEntity.class);
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
        grantRecords =
            new ArrayList<>(RecordRefs.grantsAsGrantee(primitives, entityCatalogId, entityId));
        grantRecords.addAll(RecordRefs.grantsAsSecurable(primitives, entityCatalogId, entityId));
      } else {
        grantRecords = RecordRefs.grantsAsSecurable(primitives, entityCatalogId, entityId);
      }
    } else {
      grantRecords = null;
    }

    return new ResolvedEntityResult(entity, reportedGrantRecordsVersion, grantRecords);
  }
}
