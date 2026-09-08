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
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.LookupPath;
import org.apache.polaris.spi.durable.RecordRef;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Record references and kind-scoped reads shared by the durable manager implementations in this
 * package: the identity and uniqueness keys of every record kind, and the small reads that take the
 * primitives handle as an argument. Stateless; carries no business rule.
 */
final class RecordRefs {
  private RecordRefs() {}

  /** {@link PolarisRecordKinds#ENTITY}'s identity ref: {@code (realm, id)}, realm implicit. */
  static RecordRef entityIdentity(long id) {
    return RecordRef.byIdentity(PolarisRecordKinds.ENTITY, List.of(id));
  }

  /**
   * {@link PolarisRecordKinds#ENTITY}'s uniqueness ref: {@code (parent, type, name)}, verified
   * against both shipped stores' bindings ({@code TreeMapDurableRecordStore}, {@code
   * JdbcDurableRecordStore}). Deliberately no catalog-id component: both bindings key uniqueness on
   * {@code (parentId, typeCode, name)} alone, because ids are realm-wide unique so parentId already
   * disambiguates across catalogs.
   */
  static RecordRef entityUniqueness(long parentId, int typeCode, @NonNull String name) {
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
   * Atomic. It now does, for {@code DefaultCatalogDurableManager#createEntityIfNotExists}, {@code
   * DefaultCatalogDurableManager#createEntitiesIfNotExist}, {@code
   * DefaultCatalogDurableManager#renameEntity} and {@code
   * DefaultCatalogDurableManager#dropEntityIfExists}: each attaches an {@code EXISTS} precondition
   * per {@code catalogPath} entity to its mutation (see {@link #pathExistsPreconditions}), so a
   * path entity deleted between the read below and the commit fails the write instead of silently
   * succeeding underneath it — the concrete failure mode this closes is a table left hanging under
   * a concurrently-dropped namespace, which Atomic's own unconditional derivation cannot detect.
   * This is a REAL happens-before guarantee neither old implementation has: Atomic never re-checks
   * the path at all, and Transactional's re-check (via the package-private {@code
   * PolarisEntityResolver}) is safe only because it runs inside the same DB transaction as the
   * write — nothing states that as a condition, a wrapping transaction just happens to serialize
   * against the concurrent delete. {@code updateEntityPropertiesIfNotChanged} and its batch form
   * deliberately do NOT get this treatment: neither old implementation's update path uses {@code
   * catalogPath} to reach the entity being updated (it is resolved directly by catalogId+id), so
   * there is no "hanging under a deleted path" failure mode for update to close.
   */
  static long catalogIdOf(@Nullable List<PolarisEntityCore> catalogPath) {
    return catalogPath == null || catalogPath.isEmpty()
        ? PolarisEntityConstants.getNullId()
        : catalogPath.get(0).getId();
  }

  static long parentIdOf(@Nullable List<PolarisEntityCore> catalogPath) {
    return catalogPath == null || catalogPath.isEmpty()
        ? PolarisEntityConstants.getRootEntityId()
        : catalogPath.get(catalogPath.size() - 1).getId();
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
  static List<PolarisBaseEntity> listChildEntities(
      @NonNull DurableRecordStore store,
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
        store.list(
            PolarisRecordKinds.ENTITY,
            PolarisRecordKinds.ENTITY_BY_PARENT,
            anchors,
            pageToken,
            PolarisBaseEntity.class);
    return page.items().stream().filter(e -> e.getTypeCode() == entityType.getCode()).toList();
  }

  /**
   * {@code PRINCIPAL_SECRETS}'s identity ref: {@code (realm, client-id)} — every field is part of
   * the key, the same shape as {@link #grantIdentity}.
   */
  static RecordRef secretsIdentity(@NonNull String clientId) {
    return RecordRef.byIdentity(PolarisRecordKinds.PRINCIPAL_SECRETS, List.of(clientId));
  }

  /**
   * The grant records on which {@code entity} is the securable — the anchor every entity gets,
   * grantee or not. Shared by {@code DefaultResolvedEntityReads#loadResolvedEntityById}, {@code
   * DefaultResolvedEntityReads#loadResolvedEntities} and their {@code toResolvedPolarisEntity}
   * helper below.
   */
  static List<PolarisGrantRecord> grantsAsSecurable(
      @NonNull DurableRecordStore store, @NonNull PolarisEntityCore entity) {
    return grantsAsSecurable(store, entity.getCatalogId(), entity.getId());
  }

  static List<PolarisGrantRecord> grantsAsSecurable(
      @NonNull DurableRecordStore store, long catalogId, long id) {
    return store
        .list(
            PolarisRecordKinds.GRANT_RECORD,
            PolarisRecordKinds.GRANT_RECORD_BY_SECURABLE,
            List.of(catalogId, id),
            PageToken.readEverything(),
            PolarisGrantRecord.class)
        .items();
  }

  /** The grant records where {@code entity} is the grantee — only meaningful when it is one. */
  static List<PolarisGrantRecord> grantsAsGrantee(
      @NonNull DurableRecordStore store, @NonNull PolarisEntityCore entity) {
    return grantsAsGrantee(store, entity.getCatalogId(), entity.getId());
  }

  static List<PolarisGrantRecord> grantsAsGrantee(
      @NonNull DurableRecordStore store, long catalogId, long id) {
    return store
        .list(
            PolarisRecordKinds.GRANT_RECORD,
            PolarisRecordKinds.GRANT_RECORD_BY_GRANTEE,
            List.of(catalogId, id),
            PageToken.readEverything(),
            PolarisGrantRecord.class)
        .items();
  }

  // ---------------------------------------------------------- GrantDurableManager (ticket 91)

  /**
   * Grant-record identity ref: {@code (securable-catalog, securable, grantee-catalog, grantee,
   * privilege)} — every field is part of the key, so identity and uniqueness are the same tuple
   * (verified against both shipped stores' bindings, {@code TreeMapDurableRecordStore} and {@code
   * JdbcDurableRecordStore}).
   */
  static RecordRef grantIdentity(@NonNull PolarisGrantRecord g) {
    return RecordRef.byIdentity(
        PolarisRecordKinds.GRANT_RECORD,
        List.of(
            g.getSecurableCatalogId(),
            g.getSecurableId(),
            g.getGranteeCatalogId(),
            g.getGranteeId(),
            g.getPrivilegeCode()));
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
  static PolarisBaseEntity mustLoadEntity(
      @NonNull DurableRecordStore store,
      @NonNull PolarisDiagnostics diagnostics,
      @NonNull PolarisEntityCore entity,
      String signature) {
    PolarisBaseEntity loaded =
        store.get(entityIdentity(entity.getId()), PolarisBaseEntity.class).orElse(null);
    diagnostics.checkNotNull(loaded, signature, "entity={}", entity);
    return loaded;
  }

  // ------------------------------------------------- PolicyDurableManager (ticket 92)

  /**
   * Policy-mapping identity ref: {@code (target-catalog, target, policy-type, policy-catalog,
   * policy)} — the whole tuple is the key ({@code parameters} is not part of it), so identity and
   * uniqueness coincide, verified against both shipped stores' bindings ({@code
   * TreeMapDurableRecordStore#policyKey}, {@code JdbcDurableRecordStore}'s identity column list —
   * both state exactly this order).
   */
  static RecordRef policyMappingIdentity(@NonNull PolarisPolicyMappingRecord record) {
    return RecordRef.byIdentity(
        PolarisRecordKinds.POLICY_MAPPING,
        List.of(
            record.getTargetCatalogId(),
            record.getTargetId(),
            record.getPolicyTypeCode(),
            record.getPolicyCatalogId(),
            record.getPolicyId()));
  }

  /** Every mapping record on one anchor of {@code path} — the policy-side twin of loadGrants. */
  static List<PolarisPolicyMappingRecord> policyMappingsOn(
      @NonNull DurableRecordStore store,
      @NonNull LookupPath path,
      long anchorCatalogId,
      long anchorId) {
    return store
        .list(
            PolarisRecordKinds.POLICY_MAPPING,
            path,
            List.of(anchorCatalogId, anchorId),
            PageToken.readEverything(),
            PolarisPolicyMappingRecord.class)
        .items();
  }

  // ------------------------------------------------------- EventDurableManager (ticket 92)

  /** {@code EVENT}'s identity ref: {@code (event-id)} — both stores' bindings key on it alone. */
  static RecordRef eventIdentity(@NonNull EventEntity event) {
    return RecordRef.byIdentity(PolarisRecordKinds.EVENT, List.of(event.getId()));
  }

  /**
   * All children checks below are READS, not preconditions: {@code Precondition} declares no
   * set-emptiness operator (see its own "Deliberately absent" section), so "no children under this
   * parent" cannot ride into the commit the way the retrofit's path checks do. This leaves the
   * identical TOCTOU window both old impls already carry between this read and the write — {@code
   * AtomicOperationMetaStoreManager}'s own five TODOs concede the same gap for the same reason, so
   * this is parity, not a regression introduced here.
   */
  static List<PolarisBaseEntity> rawChildEntities(
      @NonNull DurableRecordStore store, long catalogId, long parentId) {
    return store
        .list(
            PolarisRecordKinds.ENTITY,
            PolarisRecordKinds.ENTITY_BY_PARENT,
            List.of(catalogId, parentId),
            PageToken.readEverything(),
            PolarisBaseEntity.class)
        .items();
  }
}
