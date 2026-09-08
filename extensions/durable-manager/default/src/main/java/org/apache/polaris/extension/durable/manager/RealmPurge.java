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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableOrchestrator;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.OrchestrationResult;
import org.apache.polaris.spi.durable.RecordRef;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Realm-wide purge on the new persistence stack: walks every entity, then deletes secrets, policy
 * mappings, grant records and entities leaf-ward in commits no larger than the primitives'
 * per-commit cap, so any crash prefix leaves the remainder reachable to a re-run. Owned by realm
 * provisioning, which sits beside the durable managers; held here until the new stack's provisioner
 * exists.
 */
final class RealmPurge {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealmPurge.class);
  private final DurableOrchestrator orchestrator;
  private final DurableRecordStore primitives;

  RealmPurge(@NonNull DurableOrchestrator orchestrator, @NonNull DurableRecordStore primitives) {
    this.orchestrator = orchestrator;
    this.primitives = primitives;
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
  @NonNull BaseResult purge(@NonNull PolarisCallContext callCtx) {
    LOGGER.warn("Deleting all metadata in the metastore...");

    List<PolarisBaseEntity> entities = walkAllEntities();

    Map<RecordRef, PolarisGrantRecord> grants = new LinkedHashMap<>();
    Map<RecordRef, PolarisPolicyMappingRecord> mappings = new LinkedHashMap<>();
    for (PolarisBaseEntity entity : entities) {
      for (PolarisGrantRecord g : RecordRefs.grantsAsSecurable(primitives, entity)) {
        grants.putIfAbsent(RecordRefs.grantIdentity(g), g);
      }
      for (PolarisGrantRecord g : RecordRefs.grantsAsGrantee(primitives, entity)) {
        grants.putIfAbsent(RecordRefs.grantIdentity(g), g);
      }
      for (PolarisPolicyMappingRecord m :
          RecordRefs.policyMappingsOn(
              primitives,
              PolarisRecordKinds.POLICY_MAPPING_BY_TARGET,
              entity.getCatalogId(),
              entity.getId())) {
        mappings.putIfAbsent(RecordRefs.policyMappingIdentity(m), m);
      }
      if (entity.getType() == PolarisEntityType.POLICY) {
        for (PolarisPolicyMappingRecord m :
            RecordRefs.policyMappingsOn(
                primitives,
                PolarisRecordKinds.POLICY_MAPPING_BY_POLICY,
                entity.getCatalogId(),
                entity.getId())) {
          mappings.putIfAbsent(RecordRefs.policyMappingIdentity(m), m);
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
                  RecordRefs.secretsIdentity(clientId),
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
              RecordRefs.entityIdentity(entities.get(i).getId()),
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
      for (PolarisBaseEntity entity :
          RecordRefs.rawChildEntities(primitives, anchor[0], anchor[1])) {
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
}
