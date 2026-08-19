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
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.BaseDurableManagerTest;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.PolarisTestMetaStoreManager;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.PredefinedPolicyTypes;
import org.apache.polaris.extension.orchestration.DefaultDurableOrchestrator;
import org.apache.polaris.extension.primitives.routing.MappedDurableRecordStoreLocator;
import org.apache.polaris.extension.primitives.routing.RoutingDurableRecordStore;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.LookupPath;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Wires {@link DefaultDurableManager} into {@link BaseDurableManagerTest} against a fresh {@link
 * DurableRecordStore} per test, assembled through the same mapped locator, routing store, and
 * orchestrator every production deployment uses.
 *
 * <p>The {@link PolarisCallContext} handed to the manager under test carries a {@link
 * NeverCallOldPrimitives} stub whose every method throws. That stub is not filler: it is the test's
 * proof that {@link DefaultDurableManager} never reaches for the old primitives handle on the call
 * context. If it ever does, a test fails loudly here instead of silently reading through the old
 * door.
 *
 * <p>Ticket 91 disabled the five fixture tests outside its scope here; ticket 92 removes those
 * overrides as it implements each surface. Any override still present below re-declares {@code
 * @Test} alongside {@code @Disabled}, because an override without {@code @Test} does not inherit
 * the annotation and simply vanishes from discovery with no skipped entry — verified against the
 * junit-platform-commons 1.11.3 source this module depends on transitively.
 */
public abstract class AbstractDefaultDurableManagerTest extends BaseDurableManagerTest {

  protected abstract DurableRecordStore newStore();

  /**
   * Kept from the last {@link #createPolarisTestMetaStoreManager} call so the ticket-92 proving
   * cases below can drive the manager directly and observe results through the NEW handle. The
   * shared fixture family carries no oracle for these surfaces (no purge caller, and its one
   * policy-cleanup oracle reads the old handle — see {@link #testPolicyMappingCleanup}), so the
   * proving cases ride here, per EJ's ticket-85 ruling that proving tests belong to the ticket
   * whose claim they prove. Both bindings run them.
   */
  protected DefaultDurableManager managerUnderTest;

  protected DurableRecordStore newHandle;
  protected PolarisCallContext newModelCallCtx;

  @Override
  protected PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
    DurableRecordStore store = newStore();
    // The real assembly path, single-store: every kind mapped explicitly (no default store), the
    // routing implementation as the ONE primitives handle both collaborators hold.
    DurableRecordStore primitives =
        new RoutingDurableRecordStore(
            new MappedDurableRecordStoreLocator(
                Map.of(
                    PolarisRecordKinds.ENTITY, "main",
                    PolarisRecordKinds.GRANT_RECORD, "main",
                    PolarisRecordKinds.POLICY_MAPPING, "main",
                    PolarisRecordKinds.PRINCIPAL_SECRETS, "main",
                    PolarisRecordKinds.EVENT, "main"),
                Map.of("main", store)),
            List.of(store),
            store);
    var orchestrator = new DefaultDurableOrchestrator(primitives);
    var manager =
        new DefaultDurableManager(
            clock,
            new PolarisDefaultDiagServiceImpl(),
            orchestrator,
            primitives,
            PrincipalSecretsGenerator.RANDOM_SECRETS);

    RealmContext realmContext = () -> "testRealm";
    PolarisCallContext callCtx = new PolarisCallContext(realmContext, new NeverCallOldPrimitives());

    // testStartTime must be captured BEFORE bootstrap runs, matching the standard 2-arg
    // PolarisTestMetaStoreManager constructor's own sequencing (it captures the time, then calls
    // purge+bootstrap). Bootstrap-created entities stamp createTimestamp via
    // System.currentTimeMillis() at build time; capturing testStartTime afterward would make every
    // fixture assertion of "testStartTime <= entity.getCreateTimestamp()" (ensureExistsById, used
    // by validateBootstrap/testLookup/etc.) fail for the bootstrapped root principal and role.
    long testStartTime = System.currentTimeMillis();
    manager.bootstrapPolarisService(callCtx);
    this.managerUnderTest = manager;
    this.newHandle = primitives;
    this.newModelCallCtx = callCtx;
    return new PolarisTestMetaStoreManager(manager, callCtx, testStartTime, true);
  }

  // ------------------------------------------------------------ ticket-92 proving cases

  private PolarisBaseEntity newEntity(
      long catalogId,
      long parentId,
      PolarisEntityType type,
      PolarisEntitySubType subType,
      String name,
      Map<String, String> properties) {
    return new PolarisBaseEntity.Builder()
        .catalogId(catalogId)
        .id(managerUnderTest.generateNewEntityId(newModelCallCtx).getId())
        .typeCode(type.getCode())
        .subTypeCode(subType.getCode())
        .parentId(parentId)
        .name(name)
        .propertiesAsMap(properties)
        .internalPropertiesAsMap(Map.of())
        .build();
  }

  private PolarisBaseEntity created(
      List<PolarisEntityCore> catalogPath, PolarisBaseEntity entity) {
    EntityResult result =
        managerUnderTest.createEntityIfNotExists(newModelCallCtx, catalogPath, entity);
    Assertions.assertThat(result.isSuccess()).isTrue();
    return result.getEntity();
  }

  private List<PolarisPolicyMappingRecord> mappingsOn(
      LookupPath path, long anchorCatalogId, long anchorId) {
    return newHandle
        .list(
            PolarisRecordKinds.POLICY_MAPPING,
            path,
            List.of(anchorCatalogId, anchorId),
            PageToken.readEverything(),
            PolarisPolicyMappingRecord.class)
        .items();
  }

  /**
   * The module-local twin of the blocked {@code testPolicyMappingCleanup} oracle, observed through
   * the NEW handle instead of the old one: dropping a policy-target entity deletes its mappings
   * unconditionally (target side), and dropping a policy deletes its mappings unconditionally
   * (policy side) — the obligation recorded in {@code DefaultDurableManager#collectDropMutations}'s
   * javadoc since ticket 91's refute pass.
   */
  @Test
  protected void dropPathCleansPolicyMappingsObservedThroughTheNewHandle() {
    PolarisBaseEntity catalog =
        created(
            null,
            newEntity(
                PolarisEntityConstants.getNullId(),
                PolarisEntityConstants.getRootEntityId(),
                PolarisEntityType.CATALOG,
                PolarisEntitySubType.NULL_SUBTYPE,
                "C",
                Map.of()));
    PolarisBaseEntity namespace =
        created(
            List.of(catalog),
            newEntity(
                catalog.getId(),
                catalog.getId(),
                PolarisEntityType.NAMESPACE,
                PolarisEntitySubType.NULL_SUBTYPE,
                "N",
                Map.of()));
    List<PolarisEntityCore> nsPath = List.of(catalog, namespace);
    PolarisBaseEntity table1 =
        created(
            nsPath,
            newEntity(
                catalog.getId(),
                namespace.getId(),
                PolarisEntityType.TABLE_LIKE,
                PolarisEntitySubType.ICEBERG_TABLE,
                "T1",
                Map.of()));
    PolarisBaseEntity table2 =
        created(
            nsPath,
            newEntity(
                catalog.getId(),
                namespace.getId(),
                PolarisEntityType.TABLE_LIKE,
                PolarisEntitySubType.ICEBERG_TABLE,
                "T2",
                Map.of()));
    Map<String, String> dataCompaction =
        Map.of(
            PolicyEntity.POLICY_TYPE_CODE_KEY,
            Integer.toString(PredefinedPolicyTypes.DATA_COMPACTION.getCode()));
    PolicyEntity policy1 =
        PolicyEntity.of(
            created(
                List.of(catalog),
                newEntity(
                    catalog.getId(),
                    catalog.getId(),
                    PolarisEntityType.POLICY,
                    PolarisEntitySubType.NULL_SUBTYPE,
                    "P1",
                    dataCompaction)));
    PolicyEntity policy2 =
        PolicyEntity.of(
            created(
                List.of(catalog),
                newEntity(
                    catalog.getId(),
                    catalog.getId(),
                    PolarisEntityType.POLICY,
                    PolarisEntitySubType.NULL_SUBTYPE,
                    "P2",
                    dataCompaction)));

    // Target side: attach P1 -> T1, drop T1, the mapping goes with it.
    Assertions.assertThat(
            managerUnderTest
                .attachPolicyToEntity(newModelCallCtx, nsPath, table1, List.of(catalog), policy1, null)
                .isSuccess())
        .isTrue();
    Assertions.assertThat(
            mappingsOn(
                PolarisRecordKinds.POLICY_MAPPING_BY_POLICY, catalog.getId(), policy1.getId()))
        .hasSize(1);
    Assertions.assertThat(
            managerUnderTest
                .dropEntityIfExists(newModelCallCtx, nsPath, table1, Map.of(), true)
                .isSuccess())
        .isTrue();
    Assertions.assertThat(
            mappingsOn(
                PolarisRecordKinds.POLICY_MAPPING_BY_POLICY, catalog.getId(), policy1.getId()))
        .isEmpty();

    // Policy side: attach P2 -> T2, drop P2 itself, the mapping goes with it.
    Assertions.assertThat(
            managerUnderTest
                .attachPolicyToEntity(newModelCallCtx, nsPath, table2, List.of(catalog), policy2, null)
                .isSuccess())
        .isTrue();
    Assertions.assertThat(
            managerUnderTest
                .dropEntityIfExists(newModelCallCtx, List.of(catalog), policy2, Map.of(), true)
                .isSuccess())
        .isTrue();
    Assertions.assertThat(
            mappingsOn(PolarisRecordKinds.POLICY_MAPPING_BY_TARGET, catalog.getId(), table2.getId()))
        .isEmpty();
  }

  /**
   * Pins the {@code POLICY_HAS_MAPPINGS} pre-check both old impls run when a still-attached policy
   * is dropped WITHOUT cleanup — unexercised by the shared fixture (its policy drops pass {@code
   * cleanup=true}), reachable in the new model only since attach exists.
   */
  @Test
  protected void droppingAnAttachedPolicyWithoutCleanupIsRefused() {
    PolarisBaseEntity catalog =
        created(
            null,
            newEntity(
                PolarisEntityConstants.getNullId(),
                PolarisEntityConstants.getRootEntityId(),
                PolarisEntityType.CATALOG,
                PolarisEntitySubType.NULL_SUBTYPE,
                "C",
                Map.of()));
    PolarisBaseEntity namespace =
        created(
            List.of(catalog),
            newEntity(
                catalog.getId(),
                catalog.getId(),
                PolarisEntityType.NAMESPACE,
                PolarisEntitySubType.NULL_SUBTYPE,
                "N",
                Map.of()));
    List<PolarisEntityCore> nsPath = List.of(catalog, namespace);
    PolarisBaseEntity table =
        created(
            nsPath,
            newEntity(
                catalog.getId(),
                namespace.getId(),
                PolarisEntityType.TABLE_LIKE,
                PolarisEntitySubType.ICEBERG_TABLE,
                "T",
                Map.of()));
    PolicyEntity policy =
        PolicyEntity.of(
            created(
                List.of(catalog),
                newEntity(
                    catalog.getId(),
                    catalog.getId(),
                    PolarisEntityType.POLICY,
                    PolarisEntitySubType.NULL_SUBTYPE,
                    "P",
                    Map.of(
                        PolicyEntity.POLICY_TYPE_CODE_KEY,
                        Integer.toString(PredefinedPolicyTypes.DATA_COMPACTION.getCode())))));
    Assertions.assertThat(
            managerUnderTest
                .attachPolicyToEntity(newModelCallCtx, nsPath, table, List.of(catalog), policy, null)
                .isSuccess())
        .isTrue();

    DropEntityResult refused =
        managerUnderTest.dropEntityIfExists(newModelCallCtx, List.of(catalog), policy, Map.of(), false);
    Assertions.assertThat(refused.getReturnStatus())
        .isEqualTo(BaseResult.ReturnStatus.POLICY_HAS_MAPPINGS);

    // With cleanup=true the same drop succeeds and takes the mapping with it.
    Assertions.assertThat(
            managerUnderTest
                .dropEntityIfExists(newModelCallCtx, List.of(catalog), policy, Map.of(), true)
                .isSuccess())
        .isTrue();
    Assertions.assertThat(
            mappingsOn(PolarisRecordKinds.POLICY_MAPPING_BY_TARGET, catalog.getId(), table.getId()))
        .isEmpty();
  }

  /**
   * Ticket 92's ONE remaining disabled case, under the ticket's stop rule rather than missing
   * implementation: the fixture's final oracle reads the OLD primitives handle directly ({@code
   * PolarisTestMetaStoreManager#testPolicyMappingCleanup} calls {@code
   * polarisCallContext.getMetaStore().loadAllTargetsOnPolicy(...)}), the only reach into that
   * handle in the whole fixture family. The {@link NeverCallOldPrimitives} stub deliberately does
   * not serve it (weakening the stub would break its proof), and {@code PolicyMappingPersistence}'s
   * own default for the method throws, so the case cannot pass against this binding regardless of
   * what the manager implements. The by-policy oracle exists only on the old door — the manager
   * surface has no loadAllTargetsOnPolicy. Reported to the room as a fixture-oracle design finding
   * (2026-08-19); this flips when the oracle's owner rules and fixes it. The BEHAVIOUR the case
   * exercises (unconditional drop-path mapping cleanup) is implemented and proven by {@link
   * #dropPathCleansPolicyMappingsObservedThroughTheNewHandle}, whose oracle is the new handle.
   */
  @Override
  @Test
  @Disabled("ticket 92 stop rule: fixture oracle reads the old primitives handle (reported)")
  protected void testPolicyMappingCleanup() {}

}
