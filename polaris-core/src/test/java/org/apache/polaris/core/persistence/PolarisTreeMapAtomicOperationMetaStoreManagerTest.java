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
package org.apache.polaris.core.persistence;

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.dao.entity.CreateCatalogResult;
import org.apache.polaris.core.persistence.transactional.TreeMapMetaStore;
import org.apache.polaris.core.persistence.transactional.TreeMapTransactionalPersistenceImpl;
import org.apache.polaris.spi.durable.DurableManager;
import org.apache.polaris.spi.durable.DurablePrimitives;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class PolarisTreeMapAtomicOperationMetaStoreManagerTest extends BaseDurableManagerTest {
  @Override
  public PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    TreeMapMetaStore store = new TreeMapMetaStore(diagServices);
    TreeMapTransactionalPersistenceImpl metaStore =
        new TreeMapTransactionalPersistenceImpl(diagServices, store, RANDOM_SECRETS);
    AtomicOperationMetaStoreManager metaStoreManager =
        new AtomicOperationMetaStoreManager(clock, diagServices);
    PolarisCallContext callCtx = new PolarisCallContext(() -> "testRealm", metaStore);
    return new PolarisTestMetaStoreManager(metaStoreManager, callCtx);
  }

  @Override
  @Test
  @Disabled(
      "AtomicOperationMetaStoreManager calls storePrincipalSecrets outside a transaction, which is incompatible with "
          + "TreeMap's transactional slice reads. Collision detection is covered by JDBC and NoSQL backend tests.")
  protected void testResetCredentialsClientIdCollision() {}

  /**
   * ADR-0002 durable-parity invariant (HARD), mechanism facet: {@code createCatalog} on the
   * atomic/CAS manager must commit the catalog + adminRole + grants through exactly one {@code
   * commitChangeSet} call, not the ~11 independent, individually non-atomic primitive writes it
   * used to issue one at a time. This is the directly-verifiable claim -- it does not, and cannot,
   * prove no partially-initialized catalog can ever exist after a real mid-transaction crash; a
   * unit test cannot honestly simulate that.
   */
  @Test
  void testCreateCatalogIssuesExactlyOneCommitChangeSet() {
    AtomicInteger commitChangeSetCalls = new AtomicInteger();
    AtomicInteger individualWriteCalls = new AtomicInteger();
    PolarisCallContext countingCallCtx =
        withInterceptedPrimitives(
            (real, methodName, args, result) -> {
              switch (methodName) {
                case "commitChangeSet" -> commitChangeSetCalls.incrementAndGet();
                case "writeEntity", "writeEntities", "writeToGrantRecords" ->
                    individualWriteCalls.incrementAndGet();
                default -> {}
              }
            });
    DurableManager mgr = polarisTestMetaStoreManager.polarisMetaStoreManager;

    PolarisBaseEntity catalog =
        new PolarisBaseEntity(
            PolarisEntityConstants.getNullId(),
            mgr.generateNewEntityId(countingCallCtx).getId(),
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootEntityId(),
            "single_commit_catalog");

    CreateCatalogResult result = mgr.createCatalog(countingCallCtx, catalog, List.of());

    Assertions.assertThat(result.isSuccess()).isTrue();
    Assertions.assertThat(commitChangeSetCalls.get())
        .as(
            "createCatalog must commit the catalog + adminRole + grants as one commitChangeSet"
                + " call")
        .isEqualTo(1);
    Assertions.assertThat(individualWriteCalls.get())
        .as(
            "createCatalog must not also fall back to individual"
                + " writeEntity/writeEntities/writeToGrantRecords calls once it commits the"
                + " change-set")
        .isEqualTo(0);
  }

  /**
   * ADR-0002 durable-parity invariant (HARD), baseline-binding facet: a change-set's update
   * mutation must carry the exact entity object the caller last read as its CAS baseline, per
   * {@code EntityMutation#update}'s javadoc. This test simulates a concurrent writer bumping the
   * service admin role's {@code grantRecordsVersion} between {@code createCatalog}'s read of it and
   * the {@code commitChangeSet} call that uses that read as its baseline: the whole commit must
   * fail with {@link RetryOnConcurrencyException}, and nothing from the aborted change-set -- not
   * the catalog, not the admin role, not any grant -- must be visible afterwards.
   */
  @Test
  void testCreateCatalogRejectsStaleGrantRecordsVersionBaseline() {
    PolarisCallContext realCallCtx = polarisTestMetaStoreManager.polarisCallContext;
    DurableManager mgr = polarisTestMetaStoreManager.polarisMetaStoreManager;
    DurablePrimitives real = realCallCtx.getMetaStore();
    PolarisCallContext rawCallCtx = new PolarisCallContext(realCallCtx.getRealmContext(), real);

    PolarisCallContext interceptingCallCtx =
        withInterceptedPrimitives(
            (r, methodName, args, result) -> {
              if (methodName.equals("lookupEntityByName")
                  && PolarisEntityConstants.getNameOfPrincipalServiceAdminRole().equals(args[4])) {
                PolarisBaseEntity serviceAdminRole = (PolarisBaseEntity) result;
                // Simulate a concurrent writer bumping this entity's grantRecordsVersion between
                // this read (which createCatalog will bind as its CAS baseline) and the commit.
                r.writeEntity(
                    rawCallCtx,
                    serviceAdminRole.withGrantRecordsVersion(
                        serviceAdminRole.getGrantRecordsVersion() + 1),
                    false,
                    serviceAdminRole);
              }
            });

    PolarisBaseEntity catalog =
        new PolarisBaseEntity(
            PolarisEntityConstants.getNullId(),
            mgr.generateNewEntityId(interceptingCallCtx).getId(),
            PolarisEntityType.CATALOG,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootEntityId(),
            "stale_baseline_catalog");

    Assertions.assertThatThrownBy(() -> mgr.createCatalog(interceptingCallCtx, catalog, List.of()))
        .isInstanceOf(RetryOnConcurrencyException.class);

    Assertions.assertThat(
            mgr.loadEntity(
                    rawCallCtx, catalog.getCatalogId(), catalog.getId(), PolarisEntityType.CATALOG)
                .getEntity())
        .as("catalog must not be visible after the whole change-set was rejected")
        .isNull();
  }
}
