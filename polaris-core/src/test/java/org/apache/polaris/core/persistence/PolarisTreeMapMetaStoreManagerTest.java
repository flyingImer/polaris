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

import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.persistence.transactional.TransactionalMetaStoreManagerImpl;
import org.apache.polaris.persistence.treemap.TreeMapDurablePrimitivesImpl;
import org.apache.polaris.persistence.treemap.TreeMapSlices;

/**
 * Upstream's own test for {@link TransactionalMetaStoreManagerImpl}, in upstream's location and
 * under upstream's name. Only its two import lines changed, because the TreeMap primitives
 * implementation it uses as a fixture moved to its own module.
 *
 * <p><b>Why it lives here and not with the TreeMap implementation.</b> Its subject is
 * TransactionalMetaStoreManagerImpl, a polaris-core class, and a test belongs with its subject. An
 * earlier version of this PoC moved this file into the TreeMap module because its <em>fixture</em>
 * is TreeMap, which put a manager-layer test inside a primitives-implementation module. A
 * primitives module holds primitives implementations and their tests; nothing else.
 *
 * <p><b>Which layer the subject sits in:</b> TransactionalMetaStoreManagerImpl extends
 * BaseMetaStoreManager, which implements DurableManager plus the per-domain contracts
 * CatalogDurableManager, PrincipalDurableManager, TaskDurableManager, GrantDurableManager,
 * SecretsDurableManager, PolicyDurableManager and EventDurableManager, and the transitional
 * ResolvedEntityReads. So the subject is a <b>durable manager</b>, the top of the four layers,
 * exercised here over the TreeMap durable-primitives implementation.
 *
 * <p><b>The subject does not survive the overhaul, and neither does this test.</b>
 * TransactionalMetaStoreManagerImpl carries the interactive-transaction shape Issue 47 S8 removes:
 * reads happen before a commit and ride in as preconditions, so the ...InCurrentTxn hooks and the
 * TransactionalPersistence / AbstractTransactionalPersistence family above them dissolve into
 * commit plus the four reads. Issue 55 resolved that the manager layer keeps its current shape for
 * this PoC, so the subject is still here on purpose at this stage rather than by omission.
 * Dissolving it means migrating the manager layer, which is explicitly out of the PoC's scope.
 */
public class PolarisTreeMapMetaStoreManagerTest extends BaseDurableManagerTest {
  @Override
  public PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
    PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();
    TreeMapSlices store = new TreeMapSlices(diagServices);
    TreeMapDurablePrimitivesImpl metaStore =
        new TreeMapDurablePrimitivesImpl(diagServices, store, RANDOM_SECRETS);
    TransactionalMetaStoreManagerImpl metaStoreManager =
        new TransactionalMetaStoreManagerImpl(clock, diagServices);
    PolarisCallContext callCtx = new PolarisCallContext(() -> "testRealm", metaStore);
    return new PolarisTestMetaStoreManager(metaStoreManager, callCtx);
  }
}
