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
package org.apache.polaris.persistence.treemap;

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;

import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.persistence.BaseDurableManagerTest;
import org.apache.polaris.core.persistence.PolarisTestMetaStoreManager;
import org.apache.polaris.core.persistence.transactional.TransactionalMetaStoreManagerImpl;

/**
 * Upstream's own test for TransactionalMetaStoreManagerImpl, kept under upstream's name, moved with
 * its subject's implementation and otherwise unchanged.
 *
 * <p><b>The name is stale and deliberately not corrected here.</b> It fuses a durable-manager
 * concept with a storage backend, which the target model keeps apart. Two earlier attempts to fix
 * that invented names instead: first "TreeMapDurableManager", welding a business-aware layer onto a
 * storage detail, then "TransactionalManager", which is not a layer in the four-layer stack at all
 * but a shipped class name with a word chopped out of it. Renaming a shipped class's test to a name
 * nobody agreed on is worse than carrying a stale name, so this carries the stale one.
 *
 * <p><b>Which layer the subject sits in:</b> TransactionalMetaStoreManagerImpl extends
 * BaseMetaStoreManager, which implements DurableManager, GrantManager, SecretsManager,
 * PolarisPolicyMappingManager and PolarisEventManager. So the subject is a <b>durable manager</b>,
 * the top of the four layers, exercised here over the TreeMap durable-primitives implementation.
 *
 * <p><b>Both the subject and this test are transitional.</b> TransactionalMetaStoreManagerImpl
 * carries the interactive-transaction shape Issue 47 S8 removes: reads happen before a commit and
 * ride in as preconditions, so the ...InCurrentTxn hooks and the TransactionalPersistence /
 * AbstractTransactionalPersistence family above them dissolve into commit plus the four reads.
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
