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
 * The shipped TransactionalMetaStoreManagerImpl's own test suite, run against the TreeMap
 * durable-primitives implementation.
 *
 * <p>Named "TransactionalMetaStoreManagerImpl with TreeMap primitives" rather than fusing the two,
 * mirroring the JDBC sibling AtomicMetastoreManagerWithJdbcDurablePrimitivesImplTest. A durable
 * manager is business-aware and a TreeMap is a storage detail; there is no such thing as a "TreeMap
 * durable manager", and an earlier version of this file's name implied one.
 *
 * <p><b>Both names here are transitional.</b> TransactionalMetaStoreManagerImpl carries the
 * interactive-transaction shape that Issue 47 S8 removes: reads happen before a commit and ride in
 * as preconditions, so the ...InCurrentTxn hooks and the TransactionalPersistence family above them
 * dissolve into commit plus the four reads. When that lands, this suite tests whatever the manager
 * layer becomes.
 */
public class TransactionalManagerWithTreeMapPrimitivesTest extends BaseDurableManagerTest {
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
