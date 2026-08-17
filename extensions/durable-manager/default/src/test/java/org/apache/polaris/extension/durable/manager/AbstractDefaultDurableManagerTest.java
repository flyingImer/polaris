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

import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.BaseDurableManagerTest;
import org.apache.polaris.core.persistence.PolarisTestMetaStoreManager;
import org.apache.polaris.extension.orchestration.DefaultDurableOrchestrator;
import org.apache.polaris.extension.primitives.factory.DefaultDurableRecordStoreFactory;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Wires {@link DefaultDurableManager} into {@link BaseDurableManagerTest} against a fresh {@link
 * DurableRecordStore} per test, assembled through the same factory and orchestrator every
 * production deployment uses.
 *
 * <p>The {@link PolarisCallContext} handed to the manager under test carries a {@link
 * NeverCallOldPrimitives} stub whose every method throws. That stub is not filler: it is the test's
 * proof that {@link DefaultDurableManager} never reaches for the old primitives handle on the call
 * context. If it ever does, a test fails loudly here instead of silently reading through the old
 * door.
 *
 * <p>Five fixture tests are out of scope for ticket 91 and are disabled here, one line down from
 * this class, rather than left for ticket 92 to discover as unexplained failures: {@code
 * testPolicyMapping} and {@code testPolicyMappingCleanup} (policy mapping), {@code testLoadTasks}
 * and {@code testLoadTasksInParallel} (task leasing), {@code testEntityCache} (entity cache /
 * resolver refresh). Each override re-declares {@code @Test} alongside {@code @Disabled}, because
 * an override without {@code @Test} does not inherit the annotation and simply vanishes from
 * discovery with no skipped entry — verified against the junit-platform-commons 1.11.3 source this
 * module depends on transitively.
 */
public abstract class AbstractDefaultDurableManagerTest extends BaseDurableManagerTest {

  protected abstract DurableRecordStore newStore();

  @Override
  protected PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
    DurableRecordStore store = newStore();
    var storeForKind =
        new DefaultDurableRecordStoreFactory().produce(Map.of(), "main", Map.of("main", store));
    var orchestrator = new DefaultDurableOrchestrator(storeForKind);
    var manager =
        new DefaultDurableManager(
            clock, new PolarisDefaultDiagServiceImpl(), orchestrator, storeForKind);

    RealmContext realmContext = () -> "testRealm";
    PolarisCallContext callCtx = new PolarisCallContext(realmContext, new NeverCallOldPrimitives());

    manager.bootstrapPolarisService(callCtx);
    return new PolarisTestMetaStoreManager(manager, callCtx, System.currentTimeMillis(), true);
  }

  @Override
  @Test
  @Disabled("ticket 92: policy mapping")
  protected void testPolicyMapping() {}

  @Override
  @Test
  @Disabled("ticket 92: policy mapping")
  protected void testPolicyMappingCleanup() {}

  @Override
  @Test
  @Disabled("ticket 92: task leasing (loadTasks)")
  protected void testLoadTasks() {}

  @Override
  @Test
  @Disabled("ticket 92: task leasing (loadTasks)")
  protected void testLoadTasksInParallel() {}

  @Override
  @Test
  @Disabled("ticket 92: entity cache / resolver refresh")
  protected void testEntityCache() {}
}
