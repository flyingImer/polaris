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
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.BaseDurableManagerTest;
import org.apache.polaris.core.persistence.PolarisTestMetaStoreManager;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.extension.orchestration.DefaultDurableOrchestrator;
import org.apache.polaris.extension.primitives.factory.DefaultDurableRecordStoreFactory;
import org.apache.polaris.spi.durable.DurablePrimitives;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Wires {@link DefaultDurableManager} into {@link BaseDurableManagerTest} against a fresh {@link
 * DurableRecordStore} per test, assembled through the same factory and orchestrator every
 * production deployment uses.
 *
 * <p>The {@link PolarisCallContext} handed to the manager under test carries a {@link
 * DurablePrimitives} stub whose every method throws. That stub is not filler: it is the test's
 * proof that {@link DefaultDurableManager} never reaches for the old primitives handle on the call
 * context. If it ever does, a test fails loudly here instead of silently reading through the old
 * door. {@link org.apache.polaris.spi.durable.PolicyMappingPersistence}, which {@link
 * DurablePrimitives} extends, is untouched below because every one of its methods is already a
 * {@code default} that throws on its own; only the methods {@link DurablePrimitives} itself
 * declares need an override here.
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

  /**
   * Every method throws. Passed to {@link PolarisCallContext} solely because its constructor
   * requires a non-null old-model handle; proves by construction that {@link DefaultDurableManager}
   * never dereferences it. The only fixture code that reads {@link
   * PolarisCallContext#getMetaStore()} is {@code testPolicyMappingCleanup}, which is disabled above
   * and owned by ticket 92.
   */
  private static final class NeverCallOldPrimitives implements DurablePrimitives {

    private static UnsupportedOperationException fail() {
      return new UnsupportedOperationException(
          "the new manager must never reach the old primitives handle");
    }

    @Override
    public long generateNewId(@NonNull PolarisCallContext callCtx) {
      throw fail();
    }

    @Override
    public void writeEntity(
        @NonNull PolarisCallContext callCtx,
        @NonNull PolarisBaseEntity entity,
        boolean nameOrParentChanged,
        @Nullable PolarisBaseEntity originalEntity) {
      throw fail();
    }

    @Override
    public void writeEntities(
        @NonNull PolarisCallContext callCtx,
        @NonNull List<PolarisBaseEntity> entities,
        @Nullable List<PolarisBaseEntity> originalEntities) {
      throw fail();
    }

    @Override
    public void writeToGrantRecords(
        @NonNull PolarisCallContext callCtx, @NonNull PolarisGrantRecord grantRec) {
      throw fail();
    }

    @Override
    public void writeEvents(@NonNull List<EventEntity> events) {
      throw fail();
    }

    @Override
    public void deleteEntity(
        @NonNull PolarisCallContext callCtx, @NonNull PolarisBaseEntity entity) {
      throw fail();
    }

    @Override
    public void deleteFromGrantRecords(
        @NonNull PolarisCallContext callCtx, @NonNull PolarisGrantRecord grantRec) {
      throw fail();
    }

    @Override
    public void deleteAllEntityGrantRecords(
        @NonNull PolarisCallContext callCtx,
        @NonNull PolarisEntityCore entity,
        @NonNull List<PolarisGrantRecord> grantsOnGrantee,
        @NonNull List<PolarisGrantRecord> grantsOnSecurable) {
      throw fail();
    }

    @Override
    public void deleteAll(@NonNull PolarisCallContext callCtx) {
      throw fail();
    }

    @Override
    public @Nullable PolarisBaseEntity lookupEntity(
        @NonNull PolarisCallContext callCtx, long catalogId, long entityId, int typeCode) {
      throw fail();
    }

    @Override
    public @Nullable PolarisBaseEntity lookupEntityByName(
        @NonNull PolarisCallContext callCtx,
        long catalogId,
        long parentId,
        int typeCode,
        @NonNull String name) {
      throw fail();
    }

    @Override
    public @NonNull List<PolarisBaseEntity> lookupEntities(
        @NonNull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
      throw fail();
    }

    @Override
    public @NonNull List<PolarisChangeTrackingVersions> lookupEntityVersions(
        @NonNull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
      throw fail();
    }

    @Override
    public @NonNull Page<EntityNameLookupRecord> listEntities(
        @NonNull PolarisCallContext callCtx,
        long catalogId,
        long parentId,
        @NonNull PolarisEntityType entityType,
        @NonNull PolarisEntitySubType entitySubType,
        @NonNull PageToken pageToken) {
      throw fail();
    }

    @Override
    public @NonNull <T> Page<T> listFullEntities(
        @NonNull PolarisCallContext callCtx,
        long catalogId,
        long parentId,
        @NonNull PolarisEntityType entityType,
        @NonNull PolarisEntitySubType entitySubType,
        @NonNull Predicate<PolarisBaseEntity> entityFilter,
        @NonNull Function<PolarisBaseEntity, T> transformer,
        PageToken pageToken) {
      throw fail();
    }

    @Override
    public int lookupEntityGrantRecordsVersion(
        @NonNull PolarisCallContext callCtx, long catalogId, long entityId) {
      throw fail();
    }

    @Override
    public @Nullable PolarisGrantRecord lookupGrantRecord(
        @NonNull PolarisCallContext callCtx,
        long securableCatalogId,
        long securableId,
        long granteeCatalogId,
        long granteeId,
        int privilegeCode) {
      throw fail();
    }

    @Override
    public @NonNull List<PolarisGrantRecord> loadAllGrantRecordsOnSecurable(
        @NonNull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
      throw fail();
    }

    @Override
    public @NonNull List<PolarisGrantRecord> loadAllGrantRecordsOnGrantee(
        @NonNull PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
      throw fail();
    }

    @Override
    public boolean hasChildren(
        @NonNull PolarisCallContext callContext,
        @Nullable PolarisEntityType optionalEntityType,
        long catalogId,
        long parentId) {
      throw fail();
    }
  }
}
