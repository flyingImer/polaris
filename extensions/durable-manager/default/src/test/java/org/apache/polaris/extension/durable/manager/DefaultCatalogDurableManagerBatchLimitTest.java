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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.exceptions.TooManyItemsException;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.extension.orchestration.DefaultDurableOrchestrator;
import org.apache.polaris.extension.primitives.routing.MappedDurableRecordStoreLocator;
import org.apache.polaris.extension.primitives.routing.RoutingDurableRecordStore;
import org.apache.polaris.persistence.treemap.TreeMapDurableRecordStore;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.LookupPath;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.Read;
import org.apache.polaris.spi.durable.RecordKind;
import org.apache.polaris.spi.durable.RecordRef;
import org.apache.polaris.spi.durable.RecordVersions;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

/**
 * A batch the store refuses as larger than its per-commit limit is permanent, so the batch update
 * throws instead of returning the lost-race status. The two are different instructions to the
 * caller: a lost race says re-read and retry, while an oversized list says split it. Collapsing
 * them makes a client retry the identical request forever.
 *
 * <p>Assembled like {@link DefaultCatalogDurableManagerUpdateRaceTest}: routing over one store, one
 * orchestrator, one manager. Both entities are created through a manager over the bare store, so
 * only the batch update meets the refusing decorator.
 */
class DefaultCatalogDurableManagerBatchLimitTest {

  private static final PolarisCallContext CALL_CTX =
      new PolarisCallContext(() -> "testRealm", new NeverCallOldPrimitives());

  private static DefaultCatalogDurableManager managerOver(DurableRecordStore store) {
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
    return new DefaultCatalogDurableManager(
        Clock.systemUTC(),
        new PolarisDefaultDiagServiceImpl(),
        new DefaultDurableOrchestrator(primitives),
        primitives);
  }

  private static PolarisBaseEntity newCatalog(DefaultCatalogDurableManager manager, String name) {
    return new PolarisBaseEntity.Builder()
        .catalogId(PolarisEntityConstants.getNullId())
        .id(manager.generateNewEntityId(CALL_CTX).getId())
        .typeCode(PolarisEntityType.CATALOG.getCode())
        .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
        .parentId(PolarisEntityConstants.getRootEntityId())
        .name(name)
        .propertiesAsMap(Map.of())
        .internalPropertiesAsMap(Map.of())
        .build();
  }

  private static PolarisBaseEntity created(
      DefaultCatalogDurableManager manager, PolarisBaseEntity entity) {
    EntityResult result = manager.createEntityIfNotExists(CALL_CTX, null, entity);
    assertThat(result.isSuccess()).isTrue();
    return result.getEntity();
  }

  private static PolarisBaseEntity withProperty(PolarisBaseEntity entity, String value) {
    return new PolarisBaseEntity.Builder(entity).propertiesAsMap(Map.of("batched", value)).build();
  }

  private static PolarisBaseEntity readBack(DurableRecordStore store, long id) {
    return store
        .get(RecordRef.byIdentity(PolarisRecordKinds.ENTITY, List.of(id)), PolarisBaseEntity.class)
        .orElseThrow();
  }

  @Test
  void aBatchRefusedAsTooLargeThrowsRatherThanReportingALostRace() {
    TreeMapDurableRecordStore base =
        new TreeMapDurableRecordStore(new PolarisDefaultDiagServiceImpl());
    DefaultCatalogDurableManager plain = managerOver(base);
    PolarisBaseEntity first = created(plain, newCatalog(plain, "batch-limit-a"));
    PolarisBaseEntity second = created(plain, newCatalog(plain, "batch-limit-b"));

    Throwable thrown =
        catchThrowable(
            () ->
                managerOver(new RefusesEveryCommitAsTooLarge(base))
                    .updateEntitiesPropertiesIfNotChanged(
                        CALL_CTX,
                        List.of(
                            new EntityWithPath(List.of(), withProperty(first, "one")),
                            new EntityWithPath(List.of(), withProperty(second, "two")))));

    assertThat(thrown).isInstanceOf(TooManyItemsException.class);
    assertThat(((TooManyItemsException) thrown).errorCode()).isEqualTo("commit.too_many_items");

    // Nothing was applied: neither row carries the batch's own change.
    assertThat(readBack(base, first.getId()).getPropertiesAsMap()).doesNotContainKey("batched");
    assertThat(readBack(base, second.getId()).getPropertiesAsMap()).doesNotContainKey("batched");
  }

  /**
   * Forwards every read to the real store so the manager's pre-check sees live rows, and reports
   * each commit as exceeding the per-commit limit. In the style of the shared preempting store: one
   * intercepted method, everything else forwarded 1:1.
   */
  private record RefusesEveryCommitAsTooLarge(DurableRecordStore delegate)
      implements DurableRecordStore {

    @Override
    public @NonNull CommitResult commit(@NonNull List<Mutation> mutations) {
      return CommitResult.tooManyItems();
    }

    @Override
    public long generateNewId() {
      return delegate.generateNewId();
    }

    @Override
    public @NonNull <T> Optional<T> get(@NonNull RecordRef ref, @NonNull Class<T> type) {
      return delegate.get(ref, type);
    }

    @Override
    public @NonNull <T> Optional<Read<T>> read(@NonNull RecordRef ref, @NonNull Class<T> type) {
      return delegate.read(ref, type);
    }

    @Override
    public @NonNull <T> List<Optional<T>> getMany(
        @NonNull List<RecordRef> refs, @NonNull Class<T> type) {
      return delegate.getMany(refs, type);
    }

    @Override
    public @NonNull <T> Page<T> list(
        @NonNull RecordKind kind,
        @NonNull LookupPath path,
        @NonNull List<Object> anchors,
        @NonNull PageToken pageToken,
        @NonNull Class<T> type) {
      return delegate.list(kind, path, anchors, pageToken, type);
    }

    @Override
    public @NonNull <T> Page<T> list(
        @NonNull LookupPath path,
        @NonNull List<Object> anchors,
        @NonNull PageToken pageToken,
        @NonNull Class<T> type) {
      return delegate.list(path, anchors, pageToken, type);
    }

    @Override
    public @NonNull List<Optional<RecordVersions>> versionsOf(@NonNull List<RecordRef> refs) {
      return delegate.versionsOf(refs);
    }

    @Override
    public @NonNull Object domainOf(@NonNull RecordRef target) {
      return delegate.domainOf(target);
    }

    @Override
    public int maxItemsPerCommit() {
      return delegate.maxItemsPerCommit();
    }
  }
}
