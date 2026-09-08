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

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.persistence.PolarisObjectMapperUtil;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.spi.durable.DurableOrchestrator;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.OrchestrationResult;
import org.apache.polaris.spi.durable.RecordRef;
import org.apache.polaris.spi.durable.TaskDurableManager;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Default task durable manager: loads the tasks an executor may run and records the lease on each.
 * The lease window is this manager's rule; the lease itself is a version-checked update of the task
 * entity, kept here as a private copy of the entity write so this manager depends on no other
 * manager.
 *
 * <p>Two doors, strictly divided: every write goes through the orchestrator's commit, every read
 * goes to the primitives handle directly. Owns its business rules and knows no storage topology;
 * authorization and request validation live above this layer.
 */
public class DefaultTaskDurableManager implements TaskDurableManager {
  private final Clock clock;
  private final PolarisDiagnostics diagnostics;
  private final DurableOrchestrator orchestrator;
  private final DurableRecordStore primitives;

  public DefaultTaskDurableManager(
      @NonNull Clock clock,
      @NonNull PolarisDiagnostics diagnostics,
      @NonNull DurableOrchestrator orchestrator,
      @NonNull DurableRecordStore primitives) {
    this.clock = clock;
    this.diagnostics = diagnostics;
    this.orchestrator = orchestrator;
    this.primitives = primitives;
  }

  /**
   * Ported from both old impls' {@code loadTasks}, whose availability predicate is verbatim
   * identical in the two: a TASK under root is leasable when its parsed state is null (never
   * attempted, or unparseable — {@code parseTaskState} logs and returns null on bad JSON), its
   * executor is null, or its last attempt is older than {@code POLARIS_TASK_TIMEOUT_MILLIS} (realm
   * config, default 300s) against the INJECTED clock. Taking a lease stamps {@code
   * lastAttemptExecutorId}/{@code lastAttemptStartTime}/{@code attemptCount} and persists through
   * {@link #updateEntityPropertiesIfNotChanged}'s version CAS, exactly as both old impls do.
   *
   * <p>The read is {@code RecordRefs#listChildEntities} over root (the same in-memory entity-type
   * narrowing that method already discloses), with the availability predicate evaluated HERE and
   * the page limit applied AFTER it — matching the old primitives' predicate-then-limit order (the
   * fixture's second limit-5 call must return the NEXT five unleased tasks, not an empty page of
   * already-leased ones). The old interface pushed this predicate INTO the store as a callback; the
   * new SPI's own javadoc records task leasing as a missing operation rather than a filter to
   * relocate, and reshaping it is the read-side record's noted follow-up, not this ticket's — so
   * the whole candidate set crosses to the manager and is filtered in memory, the disclosed interim
   * cost. Part of the same interim shape: the caller's continuation CURSOR, if its page token ever
   * carried one, is not honored — only the page SIZE is read (the old impls thread the whole token
   * into the store scan). No caller in the tree passes a continuation-bearing token, and loadTasks
   * never returns one to chain from (old and new both return a token-less {@code Page.fromItems}),
   * so the gap has no live trigger; named by this ticket's refute pass, owned by the same read-side
   * follow-up.
   *
   * <p><b>Disclosed old-impl divergence, Atomic's form matched:</b> individual failed leases are
   * skipped, and only a batch where EVERY attempted lease failed throws {@link
   * RetryOnConcurrencyException} ({@code AtomicOperationMetaStoreManager}'s partial-success form,
   * which one-commit-per-lease natively is). {@code TransactionalMetaStoreManagerImpl} instead
   * rolls its whole batch back and throws on the FIRST failed lease; that all-or-nothing form has
   * no counterpart here because each lease is its own commit. The fixture accepts either (its
   * parallel executors catch the exception and retry; exactly-once claiming rests on the CAS, not
   * on the batch shape).
   */
  @Override
  public @NonNull EntitiesResult loadTasks(
      @NonNull PolarisCallContext callCtx, String executorId, PageToken pageToken) {
    long taskAgeTimeout =
        callCtx.getRealmConfig().getConfig(FeatureConfiguration.POLARIS_TASK_TIMEOUT_MILLIS);
    List<PolarisBaseEntity> availableTasks =
        RecordRefs.listChildEntities(
                primitives,
                null,
                PolarisEntityType.TASK,
                PolarisEntitySubType.ANY_SUBTYPE,
                PageToken.readEverything())
            .stream()
            .filter(
                entity -> {
                  PolarisObjectMapperUtil.TaskExecutionState taskState =
                      PolarisObjectMapperUtil.parseTaskState(entity);
                  return taskState == null
                      || taskState.executor == null
                      || clock.millis() - taskState.lastAttemptStartTime > taskAgeTimeout;
                })
            .limit(
                pageToken.pageSize().isPresent() ? pageToken.pageSize().getAsInt() : Long.MAX_VALUE)
            .toList();

    int failedLeaseCount = 0;
    List<PolarisBaseEntity> loadedTasks = new ArrayList<>(availableTasks.size());
    for (PolarisBaseEntity task : availableTasks) {
      PolarisBaseEntity.Builder updatedTaskBuilder = new PolarisBaseEntity.Builder(task);
      Map<String, String> properties = task.getPropertiesAsMap();
      properties.put(PolarisTaskConstants.LAST_ATTEMPT_EXECUTOR_ID, executorId);
      properties.put(PolarisTaskConstants.LAST_ATTEMPT_START_TIME, String.valueOf(clock.millis()));
      properties.put(
          PolarisTaskConstants.ATTEMPT_COUNT,
          String.valueOf(
              Integer.parseInt(properties.getOrDefault(PolarisTaskConstants.ATTEMPT_COUNT, "0"))
                  + 1));
      updatedTaskBuilder.propertiesAsMap(properties);
      EntityResult result =
          updateEntityPropertiesIfNotChanged(callCtx, null, updatedTaskBuilder.build());
      if (result.getReturnStatus() == BaseResult.ReturnStatus.SUCCESS) {
        loadedTasks.add(result.getEntity());
      } else {
        failedLeaseCount++;
      }
    }
    if (loadedTasks.isEmpty() && failedLeaseCount > 0) {
      throw new RetryOnConcurrencyException(
          "Failed to lease any of %s tasks due to concurrent leases", failedLeaseCount);
    }
    return EntitiesResult.fromPage(Page.fromItems(loadedTasks));
  }

  /**
   * The version-checked entity update the catalog manager offers publicly, kept as a private copy
   * so leasing a task depends on no other manager.
   */
  private @NonNull EntityResult updateEntityPropertiesIfNotChanged(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entity) {
    diagnostics.checkNotNull(entity, "unexpected_null_entity");

    RecordRef ref = RecordRefs.entityIdentity(entity.getId());
    Optional<PolarisBaseEntity> current = primitives.get(ref, PolarisBaseEntity.class);
    if (current.isEmpty()
        || current.get().getEntityVersion() != entity.getEntityVersion()
        || current.get().getGrantRecordsVersion() != entity.getGrantRecordsVersion()) {
      return new EntityResult(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, null);
    }
    PolarisBaseEntity currentEntity = current.get();

    PolarisBaseEntity updated =
        new PolarisBaseEntity.Builder(currentEntity)
            .properties(entity.getProperties())
            .internalProperties(entity.getInternalProperties())
            .entityVersion(currentEntity.getEntityVersion() + 1)
            // System.currentTimeMillis(), not clock.millis(): PolarisBaseEntity.Builder#build()'s
            // own createTimestamp default is real wall-clock time, decoupled from any injected
            // clock, and the fixture's testStartTime is captured the same way. This class's clock
            // field is real in production; the fixture's own MutableClock is fixed at construction
            // and only advances via explicit clock.add(...) (for the task-leasing tests) —
            // using it here made every update's timestamp read as BEFORE the entity's own
            // real-time createTimestamp. Found by testUpdateEntities/testRename failing on exactly
            // that ordering.
            .lastUpdateTimestamp(System.currentTimeMillis())
            .build();

    OrchestrationResult result =
        orchestrator.commit(
            List.of(RecordMutations.entityPropertiesUpdateMutation(ref, currentEntity, updated)));
    return result.isApplied()
        ? new EntityResult(updated)
        : new EntityResult(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, null);
  }
}
