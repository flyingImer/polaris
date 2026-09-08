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
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableOrchestrator;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.EventDurableManager;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.OrchestrationResult;
import org.apache.polaris.spi.durable.Precondition;
import org.jspecify.annotations.NonNull;

/**
 * Default event durable manager: appends events through the orchestrator in chunks no larger than
 * the primitives' per-commit cap. Events are append-only and carry no cross-row atomicity promise.
 *
 * <p>Two doors, strictly divided: every write goes through the orchestrator's commit, every read
 * goes to the primitives handle directly. Owns its business rules and knows no storage topology;
 * authorization and request validation live above this layer.
 */
public class DefaultEventDurableManager implements EventDurableManager {
  private final DurableOrchestrator orchestrator;
  private final DurableRecordStore primitives;

  public DefaultEventDurableManager(
      @NonNull DurableOrchestrator orchestrator, @NonNull DurableRecordStore primitives) {
    this.orchestrator = orchestrator;
    this.primitives = primitives;
  }

  /**
   * Overridden rather than left as the interface default: the default reaches {@link
   * PolarisCallContext#getMetaStore()}, the old primitives handle this class must never touch.
   *
   * <p>Neither old manager overrides this — the interface default is one unconditional {@code
   * ms.writeEvents(events)} with no manager-level catch, so the manager layer adds no policy and
   * the store decides: old JDBC batch-inserts, old TreeMap throws {@code
   * UnsupportedOperationException} (events are optional — data model 4.5; the one production
   * caller, the in-memory buffer listener, retries then logs and drops). Same contract here:
   * append-only {@code CREATE} mutations through the orchestrator, no catch — a store that does not
   * serve the events kind rejects loudly (the routing implementation names the kind), which IS the
   * documented refusal; on this branch both new-model stores serve it, so both fixture bindings are
   * functional where the old TreeMap pairing threw.
   *
   * <p>Two disclosed shape notes with no old-status vocabulary to map onto (the method is void): a
   * batch larger than {@link DurableRecordStore#maxItemsPerCommit} is CHUNKED into consecutive
   * commits — events are independent append-only rows with no cross-event atomicity promise, so
   * S12's no-silent-splitting rule for promised-atomic batches does not bite, and the only
   * difference from old JDBC's single INSERT transaction is the crash window between chunks; and a
   * non-applied commit (e.g. a duplicate event id — old JDBC propagates the raw uniqueness
   * violation there) surfaces as an unchecked exception naming the failure, preserving
   * failure-is-loud.
   */
  @Override
  public void writeEvents(
      @NonNull PolarisCallContext callCtx, @NonNull List<EventEntity> polarisEvents) {
    int cap = primitives.maxItemsPerCommit();
    for (int from = 0; from < polarisEvents.size(); from += cap) {
      List<EventEntity> chunk =
          polarisEvents.subList(from, Math.min(from + cap, polarisEvents.size()));
      List<Mutation> mutations =
          chunk.stream()
              .map(
                  event ->
                      Mutation.of(
                          PolarisRecordKinds.EVENT,
                          Mutation.Op.CREATE,
                          RecordRefs.eventIdentity(event),
                          event,
                          List.of(Precondition.none())))
              .toList();
      OrchestrationResult result = orchestrator.commit(mutations);
      if (!result.isApplied()) {
        throw new IllegalStateException(
            "writeEvents commit not applied: "
                + result
                    .groupFailure()
                    .flatMap(CommitResult::failure)
                    .map(Enum::toString)
                    .orElse(result.outcome().toString()));
      }
    }
  }
}
