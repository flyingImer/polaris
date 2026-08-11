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
package org.apache.polaris.spi.durable;

import java.util.List;
import org.jspecify.annotations.NonNull;

/**
 * The orchestration SPI: takes a mutation list that may span record kinds and, in a multi-store
 * assembly, stores; commits it per the guarantee below or compensates.
 *
 * <p>This is the second of the four durable layers, between the durable managers and the
 * primitives: {@code durable <domain> manager} (business-aware) → orchestration → durable
 * primitives (business-neutral). A manager states a business outcome and hands the mutations here;
 * this layer alone knows storage topology, and the layer below sees only same-domain groups.
 *
 * <h2>The guarantee</h2>
 *
 * <p>The list is grouped by atomicity domain and committed one group at a time:
 *
 * <ul>
 *   <li><b>Grouping.</b> A mutation's domain is the union of its write target and every record its
 *       preconditions reference. Mutations whose domains are equal form one group; each group is
 *       handed to exactly one primitives implementation as exactly one {@link
 *       DurableRecordStore#commit} call. There is no form in which one group reaches two
 *       implementations, so a domain never spans logical stores.
 *   <li><b>Within a group: atomic.</b> The group applies entirely or not at all, unconditionally —
 *       that is the primitives contract this layer builds on.
 *   <li><b>Across groups: ordered, compensated.</b> Groups commit in the order their first mutation
 *       appears in the list; the caller's list order is the write order. If a group fails, every
 *       group already committed is rolled back synchronously, on this same call, in reverse commit
 *       order. An ordinary failure leaves nothing behind, and a retry simply succeeds.
 *   <li><b>The crash window, disclosed rather than hidden.</b> If the process dies after a group
 *       commits and before the rollback completes, the partial state survives with no compensation
 *       having run. Every such state is inert and reclaimable through normal admin paths; nothing
 *       here pretends cross-domain all-or-nothing, which would need commit-logging and is a post-GA
 *       enhancement.
 * </ul>
 *
 * <p><b>Orchestration is always organized as N domains; N=1 is not a special case and gets no
 * separate code path.</b> Co-location is a deployment fact, not a contract fact: the first
 * integrator who moves one record kind to a different store must find orchestration already
 * structured for it, not a pass-through that breaks. A single-domain list simply produces one
 * group.
 *
 * <h2>What a caller may not ask for</h2>
 *
 * <p><b>No interactive transactions.</b> Reads happen before this call and ride in as each
 * mutation's preconditions. A stale assumption surfaces as a failed precondition on the commit,
 * never as a silent overwrite.
 *
 * <p><b>A single mutation cannot span domains.</b> A precondition must be evaluated atomically with
 * the write it gates; if the condition's record and the write's record live in different domains,
 * no backend can do both at once, and compensation operates across groups, never inside one
 * mutation. Such a mutation is a caller error and is rejected before anything commits. The caller
 * restructures: consult the remote record with a read and accept its staleness, or co-locate the
 * kinds. (Derived here from the recorded co-location rules; recorded in this javadoc rather than
 * applied silently.)
 *
 * <h2>Construction, not configuration</h2>
 *
 * <p>An instance is a realm-scoped assembly: it is constructed knowing which primitives
 * implementation holds each record kind, and requests never carry routing information. The
 * kind-to-store mapping machinery is the factory's business, on the construction axis; requests do
 * not pass through it. Like the primitives, no method takes a call context or realm parameter.
 */
public interface DurableOrchestrator {

  /**
   * Commit the list per the class guarantee: grouped by domain, atomic within a group, ordered and
   * compensated across groups.
   *
   * <p>The returned {@link OrchestrationResult} distinguishes the three outcomes a caller must be
   * able to tell apart: everything applied; a group failed and everything already committed was
   * rolled back; a group failed and the rollback itself did not complete, leaving partial state for
   * admin reclamation.
   *
   * <p>If a store throws rather than reporting failure, the same compensation runs and the
   * exception then propagates; when that rollback is itself incomplete, the propagated exception
   * carries a suppressed exception naming what remains, so the disclosure survives the error path.
   *
   * @throws IllegalArgumentException if a mutation references a record kind this assembly holds no
   *     store for, or if a single mutation's write target and condition targets do not share one
   *     atomicity domain
   */
  @NonNull OrchestrationResult commit(@NonNull List<Mutation> mutations);
}
