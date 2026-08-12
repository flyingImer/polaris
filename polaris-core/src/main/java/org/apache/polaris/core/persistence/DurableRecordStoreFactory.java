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

import java.util.Map;
import java.util.function.Function;
import org.apache.polaris.spi.durable.DurableOrchestrator;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.RecordKind;
import org.jspecify.annotations.NonNull;

/**
 * The primitives factory: the construction-axis contract that turns the two-level {@code kind →
 * store name → implementation} mapping into the realm-scoped assembly {@link DurableOrchestrator}
 * consumes, refusing at assembly time a mapping that references stores nobody wired.
 *
 * <p><b>A construction-axis non-SPI contract, deliberately outside the {@code spi.durable}
 * package.</b> The factory is pluggable but it is not a layer: nothing flows through it — requests
 * flow manager → orchestration → primitives with the factory nowhere in the path; it is consulted
 * the way a routing table is built. Its classification follows the credential-vending precedent
 * ({@code CredentialVendingCoordinator}, an interface sited in core rather than an spi package),
 * the pairing this type was named after. This file's first placement was {@code spi.durable}; moved
 * by decision 2026-08-12. The type's final fate and signature remain the factory-fate question's to
 * settle; this contract carries only what assembly needs.
 *
 * <h2>The two-level mapping</h2>
 *
 * <p>Placement and affinity are different statements, and a one-level {@code kind → impl} map can
 * only make the first. Mapping through a store NAME makes affinity expressible: <b>mapping two
 * kinds to the same store name IS the statement that they are co-located</b>, and co-location is
 * what atomicity depends on — an atomicity domain is contained within one logical store, so a
 * deployment that splits two kinds across store names has chosen per-store replaceability and given
 * up their co-atomicity in the same edit. The mapping states that trade explicitly rather than
 * hiding it.
 *
 * <p>An integrator moves one record kind to a different backend by editing that kind's store-name
 * entry and nothing else. A kind with no entry resolves to the <b>default store name</b>, which is
 * what makes adding a record kind cost nothing here: the new kind lands in the default store and
 * the mapping is untouched.
 *
 * <h2>Assembly-time validation refuses; it never warns</h2>
 *
 * <p>One check runs before anything is produced, a pure comparison over the mapping — no connection
 * is opened, no request is made, no implementation is probed: every store name the config half
 * references, the default included, resolves to an implementation in the wiring half. A dangling
 * name is a configuration error, caught here rather than at first use, and it refuses startup — a
 * warning is a legal degraded path, and a guarantee with a legal degraded path is not a guarantee.
 *
 * <p><b>What validation deliberately does not include, recorded so neither returns:</b>
 *
 * <ul>
 *   <li>Whether a wired implementation actually holds a mapper for each kind resolved to it. The
 *       primitives contract carries no kind-enumeration declaration, so that check could only be a
 *       probe, and validation is built from declarations alone. A store that receives a kind it has
 *       no mapper for rejects the call itself.
 *   <li>A co-atomicity guard (kind groups declared to require one store name). Built first, then
 *       removed by decision 2026-08-12: the contract never promises that two kinds commit together
 *       — co-location is a deployment fact, not a contract fact — so no conformant caller can
 *       depend on a split being refused; and a provider whose operations genuinely need a wider
 *       atomic boundary exposes that boundary as ONE logical store, which orchestration natively
 *       treats as one domain, leaving the guard nothing to protect. The guard's originally agreed
 *       form predated the decision that placed {@code createCatalog}'s grants outside the hard
 *       atomicity guarantee, which removed its only prospective declarant.
 * </ul>
 *
 * <h2>What is produced</h2>
 *
 * <p>The realm-scoped assembly, in the already-resolved form orchestration's construction consumes:
 * a total function from record kind to the store holding it. Realm never appears as a parameter —
 * the wired implementations are themselves realm-scoped at construction, so the assembly is
 * realm-scoped because its parts are, and one factory invocation assembles one realm.
 *
 * <p>Temporary name, migration note: this interface is named after {@link DurableRecordStore} and
 * is renamed with it at the contract step ({@code DurablePrimitives} + {@code
 * DurablePrimitivesFactory}, the recorded pairing), when the old model is deleted and the temporary
 * names retire.
 */
public interface DurableRecordStoreFactory {

  /**
   * Validate the mapping and produce the realm-scoped assembly.
   *
   * <p>Validation runs first and refuses by throwing; nothing is produced from a mapping that fails
   * it. The returned function is total: a kind with no config entry resolves through {@code
   * defaultStoreName}, so the function never returns null, and a kind genuinely unknown to the
   * deployment is rejected by the store it resolves to, which holds no mapper for it.
   *
   * @param kindToStoreName the config half; kinds absent here resolve to {@code defaultStoreName}
   * @param defaultStoreName the store name an unmapped kind resolves to
   * @param storeByName the wiring half: one realm-scoped implementation instance per store name
   * @return the assembly consumed by a {@link DurableOrchestrator} implementation's construction
   * @throws IllegalArgumentException if a referenced store name (the default included) has no
   *     wiring entry; the message names the offending kinds and store names
   */
  @NonNull Function<RecordKind, DurableRecordStore> produce(
      @NonNull Map<RecordKind, String> kindToStoreName,
      @NonNull String defaultStoreName,
      @NonNull Map<String, DurableRecordStore> storeByName);
}
