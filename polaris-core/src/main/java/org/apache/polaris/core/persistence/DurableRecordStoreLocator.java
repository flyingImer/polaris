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

import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.RecordKind;
import org.jspecify.annotations.NonNull;

/**
 * The request-time seam between the code that knows WHICH record kind is being touched and the
 * implementations that know HOW records are held: resolves a record kind to the {@link
 * DurableRecordStore} holding it.
 *
 * <p><b>A Substrate non-SPI contract, deliberately outside the {@code spi.durable} package.</b>
 * This is an internal coordination seam, not a provider extension point: a provider that wants a
 * different routing mechanism implements the primitives SPI itself — routing lives BEHIND that SPI,
 * held privately by the routing implementation, and nothing above the primitives layer can reach a
 * locator. The classification follows the credential-vending precedent ({@code
 * CredentialVendingCoordinator}, an interface sited in core rather than an spi package).
 *
 * <h2>No fallback, no default store</h2>
 *
 * <p>A kind this locator holds no mapping for is rejected by throwing, with the kind named in the
 * message — at construction-time validation where the mapping itself is malformed, or at {@link
 * #forKind} where the kind is simply unmapped. {@link org.apache.polaris.spi.durable.RecordKind}'s
 * javadoc is the authority: a store "does not guess", and the two-level {@code kind → store name →
 * implementation} mapping exists to make an unknown kind a configuration error rather than a
 * runtime surprise. A default store is a guess — it converts a boot-time configuration error into a
 * runtime write into the wrong store. (History note, 2026-08-18: the dissolved factory's {@code
 * defaultStoreName} parameter was overturned as a defect on exactly this derivation, not migrated.)
 *
 * <h2>Where the factory went</h2>
 *
 * <p>(History note, 2026-08-18: this contract absorbs the dissolved {@code
 * DurableRecordStoreFactory} pair. The factory's product — a bare {@code Function<RecordKind,
 * DurableRecordStore>} handed to orchestration — was overturned because the bare function IS a
 * routing table: whoever holds it holds kind-to-store routing plus a reachable {@code domainOf}
 * comparison, which is the co-location branching ability the durable-boundary record corrects
 * repeatedly. The factory's three jobs — copy the mapping, refuse dangling store names, resolve
 * kind to store — became this contract's implementations' constructor and this single method. The
 * two-level mapping itself survives unchanged, as configuration; the routing implementation of the
 * primitives SPI holds the locator privately, so manager and orchestration hold one primitives
 * handle and never learn that stores exist.)
 *
 * <h2>What validation deliberately does not include, carried forward so neither returns</h2>
 *
 * <ul>
 *   <li>Whether a wired implementation actually holds a mapper for each kind resolved to it. The
 *       primitives contract carries no kind-enumeration declaration, so that check could only be a
 *       probe, and validation is built from declarations alone. A store that receives a kind it has
 *       no mapper for rejects the call itself.
 *   <li>A co-atomicity guard (kind groups declared to require one store name). (History note: built
 *       inside the dissolved factory, then removed by decision 2026-08-12; the record moves here
 *       with the validation it scoped, deliberately, so the question does not return.) The contract
 *       never promises that two kinds commit together — co-location is a deployment fact, not a
 *       contract fact — so no conformant caller can depend on a split being refused; and a provider
 *       whose operations genuinely need a wider atomic boundary exposes that boundary as ONE
 *       logical store, which orchestration natively treats as one domain, leaving the guard nothing
 *       to protect. The guard's originally agreed form predated the decision that placed {@code
 *       createCatalog}'s grants outside the hard atomicity guarantee, which removed its only
 *       prospective declarant.
 * </ul>
 *
 * <p>Temporary name, migration note: this interface is named after {@link DurableRecordStore} and
 * is renamed with it at the contract step ({@code DurablePrimitives} + {@code
 * DurablePrimitivesLocator}, the recorded pairing), when the old model is deleted and the temporary
 * names retire.
 */
public interface DurableRecordStoreLocator {

  /**
   * Resolve the store holding the given record kind.
   *
   * @param kind the record kind being touched
   * @return the implementation holding that kind; never null
   * @throws IllegalArgumentException if no store holds the kind; the message names the kind
   */
  @NonNull DurableRecordStore forKind(@NonNull RecordKind kind);
}
