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
package org.apache.polaris.extension.primitives.routing;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.polaris.core.persistence.DurableRecordStoreLocator;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.RecordKind;
import org.jspecify.annotations.NonNull;

/**
 * The default {@link DurableRecordStoreLocator}: built from the two-level {@code kind → store name
 * → implementation} mapping, resolved once at construction, holding no other state.
 *
 * <p><b>The mapping is a snapshot.</b> Both maps are copied before anything is resolved, so a
 * caller's later edits to its own collections cannot re-route a locator that already exists —
 * construction, not configuration. Remapping a kind is a new locator, which is exactly the "edit
 * the mapping and nothing else" form the mapping exists for. Mapping two kinds to the same store
 * name IS the statement that they are co-located; a deployment that splits two kinds across store
 * names has chosen per-store replaceability and given up their co-atomicity in the same edit.
 *
 * <p><b>Construction refuses dangling store names; it never warns.</b> One check runs before
 * anything is resolved, a pure comparison over the mapping — no connection is opened, no
 * implementation is probed: every store name the config half references resolves to an
 * implementation in the wiring half. The refusal names every offending kind and store name in one
 * message, because the mapping is deployment configuration: the person reading it is fixing a
 * config file and should see the whole repair at once.
 *
 * <p><b>No fallback.</b> A kind with no config entry is not resolved to anything; {@link #forKind}
 * throws, naming the kind. See the contract's javadoc for the derivation and the history of the
 * default-store parameter this replaces.
 */
public class MappedDurableRecordStoreLocator implements DurableRecordStoreLocator {

  private final Map<RecordKind, DurableRecordStore> storeByKind;

  /**
   * @param kindToStoreName the config half: every kind this deployment holds, each naming its store
   * @param storeByName the wiring half: one realm-scoped implementation instance per store name
   * @throws IllegalArgumentException if a referenced store name has no wiring entry; the message
   *     names every offending kind and store name
   */
  public MappedDurableRecordStoreLocator(
      @NonNull Map<RecordKind, String> kindToStoreName,
      @NonNull Map<String, DurableRecordStore> storeByName) {
    Map<RecordKind, String> config = Map.copyOf(kindToStoreName);
    Map<String, DurableRecordStore> wiring = Map.copyOf(storeByName);

    refuseDanglingStoreNames(config, wiring);

    Map<RecordKind, DurableRecordStore> resolved = new HashMap<>();
    config.forEach((kind, storeName) -> resolved.put(kind, wiring.get(storeName)));
    this.storeByKind = Map.copyOf(resolved);
  }

  @Override
  public @NonNull DurableRecordStore forKind(@NonNull RecordKind kind) {
    DurableRecordStore store = storeByKind.get(kind);
    if (store == null) {
      throw new IllegalArgumentException(
          "No store holds record kind '"
              + kind.id()
              + "' in this mapping. An unmapped kind is a configuration error; there is no default"
              + " store to guess with.");
    }
    return store;
  }

  /**
   * Every store name the config half references must have a wiring entry. A dangling name is a
   * configuration error caught at construction, not at first use.
   */
  private static void refuseDanglingStoreNames(
      Map<RecordKind, String> config, Map<String, DurableRecordStore> wiring) {
    // Sorted so the refusal message is deterministic for the person fixing the config.
    Map<String, Set<String>> kindsByDanglingName = new TreeMap<>();
    config.forEach(
        (kind, storeName) -> {
          if (!wiring.containsKey(storeName)) {
            kindsByDanglingName.computeIfAbsent(storeName, n -> new TreeSet<>()).add(kind.id());
          }
        });
    if (!kindsByDanglingName.isEmpty()) {
      throw new IllegalArgumentException(
          "Mapping references store name(s) with no implementation wired: "
              + kindsByDanglingName
              + ". Every referenced store name must have a wiring entry.");
    }
  }
}
