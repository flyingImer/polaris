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
package org.apache.polaris.extension.primitives.factory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import org.apache.polaris.core.persistence.DurableRecordStoreFactory;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.RecordKind;
import org.jspecify.annotations.NonNull;

/**
 * The default implementation of {@link DurableRecordStoreFactory}: pure in-process mapping
 * resolution, holding no state of its own.
 *
 * <p><b>The assembly is a snapshot.</b> The maps handed to {@code produce} are copied before the
 * returned function is built, so a caller's later edits to its own collections cannot re-route an
 * assembly that already exists — construction, not configuration. Remapping a kind is a new {@code
 * produce} call yielding a new assembly, which is exactly the "edit the mapping and nothing else"
 * form the mapping exists for.
 *
 * <p>The validation failure names every offending kind and store name in one message rather than
 * failing on the first, because the mapping is deployment configuration: the person reading the
 * refusal is fixing a config file and should see the whole repair at once.
 */
public class DefaultDurableRecordStoreFactory implements DurableRecordStoreFactory {

  @Override
  public @NonNull Function<RecordKind, DurableRecordStore> produce(
      @NonNull Map<RecordKind, String> kindToStoreName,
      @NonNull String defaultStoreName,
      @NonNull Map<String, DurableRecordStore> storeByName) {

    Map<RecordKind, String> config = Map.copyOf(kindToStoreName);
    Map<String, DurableRecordStore> wiring = Map.copyOf(storeByName);

    refuseDanglingStoreNames(config, defaultStoreName, wiring);

    Map<RecordKind, DurableRecordStore> resolved = new HashMap<>();
    config.forEach((kind, storeName) -> resolved.put(kind, wiring.get(storeName)));
    DurableRecordStore defaultStore = wiring.get(defaultStoreName);
    Map<RecordKind, DurableRecordStore> snapshot = Map.copyOf(resolved);
    return kind -> snapshot.getOrDefault(kind, defaultStore);
  }

  /**
   * Every store name the config half references, the default included, must have a wiring entry. A
   * dangling name is a configuration error caught at assembly time, not at first use.
   */
  private static void refuseDanglingStoreNames(
      Map<RecordKind, String> config,
      String defaultStoreName,
      Map<String, DurableRecordStore> wiring) {
    // Sorted so the refusal message is deterministic for the person fixing the config.
    Map<String, Set<String>> kindsByDanglingName = new TreeMap<>();
    if (!wiring.containsKey(defaultStoreName)) {
      kindsByDanglingName.put(defaultStoreName, new TreeSet<>(Set.of("(default)")));
    }
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
              + ". Every referenced store name, the default included, must have a wiring entry.");
    }
  }
}
