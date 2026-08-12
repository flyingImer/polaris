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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.persistence.treemap.TreeMapDurableRecordStore;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.RecordKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The factory's own mechanics: default-entry resolution, the assembly-time refusal, and the
 * snapshot property. The end-to-end gate cases — a split assembly serving through one orchestrator,
 * a remap moving a kind, both with real JDBC + in-memory stores — live in {@link
 * FactoryAssembledTwoStoreTest}.
 */
class DefaultDurableRecordStoreFactoryTest {

  private static final PolarisDiagnostics DIAGNOSTICS = new PolarisDefaultDiagServiceImpl();
  private static final String MAIN = "main";
  private static final String AUTHZ = "authz";

  private TreeMapDurableRecordStore mainStore;
  private TreeMapDurableRecordStore authzStore;
  private DefaultDurableRecordStoreFactory factory;

  @BeforeEach
  void setUp() {
    mainStore = new TreeMapDurableRecordStore(DIAGNOSTICS);
    authzStore = new TreeMapDurableRecordStore(DIAGNOSTICS);
    factory = new DefaultDurableRecordStoreFactory();
  }

  private Map<String, DurableRecordStore> bothStores() {
    return Map.of(MAIN, mainStore, AUTHZ, authzStore);
  }

  @Test
  void anUnmappedKindResolvesToTheDefaultStore() {
    Function<RecordKind, DurableRecordStore> assembly =
        factory.produce(Map.of(PolarisRecordKinds.GRANT_RECORD, AUTHZ), MAIN, bothStores());

    // ENTITY has no config entry: it lands in the default store, so adding a kind costs nothing
    // at the factory. The mapped kind resolves to its named store.
    assertThat(assembly.apply(PolarisRecordKinds.ENTITY)).isSameAs(mainStore);
    assertThat(assembly.apply(PolarisRecordKinds.GRANT_RECORD)).isSameAs(authzStore);
  }

  @Test
  void aDanglingStoreNameRefusesNamingKindAndName() {
    assertThatThrownBy(
            () ->
                factory.produce(
                    Map.of(PolarisRecordKinds.GRANT_RECORD, AUTHZ), MAIN, Map.of(MAIN, mainStore)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(AUTHZ)
        .hasMessageContaining(PolarisRecordKinds.GRANT_RECORD.id());
  }

  @Test
  void aDanglingDefaultStoreNameRefuses() {
    assertThatThrownBy(() -> factory.produce(Map.of(), MAIN, Map.of(AUTHZ, authzStore)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(MAIN);
  }

  @Test
  void theAssemblyIsASnapshotOfTheMapping() {
    Map<RecordKind, String> config = new HashMap<>();
    Function<RecordKind, DurableRecordStore> assembly = factory.produce(config, MAIN, bothStores());

    // Editing the caller's own map after produce re-routes nothing: construction, not
    // configuration. Remapping is a new produce call yielding a new assembly.
    config.put(PolarisRecordKinds.GRANT_RECORD, AUTHZ);
    assertThat(assembly.apply(PolarisRecordKinds.GRANT_RECORD)).isSameAs(mainStore);
  }
}
