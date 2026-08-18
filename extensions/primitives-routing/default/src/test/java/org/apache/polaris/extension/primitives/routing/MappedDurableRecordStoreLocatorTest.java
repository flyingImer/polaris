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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.persistence.treemap.TreeMapDurableRecordStore;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.RecordKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The locator's own mechanics: mapped resolution, the no-fallback rejection, the construction-time
 * refusal, and the snapshot property. The end-to-end cases — a split assembly serving through one
 * orchestrator over one primitives handle, a remap moving a kind, both with real JDBC + in-memory
 * stores — live in {@link RoutingAssembledTwoStoreTest}.
 */
class MappedDurableRecordStoreLocatorTest {

  private static final PolarisDiagnostics DIAGNOSTICS = new PolarisDefaultDiagServiceImpl();
  private static final String MAIN = "main";
  private static final String AUTHZ = "authz";

  private TreeMapDurableRecordStore mainStore;
  private TreeMapDurableRecordStore authzStore;

  @BeforeEach
  void setUp() {
    mainStore = new TreeMapDurableRecordStore(DIAGNOSTICS);
    authzStore = new TreeMapDurableRecordStore(DIAGNOSTICS);
  }

  private Map<String, DurableRecordStore> bothStores() {
    return Map.of(MAIN, mainStore, AUTHZ, authzStore);
  }

  @Test
  void aMappedKindResolvesToItsNamedStore() {
    MappedDurableRecordStoreLocator locator =
        new MappedDurableRecordStoreLocator(
            Map.of(PolarisRecordKinds.ENTITY, MAIN, PolarisRecordKinds.GRANT_RECORD, AUTHZ),
            bothStores());

    assertThat(locator.forKind(PolarisRecordKinds.ENTITY)).isSameAs(mainStore);
    assertThat(locator.forKind(PolarisRecordKinds.GRANT_RECORD)).isSameAs(authzStore);
  }

  @Test
  void anUnmappedKindIsRejectedNamingTheKind() {
    // No fallback, no default store: an unmapped kind is a configuration error, never a guess.
    MappedDurableRecordStoreLocator locator =
        new MappedDurableRecordStoreLocator(Map.of(PolarisRecordKinds.ENTITY, MAIN), bothStores());

    assertThatThrownBy(() -> locator.forKind(PolarisRecordKinds.GRANT_RECORD))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(PolarisRecordKinds.GRANT_RECORD.id());
  }

  @Test
  void aDanglingStoreNameRefusesConstructionNamingKindAndName() {
    assertThatThrownBy(
            () ->
                new MappedDurableRecordStoreLocator(
                    Map.of(PolarisRecordKinds.GRANT_RECORD, AUTHZ), Map.of(MAIN, mainStore)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(AUTHZ)
        .hasMessageContaining(PolarisRecordKinds.GRANT_RECORD.id());
  }

  @Test
  void theLocatorIsASnapshotOfTheMapping() {
    Map<RecordKind, String> config = new HashMap<>();
    config.put(PolarisRecordKinds.GRANT_RECORD, MAIN);
    MappedDurableRecordStoreLocator locator =
        new MappedDurableRecordStoreLocator(config, bothStores());

    // Editing the caller's own map after construction re-routes nothing: construction, not
    // configuration. Remapping is a new locator.
    config.put(PolarisRecordKinds.GRANT_RECORD, AUTHZ);
    assertThat(locator.forKind(PolarisRecordKinds.GRANT_RECORD)).isSameAs(mainStore);
  }
}
