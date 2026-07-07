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
package org.apache.polaris.forkless.oss.durable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.polaris.durable.model.CatalogCreation;
import org.apache.polaris.durable.model.EntityRecord;
import org.junit.jupiter.api.Test;

class DefaultDurableManagerTest {

  @Test
  void createCatalogPersistsBothCatalogAndAdminRole() {
    InMemoryDurablePrimitives store = new InMemoryDurablePrimitives();
    DefaultDurableManager manager = new DefaultDurableManager(store);

    CatalogCreation created = manager.createCatalog("sales", "sales_admin");

    assertThat(store.size()).isEqualTo(2);
    assertThat(store.read(created.catalog().id())).contains(created.catalog());
    assertThat(store.read(created.adminRole().id())).contains(created.adminRole());
    assertThat(created.adminRole().payload())
        .containsEntry("catalogId", Long.toString(created.catalog().id()));
  }

  @Test
  void createCatalogIsAtomicWhenAdminRoleCreationFails() {
    // A store that lands the catalog but rejects the admin-role create.
    InMemoryDurablePrimitives store =
        new InMemoryDurablePrimitives() {
          @Override
          public EntityRecord create(EntityRecord record) {
            if ("PRINCIPAL_ROLE".equals(record.type())) {
              throw new IllegalStateException("admin-role backend unavailable");
            }
            return super.create(record);
          }
        };
    DefaultDurableManager manager = new DefaultDurableManager(store);

    assertThatThrownBy(() -> manager.createCatalog("sales", "sales_admin"))
        .isInstanceOf(IllegalStateException.class);

    // The catalog was created, then rolled back: neither record persists.
    assertThat(store.size()).isZero();
  }

  @Test
  void compareAndSwapSucceedsOnMatchingVersionAndFailsOnStale() {
    InMemoryDurablePrimitives store = new InMemoryDurablePrimitives();
    EntityRecord v0 = store.create(new EntityRecord(0, 0, "CATALOG", "sales", Map.of()));

    EntityRecord v1 = new EntityRecord(v0.id(), 1, "CATALOG", "sales", Map.of("k", "v"));
    assertThat(store.compareAndSwap(v0.id(), 0, v1)).isTrue();
    assertThat(store.read(v0.id())).contains(v1);

    // Re-applying the original expected version is now stale: it must fail and leave v1 in place.
    EntityRecord stale = new EntityRecord(v0.id(), 1, "CATALOG", "renamed", Map.of());
    assertThat(store.compareAndSwap(v0.id(), 0, stale)).isFalse();
    assertThat(store.read(v0.id())).contains(v1);
  }
}
