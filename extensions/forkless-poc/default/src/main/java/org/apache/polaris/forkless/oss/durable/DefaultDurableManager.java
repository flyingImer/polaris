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

import java.util.Map;
import org.apache.polaris.durable.model.CatalogCreation;
import org.apache.polaris.durable.model.EntityRecord;
import org.apache.polaris.spi.durable.DurableManager;

/**
 * OSS-default {@link DurableManager}. Its chosen mechanism for the createCatalog atomicity
 * guarantee is a compensating rollback over an {@link InMemoryDurablePrimitives} store: create the
 * catalog, then the admin role, and if the admin role fails, remove the catalog so neither
 * persists.
 */
public class DefaultDurableManager implements DurableManager {

  private static final String CATALOG_TYPE = "CATALOG";
  private static final String ADMIN_ROLE_TYPE = "PRINCIPAL_ROLE";

  private final InMemoryDurablePrimitives store;

  public DefaultDurableManager(InMemoryDurablePrimitives store) {
    this.store = store;
  }

  @Override
  public CatalogCreation createCatalog(String catalogName, String adminRoleName) {
    EntityRecord catalog =
        store.create(new EntityRecord(0, 0, CATALOG_TYPE, catalogName, Map.of()));
    try {
      EntityRecord adminRole =
          store.create(
              new EntityRecord(
                  0,
                  0,
                  ADMIN_ROLE_TYPE,
                  adminRoleName,
                  Map.of("catalogId", Long.toString(catalog.id()))));
      return new CatalogCreation(catalog, adminRole);
    } catch (RuntimeException failed) {
      // Admin role did not land: undo the catalog so the caller sees neither.
      store.remove(catalog.id());
      throw failed;
    }
  }
}
