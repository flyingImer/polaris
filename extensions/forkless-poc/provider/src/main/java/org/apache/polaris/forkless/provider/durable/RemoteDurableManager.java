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
package org.apache.polaris.forkless.provider.durable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.polaris.durable.model.CatalogCreation;
import org.apache.polaris.durable.model.EntityRecord;
import org.apache.polaris.spi.durable.DurableManager;

/**
 * Provider-analog {@link DurableManager} (the "Snowflake-private" side). It satisfies the same
 * createCatalog atomicity guarantee, but not by composing {@link
 * org.apache.polaris.spi.durable.DurablePrimitives}: it owns a single private composite operation
 * that lands both records or neither. Framework-agnostic: plain Java, no CDI, no jakarta.
 */
public class RemoteDurableManager implements DurableManager {

  private final ConcurrentHashMap<Long, EntityRecord> remoteStore = new ConcurrentHashMap<>();
  private final AtomicLong ids = new AtomicLong();

  @Override
  public CatalogCreation createCatalog(String catalogName, String adminRoleName) {
    return remoteCreateCatalogWithAdminRole(catalogName, adminRoleName);
  }

  /** One remote transaction: build both records, then commit them together under a single lock. */
  private synchronized CatalogCreation remoteCreateCatalogWithAdminRole(
      String catalogName, String adminRoleName) {
    EntityRecord catalog =
        new EntityRecord(ids.incrementAndGet(), 0, "CATALOG", catalogName, Map.of());
    EntityRecord adminRole =
        new EntityRecord(
            ids.incrementAndGet(),
            0,
            "PRINCIPAL_ROLE",
            adminRoleName,
            Map.of("catalogId", Long.toString(catalog.id())));
    remoteStore.put(catalog.id(), catalog);
    remoteStore.put(adminRole.id(), adminRole);
    return new CatalogCreation(catalog, adminRole);
  }
}
