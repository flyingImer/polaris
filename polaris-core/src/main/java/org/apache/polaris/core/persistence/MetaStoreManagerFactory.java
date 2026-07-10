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

import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;

/** Configuration interface for configuring the {@link DurableManager}. */
public interface MetaStoreManagerFactory {

  DurableManager getOrCreateMetaStoreManager(RealmContext realmContext);

  DurablePrimitives getOrCreateSession(RealmContext realmContext);

  /**
   * Returns the per-realm {@link MetricsPersistence}. This SPI is decoupled from {@link
   * DurablePrimitives} so backends that do not implement metrics persistence can simply return a
   * no-op instance.
   */
  MetricsPersistence getOrCreateMetricsPersistence(RealmContext realmContext);

  EntityCache getOrCreateEntityCache(RealmContext realmContext, RealmConfig realmConfig);
}
