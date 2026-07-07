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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.polaris.durable.model.CatalogCreation;
import org.junit.jupiter.api.Test;

class RemoteDurableManagerTest {

  @Test
  void createCatalogProducesBothRecordsFromOneRemoteOperation() {
    RemoteDurableManager manager = new RemoteDurableManager();

    CatalogCreation created = manager.createCatalog("sales", "sales_admin");

    assertThat(created.catalog().name()).isEqualTo("sales");
    assertThat(created.adminRole().name()).isEqualTo("sales_admin");
    assertThat(created.catalog().id()).isNotEqualTo(created.adminRole().id());
    assertThat(created.adminRole().payload())
        .containsEntry("catalogId", Long.toString(created.catalog().id()));
  }
}
