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
package org.apache.polaris.persistence.primitives;

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;

import java.util.UUID;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.persistence.BasePolarisMetaStoreManagerTest;
import org.apache.polaris.core.persistence.PolarisTestMetaStoreManager;
import org.apache.polaris.core.persistence.transactional.TransactionalMetaStoreManagerImpl;
import org.apache.polaris.persistence.primitives.api.DurablePrimitives;
import org.apache.polaris.persistence.primitives.domain.DomainRecords;
import org.apache.polaris.persistence.primitives.domain.RecordTransactionalPersistence;
import org.junit.jupiter.api.AfterEach;

/** Full inherited upstream Manager fixture suite; H2 is only a Java wiring smoke test. */
public class H2ManagerTest extends BasePolarisMetaStoreManagerTest {
  private DurablePrimitives backend;

  protected String backendName() {
    return "h2";
  }

  @Override
  public PolarisTestMetaStoreManager createPolarisTestMetaStoreManager() {
    var diagnostics = new PolarisDefaultDiagServiceImpl();
    backend = NativeBackends.open(backendName());
    String realm = "java-poc-" + UUID.randomUUID();
    var persistence =
        new RecordTransactionalPersistence(
            diagnostics, new DomainRecords(backend, realm), RANDOM_SECRETS);
    var manager = new TransactionalMetaStoreManagerImpl(clock, diagnostics);
    return new PolarisTestMetaStoreManager(
        manager, new PolarisCallContext(() -> realm, persistence));
  }

  @AfterEach
  void closeBackend() {
    if (backend != null) backend.close();
  }
}
