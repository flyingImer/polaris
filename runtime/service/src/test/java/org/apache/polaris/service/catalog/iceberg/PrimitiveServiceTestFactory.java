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
package org.apache.polaris.service.catalog.iceberg;

import java.time.Clock;
import java.util.Properties;
import java.util.UUID;
import java.util.function.UnaryOperator;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.LocalPolarisMetaStoreManagerFactory;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.transactional.TransactionalPersistence;
import org.apache.polaris.persistence.primitives.api.DurablePrimitives;
import org.apache.polaris.persistence.primitives.domain.DomainRecords;
import org.apache.polaris.persistence.primitives.domain.RecordTransactionalPersistence;
import org.apache.polaris.persistence.primitives.jdbc.JdbcPrimitives;

/** Exercises real Java service callers against the strict terminal-attempt implementation. */
final class PrimitiveServiceTestFactory
    extends LocalPolarisMetaStoreManagerFactory<DurablePrimitives> {
  private final String jdbcUrl;
  private final UnaryOperator<DurablePrimitives> decorator;

  PrimitiveServiceTestFactory() {
    this(null, UnaryOperator.identity());
  }

  PrimitiveServiceTestFactory(String jdbcUrl, UnaryOperator<DurablePrimitives> decorator) {
    super(Clock.systemUTC(), new PolarisDefaultDiagServiceImpl());
    this.jdbcUrl = jdbcUrl;
    this.decorator = decorator;
  }

  @Override
  protected DurablePrimitives createBackingStore(PolarisDiagnostics diagnostics) {
    var properties = new Properties();
    if (jdbcUrl != null) {
      properties.setProperty("user", System.getProperty("poc.jdbc.user", "polaris_poc"));
      properties.setProperty("password", System.getProperty("poc.jdbc.password", ""));
    }
    var backend =
        new JdbcPrimitives(
            jdbcUrl != null
                ? jdbcUrl
                : "jdbc:h2:mem:caller-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1",
            properties);
    backend.initialize();
    return decorator.apply(backend);
  }

  @Override
  protected TransactionalPersistence createMetaStoreSession(
      DurablePrimitives backend,
      RealmContext realm,
      RootCredentialsSet credentials,
      PolarisDiagnostics diagnostics) {
    return new RecordTransactionalPersistence(
        diagnostics,
        new DomainRecords(backend, realm.getRealmIdentifier()),
        secretsGenerator(realm, credentials));
  }
}
