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

import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.Properties;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.LocalPolarisMetaStoreManagerFactory;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.transactional.TransactionalPersistence;
import org.apache.polaris.persistence.primitives.api.DurablePrimitives;
import org.apache.polaris.persistence.primitives.domain.DomainRecords;
import org.apache.polaris.persistence.primitives.domain.RecordTransactionalPersistence;
import org.apache.polaris.persistence.primitives.fdb.FdbPrimitives;
import org.apache.polaris.persistence.primitives.jdbc.JdbcPrimitives;
import org.apache.polaris.persistence.primitives.spanner.SpannerPrimitives;

/** Opt-in runtime wiring using the existing Polaris realm/bootstrap/factory contract. */
@ApplicationScoped
@Identifier("durable-primitives-poc")
public class PrimitiveMetaStoreManagerFactory
    extends LocalPolarisMetaStoreManagerFactory<DurablePrimitives> {
  private final java.util.Set<DurablePrimitives> stores =
      java.util.concurrent.ConcurrentHashMap.newKeySet();

  @PreDestroy
  void closeStores() {
    stores.forEach(DurablePrimitives::close);
    stores.clear();
  }

  protected PrimitiveMetaStoreManagerFactory() {
    this(null, null);
  }

  @Inject
  public PrimitiveMetaStoreManagerFactory(Clock clock, PolarisDiagnostics diagnostics) {
    super(clock, diagnostics);
  }

  @Override
  protected DurablePrimitives createBackingStore(PolarisDiagnostics diagnostics) {
    DurablePrimitives store =
        switch (required("poc.backend")) {
          case "jdbc" -> {
            var properties = new Properties();
            properties.setProperty("user", required("poc.jdbc.user"));
            properties.setProperty("password", System.getProperty("poc.jdbc.password", ""));
            // Schema bootstrap is an explicit setup step; normal requests never execute DDL.
            yield new JdbcPrimitives(required("poc.jdbc.url"), properties);
          }
          case "fdb" -> new FdbPrimitives(required("poc.fdb.cluster"));
          case "spanner" ->
              new SpannerPrimitives(
                  required("poc.spanner.endpoint"), required("poc.spanner.database"));
          default -> throw new IllegalArgumentException("Unsupported poc.backend");
        };
    stores.add(store);
    return store;
  }

  @Override
  protected TransactionalPersistence createMetaStoreSession(
      DurablePrimitives store,
      RealmContext realm,
      RootCredentialsSet credentials,
      PolarisDiagnostics diagnostics) {
    return new RecordTransactionalPersistence(
        diagnostics,
        new DomainRecords(store, realm.getRealmIdentifier()),
        secretsGenerator(realm, credentials));
  }

  private static String required(String key) {
    String value = System.getProperty(key);
    if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing " + key);
    return value;
  }
}
