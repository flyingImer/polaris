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

import static org.apache.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;

import jakarta.ws.rs.core.Response;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.exceptions.CommitConflictException;
import org.apache.polaris.core.persistence.ConfirmedTransactionConflictException;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.persistence.primitives.api.DurablePrimitives;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.AccessDelegationMode;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

/** Feature checks and native publication are deliberately separated by concurrent changes. */
class PrimitiveTableCreateTest {
  private static final String CATALOG = "tables";
  private static final Namespace NS = Namespace.of("ns");

  @Test
  void changedCatalogStorageConfigRejectsAlreadyValidatedCreate(@TempDir Path directory) {
    var inject = new AtomicBoolean();
    var intercepted = new AtomicInteger();
    String base = directory.toUri().toString().replaceAll("/+$", "");
    // Tighten the catalog within the existing namespace's location restriction.
    String tightenedRoot = base + "/old-root/ns/allowed";
    var services =
        services(
            new PrimitiveServiceTestFactory(),
            manager -> {
              var spy = Mockito.spy(manager);
              Mockito.doAnswer(
                      call -> {
                        PolarisBaseEntity entity = call.getArgument(2);
                        if (entity.getSubType() == PolarisEntitySubType.ICEBERG_TABLE
                            && inject.compareAndSet(true, false)) {
                          intercepted.incrementAndGet();
                          PolarisCallContext context = call.getArgument(0);
                          var catalog =
                              CatalogEntity.of(
                                  manager
                                      .readEntityByName(
                                          context,
                                          null,
                                          PolarisEntityType.CATALOG,
                                          PolarisEntitySubType.NULL_SUBTYPE,
                                          CATALOG)
                                      .getEntity());
                          var updated =
                              new CatalogEntity.Builder(catalog)
                                  .setDefaultBaseLocation(tightenedRoot)
                                  .setStorageConfigurationInfo(
                                      context.getRealmConfig(),
                                      FileStorageConfigInfo.builder()
                                          .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
                                          .setAllowedLocations(List.of(tightenedRoot))
                                          .build())
                                  .build();
                          assertThat(
                                  manager
                                      .updateEntityPropertiesIfNotChanged(context, null, updated)
                                      .isSuccess())
                              .isTrue();
                        }
                        return call.callRealMethod();
                      })
                  .when(spy)
                  .createEntityIfNotExists(any(), any(), any());
              return spy;
            });
    setup(services, base);
    inject.set(true);
    assertThatThrownBy(() -> create(services, "stale", base + "/old-root/ns/stale"))
        .isInstanceOf(CommitConflictException.class)
        .hasMessageContaining("Validation dependency changed");
    assertThat(intercepted).hasValue(1);
    assertThat(tables(services)).isEmpty();
    // A fresh Feature request must now reject this location under the new effective config.
    assertThatThrownBy(() -> create(services, "fresh-invalid", base + "/old-root/ns/stale"))
        .isInstanceOf(ForbiddenException.class);
    create(services, "fresh-valid", tightenedRoot + "/valid");
    assertThat(tables(services)).containsExactly(TableIdentifier.of(NS, "fresh-valid"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void nativeConcurrentTableCreatesProtectLocations(boolean overlap, @TempDir Path directory)
      throws Exception {
    String url = System.getProperty("poc.jdbc.url");
    Assumptions.assumeTrue(
        url != null && url.startsWith("jdbc:postgresql:"),
        "Requires explicit disposable native PostgreSQL endpoint; H2 is not range evidence");
    var publishing = ThreadLocal.withInitial(() -> false);
    var barrier = new CountDownLatch(2);
    var joined = new AtomicInteger();
    var calls = new AtomicInteger();
    var factory =
        new PrimitiveServiceTestFactory(
            url,
            backend ->
                new DurablePrimitives() {
                  @Override
                  public ReadView readView() {
                    return backend.readView();
                  }

                  @Override
                  public Attempt begin() {
                    var attempt = backend.begin();
                    return new Attempt() {
                      @Override
                      public byte[] get(String key) {
                        return attempt.get(key);
                      }

                      @Override
                      public List<byte[]> getMany(List<String> keys) {
                        return attempt.getMany(keys);
                      }

                      @Override
                      public List<Entry> scan(String lo, String hi, int limit) {
                        return attempt.scan(lo, hi, limit);
                      }

                      @Override
                      public void commit(List<Mutation> mutations) {
                        if (publishing.get() && joined.getAndIncrement() < 2) {
                          barrier.countDown();
                          try {
                            if (!barrier.await(30, TimeUnit.SECONDS))
                              throw new AssertionError("Publication did not rendezvous");
                          } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(e);
                          }
                        }
                        attempt.commit(mutations);
                      }

                      @Override
                      public void close() {
                        attempt.close();
                      }
                    };
                  }

                  @Override
                  public void close() {
                    backend.close();
                  }
                });
    var services =
        services(
            factory,
            manager -> {
              var spy = Mockito.spy(manager);
              Mockito.doAnswer(
                      call -> {
                        PolarisBaseEntity entity = call.getArgument(2);
                        if (entity.getSubType() != PolarisEntitySubType.ICEBERG_TABLE)
                          return call.callRealMethod();
                        calls.incrementAndGet();
                        publishing.set(true);
                        try {
                          return call.callRealMethod();
                        } finally {
                          publishing.remove();
                        }
                      })
                  .when(spy)
                  .createEntityIfNotExists(any(), any(), any());
              return spy;
            });
    String base = directory.toUri().toString().replaceAll("/+$", "");
    setup(services, base);
    String firstLocation = base + "/old-root/ns/shared";
    String secondLocation = overlap ? firstLocation + "/child" : base + "/old-root/ns/separate";
    boolean first;
    boolean second;
    try (var workers = Executors.newFixedThreadPool(2)) {
      var one = workers.submit(() -> createOrConfirmedAbort(services, "one", firstLocation));
      var two = workers.submit(() -> createOrConfirmedAbort(services, "two", secondLocation));
      first = one.get(60, TimeUnit.SECONDS);
      second = two.get(60, TimeUnit.SECONDS);
    }
    assertThat(barrier.getCount()).isZero();
    assertThat(calls).hasValue(2);
    assertThat(first || second).isTrue();
    if (overlap) {
      assertThat(first && second).isFalse();
      assertThat(tables(services)).hasSize(1);
      String loser = first ? "two" : "one";
      String location = first ? secondLocation : firstLocation;
      assertThatThrownBy(() -> create(services, loser, location))
          .isInstanceOf(ForbiddenException.class);
      assertThat(tables(services)).hasSize(1);
    } else {
      // A confirmed native abort permits a fresh request. No UNKNOWN is caught here.
      if (!first) create(services, "one", firstLocation);
      if (!second) create(services, "two", secondLocation);
      assertThat(tables(services))
          .containsExactlyInAnyOrder(TableIdentifier.of(NS, "one"), TableIdentifier.of(NS, "two"));
    }
  }

  private static boolean createOrConfirmedAbort(
      TestServices services, String name, String location) {
    try {
      create(services, name, location);
      return true;
    } catch (ConfirmedTransactionConflictException e) {
      return false;
    }
  }

  private static TestServices services(
      PrimitiveServiceTestFactory factory, UnaryOperator<PolarisMetaStoreManager> decorator) {
    String realm = "table-create-" + UUID.randomUUID();
    factory.bootstrapRealms(List.of(realm), RootCredentialsSet.EMPTY);
    return TestServices.builder()
        .realmContext(() -> realm)
        .metaStoreManagerFactory(factory)
        .metaStoreManagerDecorator(decorator)
        .config(
            Map.of(
                "ALLOW_INSECURE_STORAGE_TYPES",
                true,
                "SUPPORTED_CATALOG_STORAGE_TYPES",
                List.of("FILE")))
        .build();
  }

  private static void setup(TestServices services, String base) {
    var config =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of(base + "/old-root"))
            .build();
    var catalog =
        new Catalog(
            Catalog.TypeEnum.INTERNAL,
            CATALOG,
            CatalogProperties.builder().setDefaultBaseLocation(base + "/old-root").build(),
            0L,
            0L,
            1,
            config);
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalog),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(201);
    }
    try (Response response =
        services
            .restApi()
            .createNamespace(
                CATALOG,
                CreateNamespaceRequest.builder().withNamespace(NS).build(),
                null,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(200);
    }
  }

  private static void create(TestServices services, String name, String location) {
    services
        .catalogAdapter()
        .newHandler(services.securityContext(), CATALOG)
        .createTableDirect(
            NS,
            CreateTableRequest.builder()
                .withName(name)
                .withSchema(SCHEMA)
                .withLocation(location)
                .build(),
            EnumSet.noneOf(AccessDelegationMode.class),
            Optional.empty());
  }

  private static List<TableIdentifier> tables(TestServices services) {
    return services
        .catalogAdapter()
        .newHandler(services.securityContext(), CATALOG)
        .listTables(NS, null, null)
        .identifiers();
  }
}
