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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRefType;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.exceptions.CommitConflictException;
import org.apache.polaris.core.persistence.CommitOutcomeUnknownException;
import org.apache.polaris.core.persistence.ConfirmedTransactionConflictException;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.AccessDelegationMode;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

/** Actual Iceberg callers, with requirement-only tables participating in final publication. */
class PrimitiveCommitDependencyTest {
  private static final String CATALOG = "dependency";
  private static final Namespace NS = Namespace.of("ns");

  @Test
  void requirementOnlyTableRetainsEntityAndMetadata(@TempDir Path directory) throws Exception {
    var checks = new AtomicInteger();
    var services =
        services(
            manager -> {
              var spy = Mockito.spy(manager);
              Mockito.doAnswer(
                      call -> {
                        List<EntityWithPath> dependencies = call.getArgument(2);
                        if (dependencies.isEmpty()) return call.callRealMethod();
                        checks.incrementAndGet();
                        assertThat(dependencies).hasSize(1);
                        var dependency = dependencies.getFirst().entity();
                        PolarisCallContext context = call.getArgument(0);
                        Object result = call.callRealMethod();
                        assertThat(
                                manager
                                    .loadEntity(
                                        context,
                                        dependency.getCatalogId(),
                                        dependency.getId(),
                                        dependency.getType())
                                    .getEntity())
                            .usingRecursiveComparison()
                            .isEqualTo(dependency);
                        return result;
                      })
                  .when(spy)
                  .updateEntitiesPropertiesIfNotChanged(any(), any(), any());
              return spy;
            });
    setup(services, directory);
    var before = load(services, "b");
    var files = metadataFiles(directory);
    commit(services, List.of(schemaChange("a"), requirementOnly("b")));
    assertThat(checks).hasValue(1);
    assertThat(load(services, "a").tableMetadata().currentSchemaId()).isNotEqualTo(0);
    assertThat(load(services, "b").metadataLocation()).isEqualTo(before.metadataLocation());
    assertThat(metadataFiles(directory)).containsAll(files).hasSize(files.size() + 1);
  }

  @Test
  void passthroughSnapshotChangedBeforeCommit(@TempDir Path directory) throws Exception {
    var services = services(UnaryOperator.identity());
    setup(services, directory);
    var beforeA = load(services, "a");
    // Change B after its metadata was loaded and its explicit assertion checked. Capturing a
    // replacement version after validation would incorrectly admit A's update.
    UpdateRequirement concurrentChange = afterValidation(() -> update(services, schemaChange("b")));
    var dependency =
        UpdateTableRequest.create(
            id("b"),
            List.of(new UpdateRequirement.AssertCurrentSchemaID(0), concurrentChange),
            List.of());
    assertThatThrownBy(() -> commit(services, List.of(schemaChange("a"), dependency)))
        .isInstanceOf(CommitConflictException.class);
    assertThat(load(services, "a").metadataLocation()).isEqualTo(beforeA.metadataLocation());
    assertThat(load(services, "b").tableMetadata().currentSchemaId()).isNotEqualTo(0);
    // Three original files and the competing B commit. The rejected A file is reclaimed.
    assertThat(metadataFiles(directory)).hasSize(4);
  }

  @Test
  void nativeReciprocalRequirementsCannotBothSucceed(@TempDir Path directory) throws Exception {
    Assumptions.assumeTrue(
        System.getProperty("poc.jdbc.url") != null
            || "fdb".equals(System.getProperty("poc.backend")),
        "Requires a disposable native PostgreSQL, CockroachDB or FoundationDB endpoint");
    var barrier = new CountDownLatch(2);
    var racing = new AtomicBoolean();
    var submissions = new AtomicInteger();
    var services =
        services(
            manager -> {
              var spy = Mockito.spy(manager);
              Mockito.doAnswer(
                      call -> {
                        if (racing.get()) {
                          submissions.incrementAndGet();
                          barrier.countDown();
                          if (!barrier.await(30, TimeUnit.SECONDS))
                            throw new AssertionError("Missing competitor");
                        }
                        return call.callRealMethod();
                      })
                  .when(spy)
                  .updateEntitiesPropertiesIfNotChanged(any(), any(), any());
              return spy;
            });
    setup(services, directory);
    racing.set(true);
    boolean first;
    boolean second;
    try (var workers = Executors.newFixedThreadPool(2)) {
      var one = workers.submit(() -> commitOrConflict(services, "a", "b"));
      var two = workers.submit(() -> commitOrConflict(services, "b", "a"));
      first = one.get(60, TimeUnit.SECONDS);
      second = two.get(60, TimeUnit.SECONDS);
    }
    assertThat(submissions).hasValue(2);
    assertThat(first || second).isTrue();
    assertThat(first && second).isFalse();
    assertThat(load(services, first ? "a" : "b").tableMetadata().currentSchemaId()).isNotEqualTo(0);
    assertThat(load(services, first ? "b" : "a").tableMetadata().currentSchemaId()).isZero();
  }

  @ParameterizedTest(name = "threeTablePromotionIsAllOrNothing({0})")
  @ValueSource(booleans = {false, true})
  void threeTablePromotionIsAllOrNothing(boolean competingUpdate, @TempDir Path directory) {
    var services = services(UnaryOperator.identity());
    setup(services, directory);
    prepareBranches(services);
    var beforeB = load(services, "b");
    var changes = new ArrayList<>(promotion(2));
    // The other writer commits after A and B have been staged, before final publication.
    UpdateRequirement compete =
        afterValidation(
            () -> {
              if (competingUpdate) update(services, moveMain("a", 3));
            });
    changes.set(
        2,
        UpdateTableRequest.create(
            id("c"),
            List.of(new UpdateRequirement.AssertRefSnapshotID("main", 1L), compete),
            List.of(setRef("main", 2))));
    if (competingUpdate) {
      assertThatThrownBy(() -> commit(services, changes)).isInstanceOf(CommitFailedException.class);
      assertThat(load(services, "b").metadataLocation()).isEqualTo(beforeB.metadataLocation());
      assertThat(load(services, "a").tableMetadata().ref("main").snapshotId()).isEqualTo(3);
      for (String table : List.of("b", "c")) {
        assertThat(load(services, table).tableMetadata().ref("main").snapshotId()).isEqualTo(1);
      }
    } else {
      commit(services, changes);
      for (String table : List.of("a", "b", "c")) {
        assertThat(load(services, table).tableMetadata().ref("main").snapshotId()).isEqualTo(2);
      }
    }
  }

  @ParameterizedTest(name = "nativePromotionRacePublishesOneCompleteResult({0})")
  @ValueSource(booleans = {false, true})
  void nativePromotionRacePublishesOneCompleteResult(
      boolean singleTableWriter, @TempDir Path directory) throws Exception {
    Assumptions.assumeTrue(
        System.getProperty("poc.jdbc.url") != null
            || "fdb".equals(System.getProperty("poc.backend")),
        "Requires a disposable native PostgreSQL, CockroachDB or FoundationDB endpoint");
    var barrier = new CountDownLatch(2);
    var racing = new AtomicBoolean();
    var services =
        services(
            manager -> {
              var spy = Mockito.spy(manager);
              Answer<Object> publish =
                  call -> {
                    if (racing.get()) {
                      barrier.countDown();
                      if (!barrier.await(30, TimeUnit.SECONDS))
                        throw new AssertionError("Missing competitor");
                    }
                    return call.callRealMethod();
                  };
              Mockito.doAnswer(publish)
                  .when(spy)
                  .updateEntitiesPropertiesIfNotChanged(any(), any(), any());
              Mockito.doAnswer(publish)
                  .when(spy)
                  .updateEntityPropertiesIfNotChanged(any(), any(), any());
              return spy;
            });
    setup(services, directory);
    prepareBranches(services);
    racing.set(true);
    boolean promotionWon;
    boolean competitorWon;
    try (var workers = Executors.newFixedThreadPool(2)) {
      var first = workers.submit(() -> commitOrConflict(services, promotion(2)));
      var second =
          workers.submit(
              () ->
                  operationOrConflict(
                      () -> {
                        if (singleTableWriter) update(services, moveMain("a", 3));
                        else commit(services, promotion(3));
                      }));
      promotionWon = first.get(60, TimeUnit.SECONDS);
      competitorWon = second.get(60, TimeUnit.SECONDS);
    }
    assertThat(promotionWon ^ competitorWon).isTrue();
    for (String table : List.of("a", "b", "c")) {
      long expected = promotionWon ? 2 : singleTableWriter && !table.equals("a") ? 1 : 3;
      assertThat(load(services, table).tableMetadata().ref("main").snapshotId())
          .isEqualTo(expected);
    }
  }

  private static void prepareBranches(TestServices services) {
    // Same snapshot/ref recipe as apache/polaris#5740, through the Primitives service fixture.
    for (String table : List.of("a", "b", "c")) {
      var additions = new ArrayList<MetadataUpdate>();
      for (long snapshotId = 1; snapshotId <= 3; snapshotId++) {
        additions.add(
            new MetadataUpdate.AddSnapshot(
                SnapshotParser.fromJson(
                    "{\"snapshot-id\":"
                        + snapshotId
                        + (snapshotId == 1 ? "" : ",\"parent-snapshot-id\":1")
                        + ",\"sequence-number\":"
                        + snapshotId
                        + ",\"timestamp-ms\":"
                        + (System.currentTimeMillis() + snapshotId)
                        + ",\"summary\":{\"operation\":\"append\"},\"manifest-list\":\""
                        + load(services, table).tableMetadata().location()
                        + "/metadata/snap-"
                        + snapshotId
                        + ".avro\",\"schema-id\":0}")));
      }
      additions.add(setRef("main", 1));
      additions.add(setRef("backfill", 2));
      additions.add(setRef("other", 3));
      update(services, UpdateTableRequest.create(id(table), List.of(), additions));
    }
  }

  private static UpdateRequirement afterValidation(Runnable action) {
    return new UpdateRequirement() {
      @Override
      public void validate(TableMetadata base) {
        action.run();
      }
    };
  }

  private static MetadataUpdate setRef(String name, long snapshotId) {
    return new MetadataUpdate.SetSnapshotRef(
        name, snapshotId, SnapshotRefType.BRANCH, null, null, null);
  }

  private static UpdateTableRequest moveMain(String table, long snapshotId) {
    return UpdateTableRequest.create(
        id(table),
        List.of(new UpdateRequirement.AssertRefSnapshotID("main", 1L)),
        List.of(setRef("main", snapshotId)));
  }

  private static List<UpdateTableRequest> promotion(long snapshotId) {
    return List.of(moveMain("a", snapshotId), moveMain("b", snapshotId), moveMain("c", snapshotId));
  }

  @Test
  void repeatedTableRequestsCoalesceIntoOneMutation(@TempDir Path directory) {
    var submissions = new AtomicInteger();
    var services =
        services(
            manager -> {
              var spy = Mockito.spy(manager);
              Mockito.doAnswer(
                      call -> {
                        List<EntityWithPath> updates = call.getArgument(1);
                        assertThat(updates).hasSize(1);
                        submissions.incrementAndGet();
                        return call.callRealMethod();
                      })
                  .when(spy)
                  .updateEntitiesPropertiesIfNotChanged(any(), any(), any());
              return spy;
            });
    setup(services, directory);
    commit(
        services, List.of(propertyChange("a", "first", "1"), propertyChange("a", "second", "2")));
    assertThat(submissions).hasValue(1);
    assertThat(load(services, "a").tableMetadata().properties())
        .containsEntry("first", "1")
        .containsEntry("second", "2");
  }

  @ParameterizedTest(name = "unknownWithCheckOnlyDependencyRetainsMetadata({0})")
  @ValueSource(booleans = {false, true})
  void unknownWithCheckOnlyDependencyRetainsMetadata(boolean applied, @TempDir Path directory)
      throws Exception {
    var inject = new AtomicBoolean();
    var submissions = new AtomicInteger();
    var services =
        services(
            manager -> {
              var spy = Mockito.spy(manager);
              Mockito.doAnswer(
                      call -> {
                        if (!inject.get()) return call.callRealMethod();
                        submissions.incrementAndGet();
                        if (applied) call.callRealMethod();
                        throw new CommitOutcomeUnknownException(
                            new IllegalStateException("Lost response"));
                      })
                  .when(spy)
                  .updateEntitiesPropertiesIfNotChanged(any(), any(), any());
              return spy;
            });
    setup(services, directory);
    var files = metadataFiles(directory);
    var beforeB = load(services, "b");
    inject.set(true);
    assertThatThrownBy(() -> commit(services, List.of(schemaChange("a"), requirementOnly("b"))))
        .isInstanceOf(CommitStateUnknownException.class);
    assertThat(submissions).hasValue(1);
    assertThat(metadataFiles(directory)).containsAll(files).hasSize(files.size() + 1);
    assertThat(load(services, "b").metadataLocation()).isEqualTo(beforeB.metadataLocation());
    assertThat(load(services, "a").tableMetadata().currentSchemaId() != 0).isEqualTo(applied);
  }

  private static boolean commitOrConflict(TestServices services, String update, String dependency) {
    return commitOrConflict(services, List.of(schemaChange(update), requirementOnly(dependency)));
  }

  private static boolean commitOrConflict(TestServices services, List<UpdateTableRequest> changes) {
    return operationOrConflict(() -> commit(services, changes));
  }

  private static boolean operationOrConflict(Runnable operation) {
    try {
      operation.run();
      return true;
    } catch (CommitFailedException
        | CommitConflictException
        | ConfirmedTransactionConflictException e) {
      return false;
    }
  }

  private static TestServices services(UnaryOperator<PolarisMetaStoreManager> decorator) {
    String realm = "dependency-" + UUID.randomUUID();
    var factory =
        new PrimitiveServiceTestFactory(
            System.getProperty("poc.jdbc.url"), UnaryOperator.identity());
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

  private static void setup(TestServices services, Path directory) {
    String base = directory.toUri().toString().replaceAll("/+$", "");
    var config =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of(base))
            .build();
    var catalog =
        new Catalog(
            Catalog.TypeEnum.INTERNAL,
            CATALOG,
            CatalogProperties.builder().setDefaultBaseLocation(base).build(),
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
    for (String name : List.of("a", "b", "c")) {
      services
          .catalogAdapter()
          .newHandler(services.securityContext(), CATALOG)
          .createTableDirect(
              NS,
              CreateTableRequest.builder().withName(name).withSchema(SCHEMA).build(),
              EnumSet.noneOf(AccessDelegationMode.class),
              Optional.empty());
    }
  }

  private static TableIdentifier id(String name) {
    return TableIdentifier.of(NS, name);
  }

  private static UpdateTableRequest schemaChange(String name) {
    var schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(100, "added", Types.StringType.get()));
    return UpdateTableRequest.create(
        id(name),
        List.of(new UpdateRequirement.AssertCurrentSchemaID(0)),
        List.of(new MetadataUpdate.AddSchema(schema), new MetadataUpdate.SetCurrentSchema(-1)));
  }

  private static UpdateTableRequest requirementOnly(String name) {
    return UpdateTableRequest.create(
        id(name), List.of(new UpdateRequirement.AssertCurrentSchemaID(0)), List.of());
  }

  private static UpdateTableRequest propertyChange(String name, String key, String value) {
    return UpdateTableRequest.create(
        id(name), List.of(), List.of(new MetadataUpdate.SetProperties(Map.of(key, value))));
  }

  private static void commit(TestServices services, List<UpdateTableRequest> changes) {
    services
        .catalogAdapter()
        .newHandler(services.securityContext(), CATALOG)
        .commitTransaction(new CommitTransactionRequest(changes));
  }

  private static void update(TestServices services, UpdateTableRequest request) {
    services
        .catalogAdapter()
        .newHandler(services.securityContext(), CATALOG)
        .updateTable(request.identifier(), request);
  }

  private static LoadTableResponse load(TestServices services, String name) {
    return services
        .catalogAdapter()
        .newHandler(services.securityContext(), CATALOG)
        .loadTable(
            id(name), "all", null, EnumSet.noneOf(AccessDelegationMode.class), Optional.empty())
        .orElseThrow();
  }

  private static Set<Path> metadataFiles(Path directory) throws Exception {
    try (var files = Files.walk(directory)) {
      return files.filter(p -> p.toString().endsWith(".metadata.json")).collect(Collectors.toSet());
    }
  }
}
