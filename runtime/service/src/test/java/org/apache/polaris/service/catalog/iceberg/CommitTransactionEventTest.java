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

import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.core.Response;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.AuthorizationDecision;
import org.apache.polaris.core.auth.AuthorizationRequest;
import org.apache.polaris.core.catalog.FederatedCatalogFactory;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.events.EventAttributeMap;
import org.apache.polaris.core.events.PolarisEvent;
import org.apache.polaris.core.events.PolarisEventType;
import org.apache.polaris.core.exceptions.TooManyItemsException;
import org.apache.polaris.extension.catalog.iceberg.CatalogHandlerUtils;
import org.apache.polaris.extension.catalog.iceberg.PolarisIcebergCatalog;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.DefaultAccessDelegationModeResolver;
import org.apache.polaris.service.catalog.DefaultCatalogPrefixParser;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.listeners.InMemoryEventCollector;
import org.apache.polaris.service.reporting.DefaultMetricsReporter;
import org.apache.polaris.spi.durable.CommitDisruptedException;
import org.apache.polaris.spi.durable.DurableManager;
import org.apache.polaris.spi.substrate.PolarisAuthorizer;
import org.apache.polaris.spi.substrate.ReservedProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

public class CommitTransactionEventTest {
  private static final String namespace = "ns";
  private static final String catalog = "test-catalog";
  private static final String propertyName = "custom-property-1";

  // UUID v7
  private static final UUID IDEMPOTENCY_KEY = new UUID(116617318654508422L, -7820829973016961092L);

  private String catalogLocation;

  @BeforeEach
  public void setUp(@TempDir Path tempDir) {
    catalogLocation = tempDir.toAbsolutePath().toUri().toString();
    if (catalogLocation.endsWith("/")) {
      catalogLocation = catalogLocation.substring(0, catalogLocation.length() - 1);
    }
  }

  @Test
  void testEventsForSuccessfulTransaction() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);

    String table1Name = "test-table-1";
    String table2Name = "test-table-2";
    executeTransactionTest(false, table1Name, table2Name, testServices);

    // Verify that all (Before/After)CommitTransaction and (Before/After)UpdateTable events were
    // emitted
    InMemoryEventCollector testPolarisEventDispatcher =
        (InMemoryEventCollector) testServices.polarisEventDispatcher();
    assertThat(testPolarisEventDispatcher.getLatest(PolarisEventType.BEFORE_COMMIT_TRANSACTION))
        .isNotNull();
    PolarisEvent beforeUpdateEvent =
        testPolarisEventDispatcher.getLatest(PolarisEventType.BEFORE_UPDATE_TABLE);
    assertThat(beforeUpdateEvent.attributes().getRequired(EventAttributes.TABLE_NAME))
        .isEqualTo(table2Name);

    assertThat(testPolarisEventDispatcher.getLatest(PolarisEventType.AFTER_COMMIT_TRANSACTION))
        .isNotNull();
    PolarisEvent afterUpdateEvent =
        testPolarisEventDispatcher.getLatest(PolarisEventType.AFTER_UPDATE_TABLE);
    assertThat(afterUpdateEvent.attributes().getRequired(EventAttributes.TABLE_NAME))
        .isEqualTo(table2Name);
  }

  @Test
  void testEventsForUnSuccessfulTransaction() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);

    String table3Name = "test-table-3";
    String table4Name = "test-table-4";
    executeTransactionTest(true, table3Name, table4Name, testServices);

    // Verify that all (Before)CommitTable events were emitted
    InMemoryEventCollector testPolarisEventDispatcher =
        (InMemoryEventCollector) testServices.polarisEventDispatcher();

    // Verify that all BeforeCommitTransaction and BeforeUpdateTable events were emitted,
    // and that the AfterCommitTransaction and AfterUpdateTable events were not emitted
    assertThat(testPolarisEventDispatcher.getLatest(PolarisEventType.BEFORE_COMMIT_TRANSACTION))
        .isNotNull();
    PolarisEvent beforeUpdateEvent =
        testPolarisEventDispatcher.getLatest(PolarisEventType.BEFORE_UPDATE_TABLE);
    assertThat(beforeUpdateEvent.attributes().getRequired(EventAttributes.TABLE_NAME))
        .isEqualTo(table4Name);

    assertThatThrownBy(
            () -> testPolarisEventDispatcher.getLatest(PolarisEventType.AFTER_COMMIT_TRANSACTION))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(
            () -> testPolarisEventDispatcher.getLatest(PolarisEventType.AFTER_UPDATE_TABLE))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void testLoadTableResponsesInCommitTransaction() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);

    String table1Name = "test-table-5";
    String table2Name = "test-table-6";
    executeTransactionTest(false, table1Name, table2Name, testServices);

    InMemoryEventCollector testPolarisEventDispatcher =
        (InMemoryEventCollector) testServices.polarisEventDispatcher();

    // Verify that AfterUpdateTable events contain LoadTableResponse objects
    PolarisEvent afterUpdateTableEvent =
        testPolarisEventDispatcher.getLatest(PolarisEventType.AFTER_UPDATE_TABLE);

    // Verify second table's LoadTableResponse
    assertThat(afterUpdateTableEvent.attributes().getRequired(EventAttributes.TABLE_NAME))
        .isEqualTo(table2Name);
    assertThat(afterUpdateTableEvent.attributes().get(EventAttributes.TABLE_METADATA)).isPresent();
    assertThat(afterUpdateTableEvent.attributes().get(EventAttributes.TABLE_METADATA)).isNotEmpty();
    TableMetadata metadata =
        afterUpdateTableEvent.attributes().getRequired(EventAttributes.TABLE_METADATA);
    assertThat(metadata).isNotNull();
    assertThat(metadata.properties()).containsEntry(propertyName, "value2");
  }

  /**
   * A multi-table transaction in which one table's location changes must commit. The change is
   * expressed as a property ({@code write.data.path}); a literal {@code SetLocation} update reaches
   * the identical validation, because the requested-locations-changed guard compares the base
   * location and both {@code write.*.path} properties, so either form runs {@code
   * validateNoLocationOverlap}. The literal {@code SetLocation} form has its own case in {@link
   * #testCommitTransactionWithLiteralSetLocationSucceeds()}.
   */
  @Test
  void testCommitTransactionWithLocationChangeSucceeds() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(
        testServices,
        Map.of(
            FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(),
            "false",
            FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
            "true"),
        catalogLocation);

    String table1Name = "test-table-7";
    String table2Name = "test-table-8";
    createTable(testServices, table1Name, catalogLocation);
    createTable(testServices, table2Name, catalogLocation);

    // Inside table1's own location: an out-of-table location is refused by a separate check.
    String newDataLocation =
        String.format(
            "%s/%s/%s/%s/custom-data/%s",
            catalogLocation, catalog, namespace, table1Name, UUID.randomUUID());

    CommitTransactionRequest request =
        new CommitTransactionRequest(
            List.of(
                UpdateTableRequest.create(
                    TableIdentifier.of(namespace, table1Name),
                    List.of(),
                    List.of(
                        new MetadataUpdate.SetProperties(
                            Map.of(
                                IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY,
                                newDataLocation)))),
                UpdateTableRequest.create(
                    TableIdentifier.of(namespace, table2Name),
                    List.of(),
                    List.of(new MetadataUpdate.SetProperties(Map.of(propertyName, "value2"))))));

    try (Response response =
        testServices
            .restApi()
            .commitTransaction(
                catalog,
                request,
                IDEMPOTENCY_KEY,
                testServices.realmContext(),
                testServices.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    // A 204 only proves nothing threw. Read the table back, so this case proves what its name says:
    // the location change is actually persisted.
    try (Response loaded =
        testServices
            .restApi()
            .loadTable(
                catalog,
                namespace,
                table1Name,
                null,
                null,
                "ALL",
                null,
                testServices.realmContext(),
                testServices.securityContext())) {
      assertThat(loaded.readEntity(LoadTableResponse.class).tableMetadata().properties())
          .containsEntry(
              IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY, newDataLocation);
    }

    InMemoryEventCollector testPolarisEventDispatcher =
        (InMemoryEventCollector) testServices.polarisEventDispatcher();
    assertThat(testPolarisEventDispatcher.getLatest(PolarisEventType.AFTER_COMMIT_TRANSACTION))
        .isNotNull();
  }

  /**
   * A multi-table transaction that fails must leave every table untouched. The first table's change
   * is valid and has already been applied to its metadata by the time the second table's
   * requirement check fails, so this is what proves the whole transaction is one commit rather than
   * one commit per table.
   */
  @Test
  void testFailedTransactionLeavesTheFirstTableUnchanged() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);

    String table1Name = "test-table-9";
    String table2Name = "test-table-10";
    createTable(testServices, table1Name, catalogLocation);
    createTable(testServices, table2Name, catalogLocation);

    CommitTransactionRequest request =
        new CommitTransactionRequest(
            List.of(
                UpdateTableRequest.create(
                    TableIdentifier.of(namespace, table1Name),
                    List.of(),
                    List.of(new MetadataUpdate.SetProperties(Map.of(propertyName, "value1")))),
                UpdateTableRequest.create(
                    TableIdentifier.of(namespace, table2Name),
                    List.of(new UpdateRequirement.AssertCurrentSchemaID(-1)),
                    List.of(new MetadataUpdate.SetProperties(Map.of(propertyName, "value2"))))));

    assertThatThrownBy(
            () ->
                testServices
                    .restApi()
                    .commitTransaction(
                        catalog,
                        request,
                        IDEMPOTENCY_KEY,
                        testServices.realmContext(),
                        testServices.securityContext()))
        .isInstanceOf(CommitFailedException.class);

    // The first table must carry no trace of the aborted transaction.
    try (Response loaded =
        testServices
            .restApi()
            .loadTable(
                catalog,
                namespace,
                table1Name,
                null,
                null,
                "ALL",
                null,
                testServices.realmContext(),
                testServices.securityContext())) {
      assertThat(loaded.readEntity(LoadTableResponse.class).tableMetadata().properties())
          .doesNotContainKey(propertyName);
    }
  }

  /**
   * Two tables in ONE transaction claiming the same fresh directory must be refused. Each table's
   * own overlap validation resolves siblings from committed rows, so each sees the other still at
   * its old location and passes. Nothing afterwards re-examines the request, and the single commit
   * carries a condition on each table's version but none on either location — so without a check
   * over the request itself both would land, leaving two tables at one location with no ordering
   * that explains it and no writer that lost a race.
   */
  @Test
  void testCommitTransactionWithTwoTablesClaimingOneLocationIsRefused() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(
        testServices,
        Map.of(
            FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(),
            "true",
            FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
            "true"),
        catalogLocation);

    String table1Name = "test-table-11";
    String table2Name = "test-table-12";
    createTable(testServices, table1Name, catalogLocation);
    createTable(testServices, table2Name, catalogLocation);

    // One directory, claimed by both tables in the same request. It sits outside either table's own
    // location, which is why the catalog allows external table locations here.
    String sharedDataLocation =
        String.format("%s/%s/shared-data/%s", catalogLocation, catalog, UUID.randomUUID());

    CommitTransactionRequest request =
        new CommitTransactionRequest(
            List.of(
                UpdateTableRequest.create(
                    TableIdentifier.of(namespace, table1Name),
                    List.of(),
                    List.of(
                        new MetadataUpdate.SetProperties(
                            Map.of(
                                IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY,
                                sharedDataLocation)))),
                UpdateTableRequest.create(
                    TableIdentifier.of(namespace, table2Name),
                    List.of(),
                    List.of(
                        new MetadataUpdate.SetProperties(
                            Map.of(
                                IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY,
                                sharedDataLocation))))));

    assertThatThrownBy(
            () ->
                testServices
                    .restApi()
                    .commitTransaction(
                        catalog,
                        request,
                        IDEMPOTENCY_KEY,
                        testServices.realmContext(),
                        testServices.securityContext()))
        .isInstanceOf(ForbiddenException.class);

    // Neither table kept the contested location: the whole transaction was refused, not just the
    // second table's half of it.
    assertTableDataLocationIsNot(testServices, table1Name, sharedDataLocation);
    assertTableDataLocationIsNot(testServices, table2Name, sharedDataLocation);
  }

  private void assertTableDataLocationIsNot(
      TestServices services, String tableName, String location) {
    try (Response loaded =
        services
            .restApi()
            .loadTable(
                catalog,
                namespace,
                tableName,
                null,
                null,
                "ALL",
                null,
                services.realmContext(),
                services.securityContext())) {
      assertThat(loaded.readEntity(LoadTableResponse.class).tableMetadata().properties())
          .doesNotContainEntry(
              IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY, location);
    }
  }

  private void createCatalogAndNamespace(
      TestServices services, Map<String, String> catalogConfig, String catalogLocation) {
    CatalogProperties.Builder propertiesBuilder =
        CatalogProperties.builder()
            .setDefaultBaseLocation(String.format("%s/%s", catalogLocation, catalog))
            .putAll(catalogConfig);

    StorageConfigInfo config =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .build();
    Catalog catalogObject =
        new Catalog(
            Catalog.TypeEnum.INTERNAL, catalog, propertiesBuilder.build(), 0L, 0L, 1, config);
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalogObject),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }

    CreateNamespaceRequest createNamespaceRequest =
        CreateNamespaceRequest.builder().withNamespace(Namespace.of(namespace)).build();
    try (Response response =
        services
            .restApi()
            .createNamespace(
                catalog,
                createNamespaceRequest,
                IDEMPOTENCY_KEY,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  /**
   * A literal {@code SetLocation} update inside a multi-table transaction must commit. This is the
   * wire form the retired pre-check refused outright with a 400; a REST client can send it, since
   * Iceberg's metadata-update parser has a {@code set-location} action. The relocated table does
   * not contend with the other table's location, so the intra-request check has nothing to refuse
   * here.
   */
  @Test
  void testCommitTransactionWithLiteralSetLocationSucceeds() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(
        testServices,
        Map.of(
            FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(),
            "true",
            FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
            "true"),
        catalogLocation);

    String table1Name = "test-table-13";
    String table2Name = "test-table-14";
    createTable(testServices, table1Name, catalogLocation);
    createTable(testServices, table2Name, catalogLocation);

    String movedLocation =
        String.format("%s/%s/%s/moved-%s", catalogLocation, catalog, namespace, UUID.randomUUID());

    CommitTransactionRequest request =
        new CommitTransactionRequest(
            List.of(
                UpdateTableRequest.create(
                    TableIdentifier.of(namespace, table1Name),
                    List.of(),
                    List.of(new MetadataUpdate.SetLocation(movedLocation))),
                UpdateTableRequest.create(
                    TableIdentifier.of(namespace, table2Name),
                    List.of(),
                    List.of(new MetadataUpdate.SetProperties(Map.of(propertyName, "value2"))))));

    try (Response response =
        testServices
            .restApi()
            .commitTransaction(
                catalog,
                request,
                IDEMPOTENCY_KEY,
                testServices.realmContext(),
                testServices.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    // Read the table back, so this proves the new location was persisted and not merely accepted.
    try (Response loaded =
        testServices
            .restApi()
            .loadTable(
                catalog,
                namespace,
                table1Name,
                null,
                null,
                "ALL",
                null,
                testServices.realmContext(),
                testServices.securityContext())) {
      assertThat(loaded.readEntity(LoadTableResponse.class).tableMetadata().location())
          .isEqualTo(movedLocation);
    }
  }

  private void createTable(TestServices services, String tableName, String baseLocation) {
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder()
            .withName(tableName)
            .withLocation(String.format("%s/%s/%s/%s", baseLocation, catalog, namespace, tableName))
            .withSchema(SCHEMA)
            .build();
    services
        .restApi()
        .createTable(
            catalog,
            namespace,
            createTableRequest,
            null,
            IDEMPOTENCY_KEY,
            services.realmContext(),
            services.securityContext());
  }

  /**
   * A store that stopped short of a verdict but can prove nothing was written becomes the Iceberg
   * word for a failed commit, which tells the client a retry is safe.
   */
  @Test
  void testDisruptedCommitProvenEmptyIsReportedAsFailed() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);
    createTable(testServices, "test-table-15", catalogLocation);
    createTable(testServices, "test-table-16", catalogLocation);

    assertThatThrownBy(
            () ->
                commitTransactionThrough(
                    testServices,
                    new CommitDisruptedException(
                        CommitDisruptedException.DurableEffect.NONE, "the store rolled back"),
                    generateCommitTransactionRequest(false, "test-table-15", "test-table-16")))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("the store rolled back")
        .hasCauseInstanceOf(CommitDisruptedException.class);
  }

  /**
   * A store that cannot prove either way must NOT be reported as a failure: the mutations may be in
   * storage, so the client must not retry as though nothing had happened.
   */
  @Test
  void testDisruptedCommitOfUnknownEffectIsReportedAsStateUnknown() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);
    createTable(testServices, "test-table-17", catalogLocation);
    createTable(testServices, "test-table-18", catalogLocation);

    assertThatThrownBy(
            () ->
                commitTransactionThrough(
                    testServices,
                    new CommitDisruptedException(
                        CommitDisruptedException.DurableEffect.UNKNOWN, "the connection dropped"),
                    generateCommitTransactionRequest(false, "test-table-17", "test-table-18")))
        .isInstanceOf(CommitStateUnknownException.class)
        .hasCauseInstanceOf(CommitDisruptedException.class);
  }

  /**
   * A batch the store refuses as too large is a client error, not a conflict. Reported as a
   * conflict it would be a 409 the Iceberg client retries, and the identical request can never fit.
   */
  @Test
  void testBatchRefusedAsTooLargeIsReportedAsAClientError() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);
    createTable(testServices, "test-table-19", catalogLocation);
    createTable(testServices, "test-table-20", catalogLocation);

    assertThatThrownBy(
            () ->
                commitTransactionThrough(
                    testServices,
                    new TooManyItemsException("the store declined the batch as too large"),
                    generateCommitTransactionRequest(false, "test-table-19", "test-table-20")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("declined the batch as too large")
        .hasCauseInstanceOf(TooManyItemsException.class);
  }

  /** Creates TestServices with event delegator enabled for event testing. */
  private TestServices createTestServices() {
    Map<String, Object> config =
        Map.of(
            "ALLOW_INSECURE_STORAGE_TYPES",
            "true",
            "SUPPORTED_CATALOG_STORAGE_TYPES",
            List.of("FILE"));
    return TestServices.builder()
        .config(config)
        .withEventDelegator(true) // Enable event delegator
        .build();
  }

  /**
   * Issues {@code request} through a catalog assembled the way the local catalog factory assembles
   * one, except that the single batch commit the handler performs throws {@code failure}. The REST
   * adapter builds its own catalog per call and offers no seam for injecting a store fault, so the
   * fault goes in one layer up, at the durable manager the handler commits through; everything else
   * on that manager still runs against the real one, so resolution and validation stay live.
   */
  private void commitTransactionThrough(
      TestServices services, RuntimeException failure, CommitTransactionRequest request)
      throws Exception {
    DurableManager faultyOnTheBatchCommit =
        Mockito.mock(
            DurableManager.class, AdditionalAnswers.delegatesTo(services.metaStoreManager()));
    Mockito.doThrow(failure)
        .when(faultyOnTheBatchCommit)
        .updateEntitiesPropertiesIfNotChanged(any(), any());

    PolarisAuthorizer authorizer = Mockito.mock(PolarisAuthorizer.class);
    Mockito.when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenReturn(AuthorizationDecision.allow());
    Mockito.when(authorizer.authorize(any(), any(), any(), any(), any()))
        .thenReturn(AuthorizationDecision.allow());

    @SuppressWarnings("unchecked")
    Instance<FederatedCatalogFactory> federatedCatalogFactories = Mockito.mock(Instance.class);
    Mockito.when(federatedCatalogFactories.isUnsatisfied()).thenReturn(true);

    try (PolarisIcebergCatalog icebergCatalog =
        new PolarisIcebergCatalog(
            catalog,
            services.principal(),
            services.newCallContext(),
            services.polarisDiagnostics(),
            services.entityResolver(),
            authorizer,
            faultyOnTheBatchCommit,
            services.taskExecutor(),
            services.storageAccessConfigProvider(),
            services.fileIOFactory(),
            services.polarisEventDispatcher(),
            services.eventMetadataFactory(),
            Mockito.mock(PolarisCredentialManager.class),
            federatedCatalogFactories,
            ReservedProperties.NONE,
            new CatalogHandlerUtils(services.realmConfig()),
            new EventAttributeMap(),
            services.clock(),
            new DefaultAccessDelegationModeResolver(services.realmConfig()),
            new DefaultMetricsReporter(),
            new DefaultCatalogPrefixParser())) {
      icebergCatalog.commitTransaction(request);
    }
  }

  /**
   * Executes a transaction test with the specified parameters.
   *
   * @param table1Name name of the first table
   * @param table2Name name of the second table
   * @param testServices TestServices object that will be operated on
   */
  private void executeTransactionTest(
      boolean shouldFail, String table1Name, String table2Name, TestServices testServices) {
    // Set up the test tables
    createTable(testServices, table1Name, catalogLocation);
    createTable(testServices, table2Name, catalogLocation);

    // Ignore any errors that occur during transaction commit
    try {
      testServices
          .restApi()
          .commitTransaction(
              catalog,
              generateCommitTransactionRequest(shouldFail, table1Name, table2Name),
              IDEMPOTENCY_KEY,
              testServices.realmContext(),
              testServices.securityContext());
    } catch (Exception ignored) {
    }
  }

  private CommitTransactionRequest generateCommitTransactionRequest(
      boolean shouldFail, String table1Name, String table2Name) {
    List<UpdateRequirement> updateRequirements;
    if (shouldFail) {
      // Schema ID does not exist, therefore call will fail
      updateRequirements = List.of(new UpdateRequirement.AssertCurrentSchemaID(-1));
    } else {
      updateRequirements = List.of();
    }
    return new CommitTransactionRequest(
        List.of(
            UpdateTableRequest.create(
                TableIdentifier.of(namespace, table1Name),
                updateRequirements,
                List.of(new MetadataUpdate.SetProperties(Map.of(propertyName, "value1")))),
            UpdateTableRequest.create(
                TableIdentifier.of(namespace, table2Name),
                updateRequirements,
                List.of(new MetadataUpdate.SetProperties(Map.of(propertyName, "value2"))))));
  }
}
