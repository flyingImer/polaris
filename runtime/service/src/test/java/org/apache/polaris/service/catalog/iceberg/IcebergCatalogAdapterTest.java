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

import com.google.common.base.Strings;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.BearerAuthenticationParameters;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.ConnectionConfigInfo;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.IcebergRestConnectionConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.extension.catalog.iceberg.BasePolarisIcebergCatalog;
import org.apache.polaris.extension.catalog.iceberg.PolarisIcebergCatalog;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.spi.feature.catalog.ConditionalLoadOutcome;
import org.apache.polaris.spi.feature.catalog.ExtensionPayload;
import org.apache.polaris.spi.feature.catalog.NoExtension;
import org.apache.polaris.spi.feature.catalog.PolarisResult;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

public class IcebergCatalogAdapterTest {

  private static final String FEDERATED_CATALOG_NAME = "polaris-federated-catalog";

  private TestServices testServices;
  private IcebergCatalogAdapter catalogAdapter;

  @BeforeEach
  public void setUp() {
    // Set up test services with catalog federation enabled
    testServices =
        TestServices.builder().config(Map.of("ENABLE_CATALOG_FEDERATION", "true")).build();
    catalogAdapter = Mockito.spy(testServices.catalogAdapter());

    // Prepare storage and connection configs for a federated Iceberg REST catalog
    String storageLocation = "s3://my-bucket/path/to/data";
    AwsStorageConfigInfo storageConfig =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/polaris-user-role")
            .setExternalId("externalId")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(storageLocation, "s3://externally-owned-bucket"))
            .build();

    AuthenticationParameters authParams =
        BearerAuthenticationParameters.builder()
            .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.BEARER)
            .setBearerToken("xxx")
            .build();

    IcebergRestConnectionConfigInfo connectionConfig =
        IcebergRestConnectionConfigInfo.builder()
            .setConnectionType(ConnectionConfigInfo.ConnectionTypeEnum.ICEBERG_REST)
            .setAuthenticationParameters(authParams)
            .setUri("http://localhost:8080/api/v1/catalogs")
            .setRemoteCatalogName("remote-catalog")
            .build();

    // Register the catalog in the test environment
    testServices
        .catalogsApi()
        .createCatalog(
            new CreateCatalogRequest(
                ExternalCatalog.builder()
                    .setName(FEDERATED_CATALOG_NAME)
                    .setProperties(
                        CatalogProperties.builder().setDefaultBaseLocation(storageLocation).build())
                    .setConnectionConfigInfo(connectionConfig)
                    .setStorageConfigInfo(storageConfig)
                    .build()),
            testServices.realmContext(),
            testServices.securityContext());
  }

  @ParameterizedTest(name = "[{index}] initialPageToken={0}, pageSize={1}")
  @MethodSource("paginationTestCases")
  void testPaginationForFederatedCatalog(String initialPageToken, Integer pageSize)
      throws IOException {

    try (InMemoryCatalog inMemoryCatalog = new InMemoryCatalog()) {
      // Initialize and replace the default handler with one backed by in-memory catalog
      inMemoryCatalog.initialize("inMemory", Map.of());
      mockCatalogAdapter(inMemoryCatalog);

      // Set up 10 entities in the catalog: 10 namespaces, 10 tables, 10 views
      int entityCount = 10;
      for (int i = 0; i < entityCount; ++i) {
        inMemoryCatalog.createNamespace(Namespace.of("ns" + i));
        inMemoryCatalog.createTable(TableIdentifier.of("ns0", "table" + i), new Schema());
        inMemoryCatalog
            .buildView(TableIdentifier.of("ns0", "view" + i))
            .withSchema(new Schema())
            .withDefaultNamespace(Namespace.of("ns0"))
            .withQuery("a", "SELECT * FROM ns0.table" + i)
            .create();
      }

      // Determine starting index for pagination based on the initial page token
      int pageStart =
          Strings.isNullOrEmpty(initialPageToken) ? 0 : Integer.parseInt(initialPageToken);
      int remain = entityCount - pageStart;

      // Initial tokens for pagination
      String listNamespacePageToken = initialPageToken;
      String listTablesPageToken = initialPageToken;
      String listViewsPageToken = initialPageToken;

      // Simulate page-by-page fetching until all entities are consumed
      while (remain > 0) {
        int expectedSize =
            (pageSize != null && initialPageToken != null) ? Math.min(remain, pageSize) : remain;

        // Verify namespaces pagination
        ListNamespacesResponse namespacesResponse =
            (ListNamespacesResponse)
                catalogAdapter
                    .listNamespaces(
                        FEDERATED_CATALOG_NAME,
                        listNamespacePageToken,
                        pageSize,
                        null,
                        testServices.realmContext(),
                        testServices.securityContext())
                    .getEntity();
        Assertions.assertThat(namespacesResponse.namespaces()).hasSize(expectedSize);
        listNamespacePageToken = namespacesResponse.nextPageToken();

        // Verify tables pagination
        ListTablesResponse tablesResponse =
            (ListTablesResponse)
                catalogAdapter
                    .listTables(
                        FEDERATED_CATALOG_NAME,
                        "ns0",
                        listTablesPageToken,
                        pageSize,
                        testServices.realmContext(),
                        testServices.securityContext())
                    .getEntity();
        Assertions.assertThat(tablesResponse.identifiers()).hasSize(expectedSize);
        listTablesPageToken = tablesResponse.nextPageToken();

        // Verify views pagination
        ListTablesResponse viewsResponse =
            (ListTablesResponse)
                catalogAdapter
                    .listViews(
                        FEDERATED_CATALOG_NAME,
                        "ns0",
                        listViewsPageToken,
                        pageSize,
                        testServices.realmContext(),
                        testServices.securityContext())
                    .getEntity();
        Assertions.assertThat(viewsResponse.identifiers()).hasSize(expectedSize);
        listViewsPageToken = viewsResponse.nextPageToken();

        remain -= expectedSize;
      }
    }
  }

  private void mockCatalogAdapter(org.apache.iceberg.catalog.Catalog catalog) {
    // Override catalog creation to inject the in-memory catalog as the federated delegate and
    // suppress the real close(). Presetting baseInitialized short-circuits the merged catalog's
    // lazy ensureBaseInitialized() so it does not overwrite the injected fields with a real
    // federated catalog during the post-authorization init.
    Mockito.doAnswer(
            invocation -> {
              PolarisIcebergCatalog realCatalog =
                  (PolarisIcebergCatalog) invocation.callRealMethod();
              PolarisIcebergCatalog wrappedCatalog = Mockito.spy(realCatalog);

              // Inject test catalog + federated flag using reflection. baseCatalog /
              // namespaceCatalog / viewCatalog / isFederated / baseInitialized are declared
              // directly on the generic base BasePolarisIcebergCatalog, not on this trivial
              // concrete subclass, so the declaring-class literal for getDeclaredField must be
              // the base class even though the instance being mutated is the concrete subclass.
              for (String fieldName : List.of("baseCatalog", "namespaceCatalog", "viewCatalog")) {
                Field field = BasePolarisIcebergCatalog.class.getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(wrappedCatalog, catalog);
              }
              Field federatedField =
                  BasePolarisIcebergCatalog.class.getDeclaredField("isFederated");
              federatedField.setAccessible(true);
              federatedField.set(wrappedCatalog, true);
              Field baseInitializedField =
                  BasePolarisIcebergCatalog.class.getDeclaredField("baseInitialized");
              baseInitializedField.setAccessible(true);
              baseInitializedField.set(wrappedCatalog, true);

              // Prevent catalog from being closed during test lifecycle
              Mockito.doNothing().when(wrappedCatalog).close();

              return wrappedCatalog;
            })
        .when(catalogAdapter)
        .newCatalog(Mockito.any(), Mockito.any());
  }

  private static Stream<Arguments> paginationTestCases() {
    return Stream.of(
        Arguments.of(null, null),
        Arguments.of(null, 1),
        Arguments.of(null, 3),
        Arguments.of(null, 5),
        Arguments.of(null, 10),
        Arguments.of(null, 20),
        Arguments.of("", null),
        Arguments.of("", 1),
        Arguments.of("", 3),
        Arguments.of("", 5),
        Arguments.of("", 10),
        Arguments.of("", 20),
        Arguments.of("5", null),
        Arguments.of("5", 1),
        Arguments.of("5", 3),
        Arguments.of("5", 5),
        Arguments.of("5", 10));
  }

  @Test
  void toLoadTableResponse_loadedMapsTo200WithEtagHeader() {
    LoadTableResponse body = Mockito.mock(LoadTableResponse.class);

    Response response =
        IcebergCatalogAdapter.toLoadTableResponse(
            new ConditionalLoadOutcome.Loaded<>(
                new PolarisResult<>(body, Optional.of("W/\"etag-1\""), NoExtension.INSTANCE)));

    Assertions.assertThat(response.getStatus()).isEqualTo(200);
    Assertions.assertThat(response.getHeaderString(HttpHeaders.ETAG)).isEqualTo("W/\"etag-1\"");
    Assertions.assertThat(response.getEntity()).isSameAs(body);
  }

  @Test
  void toLoadTableResponse_notModifiedMapsTo304WithEtagHeader() {
    Response response =
        IcebergCatalogAdapter.toLoadTableResponse(
            new ConditionalLoadOutcome.NotModified<LoadTableResponse, NoExtension>(
                Optional.of("W/\"etag-1\""), NoExtension.INSTANCE));

    Assertions.assertThat(response.getStatus()).isEqualTo(304);
    Assertions.assertThat(response.getHeaderString(HttpHeaders.ETAG)).isEqualTo("W/\"etag-1\"");
  }

  @Test
  void toLoadTableResponse_neverReadsProviderPayload() {
    LoadTableResponse body = Mockito.mock(LoadTableResponse.class);
    // A poison provider extension whose access throws. The adapter reads only the OSS-carried
    // etag, never the extension, so mapping must succeed identically with or without it.
    ExtensionPayload sentinelExtension =
        new ExtensionPayload() {
          @Override
          public String toString() {
            throw new AssertionError("provider extension must never be read by the adapter");
          }
        };

    Response withSentinel =
        IcebergCatalogAdapter.toLoadTableResponse(
            new ConditionalLoadOutcome.Loaded<>(
                new PolarisResult<>(body, Optional.of("W/\"etag-1\""), sentinelExtension)));
    Response withoutPayload =
        IcebergCatalogAdapter.toLoadTableResponse(
            new ConditionalLoadOutcome.Loaded<>(
                new PolarisResult<>(body, Optional.of("W/\"etag-1\""), NoExtension.INSTANCE)));

    Assertions.assertThat(withSentinel.getStatus()).isEqualTo(withoutPayload.getStatus());
    Assertions.assertThat(withSentinel.getHeaderString(HttpHeaders.ETAG))
        .isEqualTo(withoutPayload.getHeaderString(HttpHeaders.ETAG));
  }
}
