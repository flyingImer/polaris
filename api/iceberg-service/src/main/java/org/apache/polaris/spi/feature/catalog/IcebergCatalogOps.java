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
package org.apache.polaris.spi.feature.catalog;

import java.util.EnumSet;
import java.util.Optional;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.polaris.service.types.NotificationRequest;

/**
 * The iceberg-table-catalog L0 feature SPI (ADR-0001): the provider-replaceable contract for the
 * Iceberg REST catalog's namespace + table operations, plus the credential-serving,
 * metrics-reporting, and notification operations that ride the table path.
 *
 * <p>Every operation returns its plain Iceberg response wrapped in {@link PolarisResult} (Issue
 * 29): {@code body} = the Iceberg response ({@code Void} for no-body ops), plus the OSS-carried
 * {@code etag} and a typed provider-private {@code extension} of type {@code E}. The OSS default
 * implementation uses {@code E = }{@link NoExtension}; a provider fills {@code E} with its own
 * marker-interface type to carry operation metadata the Iceberg response cannot express. Operation
 * inputs stay plain parameters — there is no request-side generic wrapper. {@code loadTable}
 * returns {@link ConditionalLoadOutcome} to model the If-None-Match/304 conditional-load case.
 *
 * <p>Each method defaults to throwing {@link UnsupportedOperationException} so a provider need only
 * implement the operations it serves (mirroring Iceberg's own {@code default}-throw pattern on
 * {@code Catalog.registerTable}/{@code buildTable}).
 *
 * <p>8 methods below are named off the OSS Iceberg-REST operationIds to avoid a compile-blocking
 * signature collision (same name + params, different return type) on the current OSS default
 * implementation {@code LocalIcebergCatalog}, which still {@code extends BaseMetastoreViewCatalog}
 * pending the Issue 29 Rework R composite restructure (R4). Once {@code LocalIcebergCatalog} stops
 * inheriting from Iceberg's {@code Catalog}/{@code SupportsNamespaces}/{@code
 * SupportsNotifications}, these revert to the natural REST names ({@code
 * checkNamespaceExists→namespaceExists}, {@code checkTableExists→tableExists}, {@code
 * deleteNamespace→dropNamespace}, {@code getNamespaceMetadata→loadNamespaceMetadata}, {@code
 * submitNotification→sendNotification}; the 3 view-side collisions are on {@link
 * IcebergViewCatalogOps}).
 *
 * <p>Lives in {@code api-iceberg-service} (not bare {@code polaris-core}) because {@link
 * #submitNotification} needs {@link NotificationRequest}, a Polaris-owned OpenAPI-generated type
 * that this module owns; {@code api-iceberg-service} depends on {@code polaris-core} (never the
 * reverse), so this is the lowest layer that can see both the Iceberg SDK types, the generic {@link
 * PolarisResult}/{@link ExtensionPayload} wrapper types, and {@link NotificationRequest} together.
 *
 * @param <E> the provider-private extension payload type
 */
public interface IcebergCatalogOps<E extends ExtensionPayload> {

  private static UnsupportedOperationException unsupported(String op) {
    return new UnsupportedOperationException(
        op + " is not supported by this catalog feature-SPI implementation");
  }

  default PolarisResult<ListNamespacesResponse, E> listNamespaces(
      Namespace parent, String pageToken, Integer pageSize) {
    throw unsupported("listNamespaces");
  }

  default PolarisResult<CreateNamespaceResponse, E> createNamespace(
      CreateNamespaceRequest request) {
    throw unsupported("createNamespace");
  }

  default PolarisResult<GetNamespaceResponse, E> getNamespaceMetadata(Namespace namespace) {
    throw unsupported("getNamespaceMetadata");
  }

  default PolarisResult<Void, E> checkNamespaceExists(Namespace namespace) {
    throw unsupported("checkNamespaceExists");
  }

  default PolarisResult<Void, E> deleteNamespace(Namespace namespace) {
    throw unsupported("deleteNamespace");
  }

  default PolarisResult<UpdateNamespacePropertiesResponse, E> updateNamespaceProperties(
      Namespace namespace, UpdateNamespacePropertiesRequest request) {
    throw unsupported("updateNamespaceProperties");
  }

  default PolarisResult<ListTablesResponse, E> listTables(
      Namespace namespace, String pageToken, Integer pageSize) {
    throw unsupported("listTables");
  }

  default PolarisResult<LoadTableResponse, E> createTableDirect(
      Namespace namespace,
      CreateTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint) {
    throw unsupported("createTableDirect");
  }

  default PolarisResult<LoadTableResponse, E> createTableStaged(
      Namespace namespace,
      CreateTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint) {
    throw unsupported("createTableStaged");
  }

  default PolarisResult<LoadTableResponse, E> registerTable(
      Namespace namespace,
      RegisterTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint) {
    throw unsupported("registerTable");
  }

  default ConditionalLoadOutcome<LoadTableResponse, E> loadTable(
      TableIdentifier tableIdentifier,
      String snapshots,
      IfNoneMatch ifNoneMatch,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint) {
    throw unsupported("loadTable");
  }

  default PolarisResult<LoadTableResponse, E> updateTable(
      TableIdentifier tableIdentifier, UpdateTableRequest request) {
    throw unsupported("updateTable");
  }

  default PolarisResult<LoadTableResponse, E> updateTableForStagedCreate(
      TableIdentifier tableIdentifier, UpdateTableRequest request) {
    throw unsupported("updateTableForStagedCreate");
  }

  default PolarisResult<Void, E> dropTableWithoutPurge(TableIdentifier tableIdentifier) {
    throw unsupported("dropTableWithoutPurge");
  }

  default PolarisResult<Void, E> dropTableWithPurge(TableIdentifier tableIdentifier) {
    throw unsupported("dropTableWithPurge");
  }

  default PolarisResult<Void, E> checkTableExists(TableIdentifier tableIdentifier) {
    throw unsupported("checkTableExists");
  }

  default PolarisResult<Void, E> renameTable(RenameTableRequest request) {
    throw unsupported("renameTable");
  }

  default PolarisResult<Void, E> commitTransaction(
      CommitTransactionRequest commitTransactionRequest) {
    throw unsupported("commitTransaction");
  }

  default PolarisResult<ImmutableLoadCredentialsResponse, E> loadCredentials(
      TableIdentifier tableIdentifier, Optional<String> refreshCredentialsEndpoint) {
    throw unsupported("loadCredentials");
  }

  default PolarisResult<Void, E> reportMetrics(
      TableIdentifier identifier, ReportMetricsRequest request) {
    throw unsupported("reportMetrics");
  }

  default PolarisResult<Boolean, E> submitNotification(
      TableIdentifier identifier, NotificationRequest request) {
    throw unsupported("submitNotification");
  }

  default PolarisResult<ConfigResponse, E> getConfig() {
    throw unsupported("getConfig");
  }
}
