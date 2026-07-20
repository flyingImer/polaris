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

/**
 * The iceberg-table-catalog L0 feature SPI (ADR-0001): the provider-replaceable contract for the
 * Iceberg REST catalog's namespace + table operations, plus the credential-serving,
 * metrics-reporting, and notification operations that ride the table path.
 *
 * <p>Every operation returns its plain Iceberg response wrapped in {@link PolarisResult} (Issue
 * 29): {@code body} = the Iceberg response ({@code Void} for no-body ops), plus a typed
 * provider-private {@code extension} of type {@code E}, used to carry operation metadata the
 * Iceberg response cannot express. Operation inputs stay plain parameters — there is no
 * request-side generic wrapper.
 *
 * <p>{@code loadTable}, {@code createTableDirect}, and {@code registerTable} are the operations
 * with a per-call OSS ETag (pre-PoC OSS requirement, #1037): the OSS default implementation binds
 * {@code E = }{@link ETagPayload} (an {@link ETagCarrier}) so it can carry one. {@code loadTable}
 * returns a plain {@link PolarisResult} with a nullable {@code body} -- {@code null} means the
 * caller's cached copy is still current (maps to HTTP 304), a populated body means it was loaded
 * (maps to 200) -- while {@code extension} is always populated with the ETag in both cases, so the
 * not-modified response still carries it. (An earlier design used a dedicated sealed {@code
 * ConditionalLoadOutcome} type for this instead of a nullable body; retired -- see Issue 32 -- once
 * ETag moved into the always-present {@code extension}, nothing distinguishing was left to lose by
 * collapsing to a plain, nullable-body {@code PolarisResult}.) {@code createTableDirect}/{@code
 * registerTable} always return a populated body, so they carry the ETag the same way without the
 * nullable-body case. No other operation on this interface needs a per-call ETag; a provider fills
 * {@code E} with its own marker-interface type, composing {@link ETagCarrier} too if it needs to
 * carry an ETag on more than these three.
 *
 * <p>Each method defaults to throwing {@link UnsupportedOperationException} so a provider need only
 * implement the operations it serves (mirroring Iceberg's own {@code default}-throw pattern on
 * {@code Catalog.registerTable}/{@code buildTable}).
 *
 * <p>Method names match the OSS Iceberg-REST operationIds directly ({@code namespaceExists}, {@code
 * tableExists}, {@code dropNamespace}, {@code loadNamespaceMetadata}, {@code sendNotification}).
 * These 5 were temporarily renamed off their natural names ({@code checkNamespaceExists}, {@code
 * checkTableExists}, {@code deleteNamespace}, {@code getNamespaceMetadata}, {@code
 * submitNotification}) during Issue 29 Rework R1-R3, to avoid a compile-blocking signature
 * collision (same name + params, different return type) while the OSS default implementation (then
 * named {@code LocalIcebergCatalog}) still extended Iceberg's {@code
 * BaseMetastoreViewCatalog}/{@code SupportsNamespaces}/{@code SupportsNotifications} directly.
 * Rework R4 removed that inheritance (composing a separate Iceberg-mechanics delegate instead, now
 * {@code BridgeBaseMetastoreViewCatalog}), which permanently removes the collision, so the natural
 * names are reverted here for good (the 3 view-side collisions are on {@link
 * IcebergViewCatalogOps}).
 *
 * <p>Moved to {@code polaris-core} in Issue 29 Rework S Stage S1, now that {@code
 * NotificationRequest} (the one dependency that previously required living in {@code
 * api-iceberg-service}) has moved here too. The OSS default implementation of this interface is
 * {@code BasePolarisIcebergCatalog} (generic over {@code E}, with the OSS default concrete subclass
 * {@code PolarisIcebergCatalog} instantiating {@code E = ETagPayload}), relocated and renamed into
 * {@code extensions/catalog/iceberg-default} in Stage S3b -- it implements this interface directly
 * rather than extending an intermediate base (the {@code BasePolarisCatalog} empty abstract base
 * this javadoc previously pointed to was retired in that same stage as unneeded: no second
 * implementation of this interface ever materialized to justify it).
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

  default PolarisResult<GetNamespaceResponse, E> loadNamespaceMetadata(Namespace namespace) {
    throw unsupported("loadNamespaceMetadata");
  }

  default PolarisResult<Void, E> namespaceExists(Namespace namespace) {
    throw unsupported("namespaceExists");
  }

  default PolarisResult<Void, E> dropNamespace(Namespace namespace) {
    throw unsupported("dropNamespace");
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

  /**
   * Loads a table, honoring {@code ifNoneMatch} for conditional-load (Issue 32). The returned
   * {@code body} is {@code null} when the caller's cached copy is still current (not-modified, HTTP
   * 304); {@code extension} is always populated with the ETag either way (see the interface
   * javadoc).
   */
  default PolarisResult<LoadTableResponse, E> loadTable(
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

  default PolarisResult<Void, E> tableExists(TableIdentifier tableIdentifier) {
    throw unsupported("tableExists");
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

  // NotificationRequest lives in polaris-core (org.apache.polaris.spi.feature.catalog) purely
  // because this signature needs it here. Whether polaris-core is the right long-term home for a
  // Polaris-owned, OpenAPI-generated, IRC-spec-shaped wire type (as opposed to a framework-agnostic
  // domain type) is an open question shared with a real apache/polaris upstream discussion
  // (PR #5052, IRC-spec-scoped-but-framework-agnostic code has no clean tier in core/extensions/
  // runtime) -- see Issue 30. This placement is interim, not a considered final answer.
  default PolarisResult<Boolean, E> sendNotification(
      TableIdentifier identifier, NotificationRequest request) {
    throw unsupported("sendNotification");
  }

  default PolarisResult<ConfigResponse, E> getConfig() {
    throw unsupported("getConfig");
  }
}
