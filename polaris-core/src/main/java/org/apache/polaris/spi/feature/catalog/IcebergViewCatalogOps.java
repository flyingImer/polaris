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

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterViewRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;

/**
 * The iceberg-view-catalog L0 feature SPI (ADR-0001): the provider-replaceable contract for the
 * Iceberg REST catalog's view operations.
 *
 * <p>Every operation returns its plain Iceberg response wrapped in {@link PolarisResult} (Issue
 * 29), with the OSS-carried {@code etag} and a typed provider-private {@code extension} of type
 * {@code E} (OSS default {@code E = }{@link NoExtension}). Inputs stay plain parameters. {@code
 * listViews} returns a {@link ListTablesResponse} (Iceberg's own view-listing response type). Each
 * method defaults to throwing {@link UnsupportedOperationException} so a provider need only
 * implement the operations it serves.
 *
 * <p>Method names match the OSS Iceberg-REST operationIds directly ({@code viewExists}, {@code
 * dropView}, {@code loadView}). These 3 were temporarily renamed off their natural names ({@code
 * checkViewExists}, {@code deleteView}, {@code getView}) during Issue 29 Rework R1-R3, to avoid a
 * compile-blocking signature collision on the OSS default implementation {@code
 * LocalIcebergCatalog}, which still {@code extends BaseMetastoreViewCatalog implements ViewCatalog}
 * at the time. Rework R4 removed that inheritance (composing a separate {@code
 * PolarisIcebergCatalog} Iceberg-mechanics delegate instead via {@link BasePolarisCatalog}), which
 * permanently removes the collision, so the natural names are reverted here for good (see {@link
 * IcebergCatalogOps} for the 5 table/namespace-side ones).
 *
 * @param <E> the provider-private extension payload type
 */
public interface IcebergViewCatalogOps<E extends ExtensionPayload> {

  private static UnsupportedOperationException unsupported(String op) {
    return new UnsupportedOperationException(
        op + " is not supported by this catalog feature-SPI implementation");
  }

  default PolarisResult<ListTablesResponse, E> listViews(
      Namespace namespace, String pageToken, Integer pageSize) {
    throw unsupported("listViews");
  }

  default PolarisResult<LoadViewResponse, E> createView(
      Namespace namespace, CreateViewRequest request) {
    throw unsupported("createView");
  }

  default PolarisResult<LoadViewResponse, E> registerView(
      Namespace namespace, RegisterViewRequest request) {
    throw unsupported("registerView");
  }

  default PolarisResult<LoadViewResponse, E> loadView(TableIdentifier viewIdentifier) {
    throw unsupported("loadView");
  }

  default PolarisResult<LoadViewResponse, E> replaceView(
      TableIdentifier viewIdentifier, UpdateTableRequest request) {
    throw unsupported("replaceView");
  }

  default PolarisResult<Void, E> dropView(TableIdentifier viewIdentifier) {
    throw unsupported("dropView");
  }

  default PolarisResult<Void, E> viewExists(TableIdentifier viewIdentifier) {
    throw unsupported("viewExists");
  }

  default PolarisResult<Void, E> renameView(RenameTableRequest request) {
    throw unsupported("renameView");
  }
}
