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
package org.apache.polaris.core.events;

import com.google.common.reflect.TypeToken;
import java.util.List;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * The event attribute keys shared by the Iceberg catalog default implementation ({@code
 * org.apache.polaris.extension.catalog.iceberg}) and the runtime/service event listeners. Split out
 * of {@code org.apache.polaris.service.events.EventAttributes} because that class also carries
 * OpenAPI-generated request/response types owned by modules the Iceberg catalog extension cannot
 * depend on without creating a module cycle; these four keys only reference Iceberg SDK + JDK
 * types, so they live in core instead.
 */
public final class IcebergEventAttributes {
  private IcebergEventAttributes() {}

  public static final AttributeKey<String> CATALOG_NAME =
      new AttributeKey<>("catalog_name", String.class);
  public static final AttributeKey<TableIdentifier> TABLE_IDENTIFIER =
      new AttributeKey<>("table_identifier", TableIdentifier.class);
  // Used internally only. Not for external usage.
  public static final AttributeKey<List<TableMetadata>> TABLE_METADATAS =
      new AttributeKey<>("table_metadatas", new TypeToken<>() {});
  public static final AttributeKey<TableIdentifier> VIEW_IDENTIFIER =
      new AttributeKey<>("view_identifier", TableIdentifier.class);
}
