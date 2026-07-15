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

/**
 * The reusable base type for an Iceberg catalog feature-SPI implementation (Issue 29 Rework R).
 *
 * <p>Composes the L0 feature-SPI contracts ({@link IcebergCatalogOps}/{@link
 * IcebergViewCatalogOps}) WITHOUT extending any Iceberg SDK type ({@code Catalog}/{@code
 * ViewCatalog}/{@code SupportsNamespaces}). This is the load-bearing structural fix this rework
 * exists for: a class that both extends an Iceberg SDK type AND implements these feature-SPI
 * interfaces cannot exist, because several feature-SPI op names (deliberately matching the OSS
 * Iceberg-REST operationIds) collide with Iceberg SDK method names — same name and parameter types,
 * different return type, which Java forbids on one class regardless of inheritance. A feature-SPI
 * implementation that also needs real Iceberg-catalog behavior (table/view/namespace persistence,
 * the {@code TableOperations}/{@code ViewOperations} commit protocol) must COMPOSE a separate
 * object that implements the Iceberg SDK interfaces, not inherit from it directly. See {@code
 * LocalIcebergCatalog} (composes {@code PolarisIcebergCatalog}) for the OSS-default realization of
 * this pattern.
 *
 * <p>Deliberately does not provide delegate-based default implementations for the two interfaces'
 * {@code UnsupportedOperationException}-throwing defaults: no real provider need has surfaced yet
 * for a generic "delegate every op to an arbitrary composed Iceberg {@code Catalog}" convenience
 * layer, and speculative machinery for a hypothetical future provider is not worth the complexity
 * here (YAGNI). A provider extending this class implements exactly the operations it serves,
 * exactly as {@link IcebergCatalogOps} and {@link IcebergViewCatalogOps} already allow directly. If
 * a real need for that convenience layer materializes, add it as a follow-up on this class without
 * changing its identity as the shared reusable base.
 *
 * @param <E> the provider-private extension payload type
 */
public abstract class BasePolarisCatalog<E extends ExtensionPayload>
    implements IcebergCatalogOps<E>, IcebergViewCatalogOps<E> {}
