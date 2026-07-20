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
 * Marker for a provider-private, OSS-uninterpreted payload a catalog feature-SPI implementation may
 * attach to an operation result (the {@code E} in {@link PolarisResult}). It carries operation
 * metadata that exceeds what the plain Iceberg response type can express and that OSS does not
 * interpret -- for example Snowflake Horizon freshness/status semantics beyond the OSS-carried
 * ETag.
 *
 * <p>A pure, behaviorless marker by design -- it declares no methods. A payload type that also
 * needs to carry an ETag composes {@link ETagCarrier} separately (see {@link ETagPayload});
 * ETag-carrying is a general, reusable capability, not a specialization of "being an extension
 * payload," so it is deliberately not nested under this interface.
 *
 * <p>The OSS default implementation uses {@link NoExtension} when there is nothing to carry, or
 * {@link ETagPayload} for feature-SPIs (like the Iceberg catalog) that need a per-call ETag. A
 * provider supplies its own {@code ExtensionPayload} subtype, composing {@link ETagCarrier} too if
 * it needs to carry an ETag.
 */
public interface ExtensionPayload {}
