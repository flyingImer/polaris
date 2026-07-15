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
 * <p>The OSS default implementation uses {@link NoExtension} (nothing to carry). A provider
 * supplies its own {@code ExtensionPayload} subtype. Per ADR-0003, the runtime adapter is a pure
 * pass-through and must NEVER inspect this payload; it is interpreted only by the provider's own
 * runtime. The OSS-carried ETag is a distinct, first-class slot on {@link PolarisResult}, not part
 * of this type.
 */
public interface ExtensionPayload {}
