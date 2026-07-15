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

import java.util.Objects;
import java.util.Optional;

/**
 * The result of a catalog feature-SPI operation: the plain Iceberg response {@code body}, the
 * OSS-carried operation freshness/ETag, and a typed provider-private {@code extension}.
 *
 * <p>Per ADR-0003 the three slots are distinct:
 *
 * <ul>
 *   <li>{@code body} -- the unchanged Iceberg response type ({@code LoadTableResponse}, {@code
 *       ListTablesResponse}, ...), or {@code null}/{@code Void} for operations with no body.
 *   <li>{@code etag} -- OSS-carried operation metadata. The implementation supplies it (a local
 *       impl derives it from the metadata location / entity version; a federated impl supplies the
 *       remote catalog's ETag), and the runtime adapter maps it to the HTTP {@code ETag} header.
 *       The adapter reads ONLY this slot for freshness, never {@code extension}.
 *   <li>{@code extension} -- a provider-private, OSS-uninterpreted {@link ExtensionPayload}. The
 *       OSS default uses {@link NoExtension}. The runtime adapter must never inspect it.
 * </ul>
 *
 * <p>There is no request-side generic wrapper; operation inputs stay plain parameters. This wraps
 * only the output.
 *
 * @param <O> the plain Iceberg response type
 * @param <E> the provider-private extension payload type
 */
public record PolarisResult<O, E extends ExtensionPayload>(
    O body, Optional<String> etag, E extension) {

  public PolarisResult {
    Objects.requireNonNull(etag, "etag");
    Objects.requireNonNull(extension, "extension");
  }

  /** An OSS-default result ({@link NoExtension}) with no ETag. */
  public static <O> PolarisResult<O, NoExtension> of(O body) {
    return new PolarisResult<>(body, Optional.empty(), NoExtension.INSTANCE);
  }

  /** An OSS-default result ({@link NoExtension}) carrying an ETag. */
  public static <O> PolarisResult<O, NoExtension> of(O body, Optional<String> etag) {
    return new PolarisResult<>(body, etag, NoExtension.INSTANCE);
  }
}
