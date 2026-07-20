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

/**
 * The result of a catalog feature-SPI operation: the plain Iceberg response {@code body} and a
 * typed provider-private {@code extension}.
 *
 * <ul>
 *   <li>{@code body} -- the unchanged Iceberg response type ({@code LoadTableResponse}, {@code
 *       ListTablesResponse}, ...), or {@code null}/{@code Void} for operations with no body.
 *   <li>{@code extension} -- a provider-private, OSS-uninterpreted {@link ExtensionPayload}. A
 *       feature-SPI with nothing to carry uses {@link NoExtension}; the Iceberg catalog feature-SPI
 *       specifically binds {@code E = }{@link ETagPayload}, since per ADR-0003 {@code E} is the
 *       universal channel for everything crossing the runtime/core boundary, including the
 *       OSS-carried operation ETag: the implementation supplies it (a local impl derives it from
 *       the metadata location / entity version; a federated impl passes through the remote
 *       catalog's ETag), and the runtime adapter reads it generically via {@code instanceof
 *       ETagCarrier} on {@code result.extension()} to map it to the HTTP {@code ETag} header. The
 *       adapter must never otherwise inspect {@code extension}.
 * </ul>
 *
 * <p>There is no request-side generic wrapper; operation inputs stay plain parameters. This wraps
 * only the output.
 *
 * @param <O> the plain Iceberg response type
 * @param <E> the provider-private extension payload type
 */
public record PolarisResult<O, E extends ExtensionPayload>(O body, E extension) {

  public PolarisResult {
    Objects.requireNonNull(extension, "extension");
  }

  /** A result with no extension payload at all ({@link NoExtension}). */
  public static <O> PolarisResult<O, NoExtension> of(O body) {
    return new PolarisResult<>(body, NoExtension.INSTANCE);
  }
}
