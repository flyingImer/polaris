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
package org.apache.polaris.operation.model;

/**
 * The outcome of a conditional-load feature-SPI operation (e.g. Iceberg REST's If-None-Match {@code
 * loadTable}): either the body was loaded ({@link Loaded}) or the caller's cached copy is still
 * current ({@link NotModified}). Both cases carry {@link OperationMetadata} -- including the
 * not-modified case, since a 304 response may still carry an ETag. The not-modified DECISION is
 * made by the feature-SPI implementation (local impl compares If-None-Match itself; a federated
 * impl delegates to the remote catalog), never by the runtime adapter (ADR-0003 amendment
 * refinement 2). The runtime adapter is a pure pass-through: {@code Loaded} maps to 200 + body +
 * ETag header, {@code NotModified} maps to 304 + ETag header.
 *
 * @param <R> the Iceberg response type the {@code Loaded} case returns
 */
public sealed interface ConditionalLoadOutcome<R> {

  /** The body was loaded; {@code result} carries the Iceberg response plus its metadata. */
  record Loaded<R>(OperationResult<R> result) implements ConditionalLoadOutcome<R> {}

  /**
   * The caller's cached copy is still current; there is no body, but {@code metadata} (in
   * particular its etag) still transits so the impl's not-modified decision is fully expressed.
   */
  record NotModified<R>(OperationMetadata metadata) implements ConditionalLoadOutcome<R> {}
}
