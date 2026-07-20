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

import java.util.Optional;

/**
 * A type that can carry the OSS-carried operation ETag, or empty if the operation it came from does
 * not produce one.
 *
 * <p>Deliberately not nested under {@link ExtensionPayload}: ETag-carrying is a general, reusable
 * capability, not a specialization of "being a provider-private catalog extension payload" -- an
 * {@code ExtensionPayload} that also needs to carry an ETag composes both interfaces explicitly
 * (see {@link ETagPayload}), rather than inheriting etag-ness through a nested marker relationship.
 * This keeps ETagCarrier reusable by any future type that needs to carry an ETag, whether or not it
 * is also an {@code ExtensionPayload}.
 *
 * <p>The runtime adapter reads this generically via {@code instanceof ETagCarrier}; it must never
 * inspect anything else about a payload beyond this one contract.
 */
public interface ETagCarrier {

  /** The OSS-carried operation ETag, or empty if the operation does not produce one. */
  Optional<String> etag();
}
