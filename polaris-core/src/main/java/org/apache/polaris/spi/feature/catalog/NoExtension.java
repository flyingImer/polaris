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
 * The empty {@link ExtensionPayload}: a feature-SPI implementation with no provider-private
 * operation payload to carry at all instantiates {@code E = NoExtension}. Carries nothing -- for a
 * feature-SPI that needs a per-call ETag (like the Iceberg catalog), see {@link ETagPayload}
 * instead; {@code NoExtension} is not reshaped to carry one.
 */
public record NoExtension() implements ExtensionPayload {
  /**
   * The shared singleton -- {@code NoExtension} carries no per-call state, so one instance
   * suffices.
   */
  public static final NoExtension INSTANCE = new NoExtension();
}
