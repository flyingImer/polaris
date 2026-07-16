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
package org.apache.polaris.spi.substrate;

import org.apache.polaris.core.events.PolarisEventMetadata;

/** Factory for {@link PolarisEventMetadata}. */
public interface PolarisEventMetadataFactory {

  /**
   * Creates a new event metadata object.
   *
   * <p>This method should only be called with the request scope active in production. It may be
   * called outside the request scope in tests, in which case some fields may be missing.
   */
  PolarisEventMetadata create();

  /**
   * Creates a copy of the given metadata, with a new timestamp and new OpenTelemetry context.
   *
   * <p>Contrary to {@link #create()}, this method does not require an active request scope, and is
   * safe to use from any thread, e.g. from a task executor thread.
   */
  PolarisEventMetadata copy(PolarisEventMetadata original);
}
