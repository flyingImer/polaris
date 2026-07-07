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
package org.apache.polaris.forkless.wiring;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Produces;
import org.apache.polaris.forkless.oss.DefaultStorageIoProvider;
import org.apache.polaris.forkless.provider.SnowflakeStorageIoProvider;
import org.apache.polaris.spi.substrate.StorageIoProvider;

/**
 * CDI wiring for the storage-IO seam. The framework annotations that select which impl the runtime
 * uses live here, in the runtime/app layer, never on the impls. The OSS default is the plain
 * producer; the provider impl is an {@code @Alternative} with a higher {@code @Priority}, which is
 * exactly how a provider overrides an OSS default without forking (the pattern managed already uses
 * across its seams).
 */
@ApplicationScoped
public class ForklessStorageIoProducers {

  @Produces
  public StorageIoProvider defaultStorageIoProvider() {
    return new DefaultStorageIoProvider();
  }

  @Produces
  @Alternative
  @Priority(1000)
  public StorageIoProvider snowflakeStorageIoProvider() {
    return new SnowflakeStorageIoProvider();
  }
}
