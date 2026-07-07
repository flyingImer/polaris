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
package org.apache.polaris.forkless.provider;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.spi.substrate.StorageIoProvider;
import org.apache.polaris.storage.model.VendedClientStorageAccess;
import org.apache.polaris.storage.model.VendedServerStorageAccess;
import org.junit.jupiter.api.Test;

class SnowflakeStorageIoProviderTest {

  private static final String IO_IMPL = "org.apache.iceberg.inmemory.InMemoryFileIO";

  @Test
  void implementsCoreSpiWithoutFork() {
    StorageIoProvider provider = new SnowflakeStorageIoProvider();
    VendedClientStorageAccess client =
        new VendedClientStorageAccess(Map.of("k", "v"), Map.of(), Optional.empty());
    VendedServerStorageAccess server =
        new VendedServerStorageAccess(client, Map.of("internal", "x"), IO_IMPL);

    FileIO io = provider.fileIoFor(server);

    assertThat(io).isInstanceOf(InMemoryFileIO.class);
  }
}
