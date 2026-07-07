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
package org.apache.polaris.forkless.oss;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.storage.model.VendedClientStorageAccess;
import org.apache.polaris.storage.model.VendedServerStorageAccess;
import org.junit.jupiter.api.Test;

class DefaultStorageIoProviderTest {

  private static final String IO_IMPL = "org.apache.iceberg.inmemory.InMemoryFileIO";

  @Test
  void buildsFileIoFromServerAccess() {
    VendedClientStorageAccess client =
        new VendedClientStorageAccess(
            Map.of("client.key", "v"), Map.of("extra", "e"), Optional.empty());
    VendedServerStorageAccess server =
        new VendedServerStorageAccess(client, Map.of("server.secret", "s"), IO_IMPL);

    FileIO io = new DefaultStorageIoProvider().fileIoFor(server);

    assertThat(io).isInstanceOf(InMemoryFileIO.class);
  }

  @Test
  void clientViewNeverCarriesServerOnlyData() {
    VendedClientStorageAccess client =
        new VendedClientStorageAccess(
            Map.of("aws.access.key", "AKIA"), Map.of("region", "us-west-2"), Optional.empty());
    VendedServerStorageAccess server =
        new VendedServerStorageAccess(client, Map.of("aws.secret.key", "TOPSECRET"), IO_IMPL);

    VendedClientStorageAccess view = server.clientView();

    // The server-only secret is present server-side...
    assertThat(server.internalProperties()).containsEntry("aws.secret.key", "TOPSECRET");
    assertThat(server.serverProperties()).containsEntry("aws.secret.key", "TOPSECRET");
    // ...and absent from every client-visible surface. The client type has no accessor for
    // internalProperties at all, so this is a structural guarantee, not a runtime filter.
    assertThat(view.credentials().values()).doesNotContain("TOPSECRET");
    assertThat(view.extraProperties().values()).doesNotContain("TOPSECRET");
    assertThat(view.credentials()).doesNotContainKey("aws.secret.key");
    assertThat(view.extraProperties()).doesNotContainKey("aws.secret.key");
  }
}
