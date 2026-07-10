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
package org.apache.polaris.extension.io;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.spi.substrate.StorageIoProvider;
import org.apache.polaris.storage.model.VendedServerStorageAccess;

/** A {@link StorageIoProvider} that translates WASB paths to ABFS ones */
@ApplicationScoped
@Identifier("wasb")
public class WasbTranslatingStorageIoProvider implements StorageIoProvider {

  private final StorageIoProvider defaultStorageIoProvider;

  @Inject
  public WasbTranslatingStorageIoProvider() {
    defaultStorageIoProvider = new DefaultStorageIoProvider();
  }

  @Override
  public FileIO fileIoFor(VendedServerStorageAccess access) {
    return new WasbTranslatingFileIO(defaultStorageIoProvider.fileIoFor(access));
  }
}
