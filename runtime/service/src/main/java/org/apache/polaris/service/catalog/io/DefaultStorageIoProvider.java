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
package org.apache.polaris.service.catalog.io;

import com.google.common.annotations.VisibleForTesting;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.spi.substrate.StorageIoProvider;
import org.apache.polaris.storage.model.VendedServerStorageAccess;
import org.jspecify.annotations.NonNull;

/**
 * A default storage-IO provider implementation for creating Iceberg {@link FileIO} instances from
 * already-vended server-side storage access.
 *
 * <p>This class acts as a translation layer between Polaris's vended access and the properties
 * required by Iceberg's {@link FileIO}.
 *
 * <p>Available via CDI as a {@link RequestScoped @RequestScoped} bean.
 */
@RequestScoped
@Identifier("default")
public class DefaultStorageIoProvider implements StorageIoProvider {

  @Inject
  public DefaultStorageIoProvider() {}

  @Override
  public FileIO fileIoFor(@NonNull VendedServerStorageAccess access) {
    return fileIoForInternal(access.ioImplementation(), access.serverProperties());
  }

  @VisibleForTesting
  FileIO fileIoForInternal(
      @NonNull String ioImplClassName, @NonNull Map<String, String> properties) {
    return new ExceptionMappingFileIO(CatalogUtil.loadFileIO(ioImplClassName, properties, null));
  }
}
