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

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.spi.substrate.StorageIoProvider;
import org.apache.polaris.storage.model.VendedServerStorageAccess;

/**
 * Provider-analog storage-IO impl (the "Snowflake-private" side). It implements the OSS core SPI
 * from a module that depends only on polaris-core and Iceberg, with no OSS source fork and no
 * framework dependency. The provider customizes behind the seam (here: a PrivateLink-style endpoint
 * rewrite) without changing the contract or any OSS code.
 */
public class SnowflakeStorageIoProvider implements StorageIoProvider {

  @Override
  public FileIO fileIoFor(VendedServerStorageAccess access) {
    Map<String, String> props = new LinkedHashMap<>(access.serverProperties());
    props.replaceAll((key, value) -> rewriteForPrivateLink(key, value));
    return CatalogUtil.loadFileIO(access.ioImplementation(), props, null);
  }

  private static String rewriteForPrivateLink(String key, String value) {
    if (value != null && value.contains(".amazonaws.com")) {
      return value.replace(".amazonaws.com", ".privatelink.amazonaws.com");
    }
    return value;
  }
}
