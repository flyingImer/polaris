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
package org.apache.polaris.core.storage;

import org.apache.polaris.core.entity.PolarisEntity;

/**
 * Framework-agnostic factory for a single cloud backend's {@link StorageCredentialVendor}.
 *
 * <p>This is the CDI-selectable unit the coordinator ({@link CredentialVendingCoordinator}) picks
 * by string key ({@code "s3"}, {@code "gcs"}, {@code "azure"}, {@code "file"} -- one factory bean
 * per {@link PolarisStorageConfigurationInfo.StorageType}, qualified with a matching
 * {@code @Identifier}). The coordinator resolves the storage-config-bearing entity from a resolved
 * path, derives the key from that entity's storage type, and selects the one factory bean
 * registered under that key.
 *
 * <p>This interface itself carries no CDI imports so it can live in polaris-core alongside the
 * other framework-agnostic storage SPIs; the concrete beans that implement it (and that carry the
 * {@code @Identifier}/{@code @ApplicationScoped} annotations) live in the runtime module that wires
 * CDI.
 *
 * <p>{@link #createVendor} returns a fresh {@link StorageCredentialVendor} bound to the given
 * entity's storage configuration -- the same "construct fresh per call, bind config at
 * construction" pattern already used by the AWS/GCP/Azure vendors themselves (each vendor is
 * immutable once constructed, wrapping a single {@link PolarisStorageConfigurationInfo}). Callers
 * must not cache the returned vendor across resolved entities.
 */
public interface StorageCredentialVendorFactory {

  /**
   * Construct a fresh {@link StorageCredentialVendor} bound to the storage configuration carried by
   * {@code resolvedStorageEntity}.
   *
   * @param resolvedStorageEntity the resolved entity that carries the storage configuration info
   *     (as found by {@link PolarisStorageConfigurationInfo#findStorageInfoFromHierarchy})
   * @return a new vendor instance bound to that entity's storage configuration
   */
  StorageCredentialVendor createVendor(PolarisEntity resolvedStorageEntity);
}
