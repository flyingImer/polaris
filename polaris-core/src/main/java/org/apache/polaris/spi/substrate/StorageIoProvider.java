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

import org.apache.iceberg.io.FileIO;
import org.apache.polaris.storage.model.VendedServerStorageAccess;

/**
 * Server-side storage-IO seam: produce an Iceberg {@link FileIO} for Polaris's own metadata
 * read/write, given already-vended server-side storage access.
 *
 * <p>One typed input, no FileIO-class-name String and no raw property map on the contract. The
 * implementation composes the Iceberg {@code FileIO} and any decorators internally.
 */
public interface StorageIoProvider {

  FileIO fileIoFor(VendedServerStorageAccess access);
}
