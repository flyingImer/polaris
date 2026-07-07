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
package org.apache.polaris.storage.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The server-side view of vended storage access. It composes a {@link VendedClientStorageAccess}
 * (what a client may see) plus server-only {@code internalProperties} that must never reach a
 * client. Server-only data lives only here; {@link #clientView()} hands back the composed
 * client-visible object, which structurally cannot expose the server-only data.
 */
public final class VendedServerStorageAccess {

  private final VendedClientStorageAccess clientView;
  private final Map<String, String> internalProperties;
  private final String ioImplementation;

  public VendedServerStorageAccess(
      VendedClientStorageAccess clientView,
      Map<String, String> internalProperties,
      String ioImplementation) {
    this.clientView = clientView;
    this.internalProperties = Map.copyOf(internalProperties);
    this.ioImplementation = ioImplementation;
  }

  public VendedClientStorageAccess clientView() {
    return clientView;
  }

  public Map<String, String> internalProperties() {
    return internalProperties;
  }

  public String ioImplementation() {
    return ioImplementation;
  }

  /** The full server-side property set the storage-IO impl uses to build its FileIO. */
  public Map<String, String> serverProperties() {
    Map<String, String> merged = new LinkedHashMap<>();
    merged.putAll(clientView.extraProperties());
    merged.putAll(clientView.credentials());
    merged.putAll(internalProperties);
    return merged;
  }
}
