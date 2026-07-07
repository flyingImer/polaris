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
package org.apache.polaris.durable.model;

import java.util.Map;

/**
 * One durably-stored entity as the durable layer sees it: an identity, an optimistic-concurrency
 * version, a coarse type tag, a name, and an opaque payload. Business meaning of the payload lives
 * above this layer, not here.
 */
public record EntityRecord(
    long id, int version, String type, String name, Map<String, String> payload) {

  public EntityRecord {
    payload = Map.copyOf(payload);
  }
}
