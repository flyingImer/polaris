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
package org.apache.polaris.spi.feature;

import org.apache.polaris.resolution.model.ResolutionRequest;
import org.apache.polaris.resolution.model.ResolutionResult;

/**
 * Entity-path resolution as a functional provider SPI: turn a request's name-paths into resolved
 * entities, the caller's activated roles, and a status, in one call.
 *
 * <p>Guarantee: each returned result is a single-call, point-in-time consistent snapshot. It never
 * mixes stale and fresh entities (an authorization or conflict decision made over a mixed view can
 * be wrong). Two {@code resolve} calls in one request are two independent snapshots; the later one
 * may be fresher, which is what a federated commit needs when it re-resolves.
 *
 * <p>Being a single-method interface (not a concrete builder class) lets a provider implement
 * resolution end to end and own the read strategy, instead of subclassing a concrete resolver and
 * delegating around it.
 */
public interface EntityResolver {

  ResolutionResult resolve(ResolutionRequest request);
}
