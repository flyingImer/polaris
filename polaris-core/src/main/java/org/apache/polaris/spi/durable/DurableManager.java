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
package org.apache.polaris.spi.durable;

import org.apache.polaris.durable.model.CatalogCreation;

/**
 * The high durable layer: business-aware operations expressed as atomicity guarantees, not as
 * sequences of primitive writes.
 *
 * <p>{@link #createCatalog} atomically creates BOTH the catalog and its admin role: on success both
 * exist, on any failure neither exists. There is no intermediate state a caller can observe or be
 * left with. The implementation chooses the mechanism (a real transaction, a single remote call, or
 * a compensating rollback over {@link DurablePrimitives}); the guarantee is the contract, the
 * mechanism is not.
 */
public interface DurableManager {

  CatalogCreation createCatalog(String catalogName, String adminRoleName);
}
