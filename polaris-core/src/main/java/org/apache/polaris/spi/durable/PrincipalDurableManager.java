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

import java.util.Optional;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.jspecify.annotations.NonNull;

/**
 * Durable manager for principals: creating a principal together with its secrets, and the
 * principal-typed lookups callers use to find the root principal, a principal, or a principal role.
 * Owns those business rules and knows no storage topology. Authorization and request validation
 * live above this layer.
 *
 * <p>Creating a principal submits mutations for two record kinds, the principal entity and its
 * secrets record, in one list. A commit cannot span atomicity domains, so that list is atomic only
 * when both kinds resolve to the same domain; where they resolve to different ones it becomes more
 * than one commit, and nothing here promises what a crash in between leaves behind. Cross-domain
 * atomicity is not part of this contract.
 */
public interface PrincipalDurableManager {

  /**
   * Create a new principal. This not only creates the new principal entity but also generates a
   * client_id/secret pair for this new principal.
   *
   * @param callCtx call context
   * @param principal the principal entity to create
   * @return the client_id/secret for the new principal which was created. Will return
   *     ENTITY_ALREADY_EXISTS if the principal already exists
   */
  @NonNull CreatePrincipalResult createPrincipal(
      @NonNull PolarisCallContext callCtx, @NonNull PrincipalEntity principal);

  Optional<PrincipalEntity> findRootPrincipal(PolarisCallContext polarisCallContext);

  Optional<PrincipalEntity> findPrincipalById(
      PolarisCallContext polarisCallContext, long principalId);

  Optional<PrincipalEntity> findPrincipalByName(
      PolarisCallContext polarisCallContext, String principalName);

  Optional<PrincipalRoleEntity> findPrincipalRoleByName(
      PolarisCallContext polarisCallContext, String principalRoleName);
}
