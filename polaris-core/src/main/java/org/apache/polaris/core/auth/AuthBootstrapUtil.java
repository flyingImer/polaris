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

package org.apache.polaris.core.auth;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Preconditions;
import java.util.Optional;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.dao.entity.GenerateEntityIdResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.spi.durable.CatalogDurableManager;
import org.apache.polaris.spi.durable.DurableManager;
import org.apache.polaris.spi.durable.GrantDurableManager;
import org.apache.polaris.spi.durable.PrincipalDurableManager;
import org.apache.polaris.spi.durable.SecretsDurableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for sharing root Polaris Principal setup code among all persistence
 * implementations.
 *
 * <p>Note: this class is not meant to be reused outside of Polaris code.
 */
public class AuthBootstrapUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(AuthBootstrapUtil.class);

  private AuthBootstrapUtil() {}

  public static PrincipalSecretsResult createPolarisPrincipalForRealm(
      DurableManager metaStoreManager, PolarisCallContext ctx) {
    // The previous model's single manager implements every per-domain contract; narrow it once
    // here.
    return createPolarisPrincipalForRealm(
        (CatalogDurableManager) metaStoreManager,
        (PrincipalDurableManager) metaStoreManager,
        (GrantDurableManager) metaStoreManager,
        (SecretsDurableManager) metaStoreManager,
        ctx);
  }

  public static PrincipalSecretsResult createPolarisPrincipalForRealm(
      CatalogDurableManager catalogManager,
      PrincipalDurableManager principalManager,
      GrantDurableManager grantManager,
      SecretsDurableManager secretsManager,
      PolarisCallContext ctx) {

    Optional<PrincipalEntity> preliminaryRootPrincipal = principalManager.findRootPrincipal(ctx);
    if (preliminaryRootPrincipal.isPresent()) {
      String overrideMessage =
          "It appears this metastore manager has already been bootstrapped. "
              + "To continue bootstrapping, please first purge the metastore with the `purge` command.";
      LOGGER.error("\n\n {} \n\n", overrideMessage);
      throw new IllegalArgumentException(overrideMessage);
    }

    // Create a root container entity that can represent the securable for any top-level grants.
    PolarisBaseEntity rootContainer =
        new PolarisBaseEntity(
            PolarisEntityConstants.getNullId(),
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityType.ROOT,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootEntityId(),
            PolarisEntityConstants.getRootContainerName());
    catalogManager.createEntityIfNotExists(ctx, null, rootContainer);

    CreatePrincipalResult principalResult =
        principalManager.createPrincipal(
            ctx,
            new PrincipalEntity.Builder()
                .setId(generateId(catalogManager, ctx))
                .setName(PolarisEntityConstants.getRootPrincipalName())
                .setCreateTimestamp(System.currentTimeMillis())
                .build());
    checkState(principalResult.isSuccess(), "Unable to create root principal");
    PrincipalEntity rootPrincipal = principalResult.getPrincipal();

    // now create the account admin principal role
    PrincipalRoleEntity serviceAdminPrincipalRole =
        new PrincipalRoleEntity.Builder()
            .setId(generateId(catalogManager, ctx))
            .setName(PolarisEntityConstants.getNameOfPrincipalServiceAdminRole())
            .setCreateTimestamp(System.currentTimeMillis())
            .build();
    catalogManager.createEntityIfNotExists(ctx, null, serviceAdminPrincipalRole);

    // we also need to grant usage on the account-admin principal to the principal
    grantManager.grantPrivilegeOnSecurableToRole(
        ctx, rootPrincipal, null, serviceAdminPrincipalRole, PolarisPrivilege.PRINCIPAL_ROLE_USAGE);

    // grant SERVICE_MANAGE_ACCESS on the rootContainer to the serviceAdminPrincipalRole
    grantManager.grantPrivilegeOnSecurableToRole(
        ctx,
        serviceAdminPrincipalRole,
        null,
        rootContainer,
        PolarisPrivilege.SERVICE_MANAGE_ACCESS);

    return secretsManager.loadPrincipalSecrets(ctx, rootPrincipal.getClientId());
  }

  private static long generateId(CatalogDurableManager catalogManager, PolarisCallContext ctx) {
    GenerateEntityIdResult res = catalogManager.generateNewEntityId(ctx);
    Preconditions.checkState(res.isSuccess(), "Unable to generate id for polaris entity");
    return res.getId();
  }
}
