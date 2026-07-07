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

import java.util.Optional;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntityConstants;

/**
 * Common pre-condition checks shared across authorizer implementations for credential-related
 * operations.
 */
public final class AuthorizationPreConditions {

  private AuthorizationPreConditions() {}

  private static boolean credentialRotationRequired(
      PolarisPrincipal polarisPrincipal,
      PolarisAuthorizableOperation authzOp,
      RealmConfig realmConfig) {
    return realmConfig.getConfig(
            FeatureConfiguration.ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING)
        && authzOp != PolarisAuthorizableOperation.ROTATE_CREDENTIALS
        && polarisPrincipal
            .getProperties()
            .containsKey(PolarisEntityConstants.PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE);
  }

  private static String credentialRotationDenialMessage(
      PolarisPrincipal polarisPrincipal, PolarisAuthorizableOperation authzOp) {
    return String.format(
        "Principal '%s' is not authorized for op %s due to PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE",
        polarisPrincipal.getName(), authzOp);
  }

  /**
   * Decision-native form of {@link #checkCredentialRotationRequired}: returns the deny reason when
   * the principal must rotate credentials before performing {@code authzOp}, otherwise empty. Used
   * by decision-returning authorizers so the credential-rotation pre-check does not require
   * throw-as-control-flow.
   */
  public static Optional<String> credentialRotationDenial(
      PolarisPrincipal polarisPrincipal,
      PolarisAuthorizableOperation authzOp,
      RealmConfig realmConfig) {
    return credentialRotationRequired(polarisPrincipal, authzOp, realmConfig)
        ? Optional.of(credentialRotationDenialMessage(polarisPrincipal, authzOp))
        : Optional.empty();
  }

  /**
   * Checks whether the principal is required to rotate credentials before performing the requested
   * operation. If the principal has the {@code PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_STATE}
   * property set, only {@link PolarisAuthorizableOperation#ROTATE_CREDENTIALS} is allowed.
   *
   * @param polarisPrincipal the principal attempting the operation
   * @param authzOp the operation being attempted
   * @param realmConfig the realm config, used to read the enforcement feature flag
   * @throws ForbiddenException if the principal must rotate credentials first
   */
  public static void checkCredentialRotationRequired(
      PolarisPrincipal polarisPrincipal,
      PolarisAuthorizableOperation authzOp,
      RealmConfig realmConfig) {
    if (credentialRotationRequired(polarisPrincipal, authzOp, realmConfig)) {
      throw new ForbiddenException(
          "%s", credentialRotationDenialMessage(polarisPrincipal, authzOp));
    }
  }
}
