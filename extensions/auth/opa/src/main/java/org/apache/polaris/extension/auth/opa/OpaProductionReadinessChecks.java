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
package org.apache.polaris.extension.auth.opa;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import java.util.ArrayList;
import java.util.List;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.apache.polaris.core.config.ProductionReadinessCheck.Error;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class OpaProductionReadinessChecks {

  /**
   * Production readiness check for OPA authorization.
   *
   * <p>This producer runs at startup (the aggregate readiness observer iterates every {@code
   * ProductionReadinessCheck} bean on {@code Startup}). When OPA is the active authorization type
   * it eagerly resolves the {@code @Identifier("opa")} authorizer, which forces {@link
   * OpaPolarisAuthorizerProducer}'s {@code @PostConstruct} to run (config validation, HTTP client
   * creation, auth setup) at boot. This preserves the fail-fast-at-boot behavior that the former
   * {@code OpaPolarisAuthorizerFactory} provided when it was injected as the selected factory. When
   * OPA is not the active type, nothing is resolved (the OPA authorizer is never constructed),
   * matching the previous behavior where only the selected authorizer was realized.
   */
  @Produces
  public ProductionReadinessCheck checkOpaAuthorization(
      @ConfigProperty(name = "polaris.authorization.type", defaultValue = "internal")
          String authorizationType,
      OpaAuthorizationConfig config,
      @Identifier("opa") Instance<PolarisAuthorizer> opaAuthorizer) {
    if (!"opa".equals(authorizationType)) {
      return ProductionReadinessCheck.OK;
    }

    // Force eager construction and validation of the active OPA authorizer at boot (fail-fast).
    opaAuthorizer.get();

    List<Error> errors = new ArrayList<>();

    errors.add(
        Error.of(
            "OPA authorization is currently a Beta feature and is not a stable release. Breaking changes may be introduced in future versions. Use with caution in production environments.",
            "polaris.authorization.type"));

    if (!config.http().verifySsl()) {
      errors.add(
          Error.ofSevere(
              "SSL certificate verification is disabled for OPA communication. This exposes the service to man-in-the-middle attacks and other severe security risks.",
              "polaris.authorization.opa.http.verify-ssl"));
    }

    return ProductionReadinessCheck.of(errors);
  }
}
