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
package org.apache.polaris.extension.auth.ranger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class RangerProductionReadinessChecks {

  /**
   * Production readiness check for Ranger authorization.
   *
   * <p>This producer runs at startup (the aggregate readiness observer iterates every {@code
   * ProductionReadinessCheck} bean on {@code Startup}). When Ranger is the active authorization
   * type it eagerly resolves {@link RangerPolarisAuthorizerProducer}, forcing its constructor to
   * run (config validation and embedded Ranger authorizer initialization) at boot. This preserves
   * the fail-fast-at-boot behavior of the former {@code RangerPolarisAuthorizerFactory}, whose
   * constructor performed the same initialization when it was realized as the selected factory.
   *
   * <p>The {@code @Identifier("ranger")} authorizer itself is request-scoped (it composes
   * per-request realm state), so it cannot be resolved at startup; resolving the application-scoped
   * producer bean is what carries the boot-time initialization and fail-fast.
   */
  @Produces
  public ProductionReadinessCheck checkRangerAuthorization(
      @ConfigProperty(name = "polaris.authorization.type", defaultValue = "internal")
          String authorizationType,
      Instance<RangerPolarisAuthorizerProducer> rangerAuthorizerProducer) {
    if ("ranger".equals(authorizationType)) {
      // Force eager construction and initialization of the Ranger authorizer at boot (fail-fast).
      rangerAuthorizerProducer.get();
    }
    return ProductionReadinessCheck.OK;
  }
}
