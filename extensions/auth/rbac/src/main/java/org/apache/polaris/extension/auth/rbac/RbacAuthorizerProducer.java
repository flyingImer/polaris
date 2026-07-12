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
package org.apache.polaris.extension.auth.rbac;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.spi.substrate.EntityResolver;
import org.apache.polaris.spi.substrate.PolarisAuthorizer;

/**
 * Produces the built-in RBAC Polaris authorizer under the {@code "internal"} identifier.
 *
 * <p>The {@code "internal"} identifier is the config selector value that {@code
 * polaris.authorization.type} matches to select the built-in role-based authorizer; it is kept
 * unchanged so existing deployments keep resolving this bean. Only the class moved to the {@code
 * extensions/auth/rbac} peer module (alongside the OPA and Ranger authorizers) so the default RBAC
 * authorizer is a swappable peer rather than a privileged core type.
 *
 * <p>The RBAC authorizer composes the request-scoped {@link RealmConfig} and {@link EntityResolver}
 * directly, so it is produced per request. It has no heavy initialization and no boot-time
 * readiness requirement (the previous {@code DefaultPolarisAuthorizerFactory} had a trivial
 * constructor with no validation).
 */
@ApplicationScoped
public class RbacAuthorizerProducer {

  @Produces
  @RequestScoped
  @Identifier("internal")
  public PolarisAuthorizer internalAuthorizer(
      RealmConfig realmConfig, EntityResolver entityResolver) {
    return new RbacAuthorizer(realmConfig, entityResolver);
  }
}
