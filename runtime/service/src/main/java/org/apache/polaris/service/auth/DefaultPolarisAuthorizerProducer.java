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
package org.apache.polaris.service.auth;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.persistence.resolver.EntityResolver;

/**
 * Produces the default in-process Polaris authorizer under the {@code "internal"} identifier.
 *
 * <p>The default authorizer composes the request-scoped {@link RealmConfig} and {@link
 * EntityResolver} directly, so it is produced per request. It has no heavy initialization and no
 * boot-time readiness requirement (the previous {@code DefaultPolarisAuthorizerFactory} had a
 * trivial constructor with no validation).
 */
@ApplicationScoped
public class DefaultPolarisAuthorizerProducer {

  @Produces
  @RequestScoped
  @Identifier("internal")
  public PolarisAuthorizer internalAuthorizer(
      RealmConfig realmConfig, EntityResolver entityResolver) {
    return new PolarisAuthorizerImpl(realmConfig, entityResolver);
  }
}
