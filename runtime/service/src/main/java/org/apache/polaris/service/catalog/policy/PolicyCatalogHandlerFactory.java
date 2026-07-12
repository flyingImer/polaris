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
package org.apache.polaris.service.catalog.policy;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.spi.substrate.EntityResolver;
import org.apache.polaris.spi.durable.DurableManager;
import org.apache.polaris.spi.durable.PolarisPolicyMappingManager;
import org.apache.polaris.spi.substrate.PolarisAuthorizer;

@RequestScoped
public class PolicyCatalogHandlerFactory {

  @Inject CallContext callContext;
  @Inject EntityResolver entityResolver;
  @Inject DurableManager metaStoreManager;
  @Inject PolarisPolicyMappingManager policyMappingManager;
  @Inject PolarisAuthorizer authorizer;

  public PolicyCatalogHandler createHandler(String catalogName, PolarisPrincipal principal) {
    return ImmutablePolicyCatalogHandler.builder()
        .catalogName(catalogName)
        .polarisPrincipal(principal)
        .callContext(callContext)
        .entityResolver(entityResolver)
        .metaStoreManager(metaStoreManager)
        .policyMappingManager(policyMappingManager)
        .authorizer(authorizer)
        .build();
  }
}
