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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.apache.ranger.authz.embedded.RangerEmbeddedAuthorizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Produces the Apache Ranger-based Polaris authorizer under the {@code "ranger"} identifier.
 *
 * <p>The heavy, once-per-application setup (config validation and embedded Ranger authorizer
 * initialization) runs eagerly in this {@link ApplicationScoped} bean's constructor and fails fast
 * if Ranger cannot be initialized, exactly as the former {@code RangerPolarisAuthorizerFactory}
 * constructor did. The produced authorizer carries per-request realm state ({@link RealmConfig} and
 * {@link RealmContext}), so it is produced {@link RequestScoped} from the pre-initialized embedded
 * authorizer.
 *
 * <p>Boot-time fail-fast is preserved by {@code RangerProductionReadinessChecks}, which eagerly
 * resolves this producer bean at startup when Ranger is the active authorization type, forcing the
 * constructor initialization at boot.
 */
@ApplicationScoped
public class RangerPolarisAuthorizerProducer {
  private static final Logger LOG = LoggerFactory.getLogger(RangerPolarisAuthorizerProducer.class);

  private static final String ERR_AUTHORIZER_FACTORY_NOT_INITIALIZED =
      "Ranger authorizer was not initialized successfully";

  private final RangerPolarisAuthorizerConfig config;
  private RangerEmbeddedAuthorizer authorizer;
  private String serviceName;
  @Inject private RealmContext realmContext;

  @Inject
  RangerPolarisAuthorizerProducer(RangerPolarisAuthorizerConfig config) {
    this.config = config;
    config.validate();
    LOG.info("Initializing RangerAuthorizer");
    try {
      Properties properties = config.toRangerProperties();
      RangerEmbeddedAuthorizer authorizer = new RangerEmbeddedAuthorizer(properties);
      authorizer.init();
      this.authorizer = authorizer;
      this.serviceName = config.serviceName().get();
    } catch (RangerAuthzException t) {
      throw new RuntimeException("Failed to initialize RangerPolarisAuthorizer", t);
    }
    LOG.info("RangerAuthorizer initialized successfully");
    LOG.debug("RangerPolarisAuthorizerProducer has been activated.");
  }

  @Produces
  @RequestScoped
  @Identifier("ranger")
  public RangerPolarisAuthorizer rangerAuthorizer(RealmConfig realmConfig) {
    // Ranger evaluates against its own policy store, so no EntityResolver is composed here.
    LOG.debug("Creating RangerPolarisAuthorizer");

    if (authorizer == null || StringUtils.isBlank(serviceName)) {
      throw new IllegalStateException(ERR_AUTHORIZER_FACTORY_NOT_INITIALIZED);
    }

    RangerPolarisAuthorizer polarisAuthorizer =
        new RangerPolarisAuthorizer(authorizer, serviceName, realmConfig);

    if (realmContext != null) {
      polarisAuthorizer.setRealmContext(realmContext);
    }

    return polarisAuthorizer;
  }
}
