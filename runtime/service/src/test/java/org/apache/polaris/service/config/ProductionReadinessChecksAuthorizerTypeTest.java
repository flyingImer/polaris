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
package org.apache.polaris.service.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.apache.polaris.spi.substrate.PolarisAuthorizer;
import org.junit.jupiter.api.Test;

/**
 * Guards the boot-time fail-fast for a misconfigured {@code polaris.authorization.type} (the
 * invalid-type surface that the dissolved {@code PolarisAuthorizerFactory} used to abort startup
 * on). {@code checkAuthorizerTypeResolvable} MUST emit a severe error when no authorizer matches
 * the configured type, so an unknown type / missing authorizer module aborts startup rather than
 * failing every request. A revert to booting-clean-then-500-per-request goes red here.
 */
public class ProductionReadinessChecksAuthorizerTypeTest {

  @SuppressWarnings("unchecked")
  private static Instance<PolarisAuthorizer> selecting(boolean unsatisfied) {
    Instance<PolarisAuthorizer> authorizers = mock(Instance.class);
    Instance<PolarisAuthorizer> selected = mock(Instance.class);
    when(authorizers.select(any())).thenReturn(selected);
    when(selected.isUnsatisfied()).thenReturn(unsatisfied);
    return authorizers;
  }

  @Test
  public void unresolvableAuthorizerType_isSevere() {
    ProductionReadinessChecks checks = new ProductionReadinessChecks();
    AuthorizationConfiguration config = mock(AuthorizationConfiguration.class);
    when(config.type()).thenReturn("nonexistent");

    ProductionReadinessCheck result = checks.checkAuthorizerTypeResolvable(config, selecting(true));

    assertThat(result.getErrors()).hasSize(1);
    assertThat(result.getErrors().get(0).severe()).isTrue();
    assertThat(result.getErrors().get(0).offendingProperty())
        .isEqualTo("polaris.authorization.type");
  }

  @Test
  public void resolvableAuthorizerType_isOk() {
    ProductionReadinessChecks checks = new ProductionReadinessChecks();
    AuthorizationConfiguration config = mock(AuthorizationConfiguration.class);
    when(config.type()).thenReturn("internal");

    ProductionReadinessCheck result =
        checks.checkAuthorizerTypeResolvable(config, selecting(false));

    assertThat(result).isSameAs(ProductionReadinessCheck.OK);
  }
}
