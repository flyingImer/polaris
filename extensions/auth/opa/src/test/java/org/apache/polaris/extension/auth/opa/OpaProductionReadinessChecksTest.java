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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.apache.polaris.spi.substrate.PolarisAuthorizer;
import org.junit.jupiter.api.Test;

/**
 * Regression guard for the eager-at-boot readiness of the dissolved authorizer factory (the former
 * {@code OpaPolarisAuthorizerFactory}, dissolved into per-{@code @Identifier} CDI beans).
 *
 * <p>When OPA is the active authorization type, {@code OpaProductionReadinessChecks} MUST resolve
 * the selected {@code @Identifier("opa")} authorizer at startup, which forces {@code
 * OpaPolarisAuthorizerProducer}'s {@code @PostConstruct} validation (policy URI, HTTP client, auth)
 * to run at boot. This is the fail-fast-at-boot the factory used to give by being injected as the
 * selected bean. If someone reverts the check to lazy (drops the eager resolve), an OPA
 * misconfiguration would only surface on the first request instead of at boot, the exact regression
 * that got the first dissolution attempt reverted. These tests go red on that revert.
 */
public class OpaProductionReadinessChecksTest {

  @Test
  public void opaActive_eagerlyResolvesSelectedAuthorizer_propagatingInitFailureAtBoot() {
    OpaProductionReadinessChecks checks = new OpaProductionReadinessChecks();
    @SuppressWarnings("unchecked")
    Instance<PolarisAuthorizer> opaAuthorizer = mock(Instance.class);
    OpaAuthorizationConfig config = mock(OpaAuthorizationConfig.class);
    // A misconfigured OPA authorizer fails when its @PostConstruct/producer runs on first resolve.
    IllegalStateException bootFailure =
        new IllegalStateException("OPA policy URI must be configured");
    when(opaAuthorizer.get()).thenThrow(bootFailure);

    // type == opa: the check must resolve the authorizer eagerly, so the failure surfaces at boot.
    assertThatThrownBy(() -> checks.checkOpaAuthorization("opa", config, opaAuthorizer))
        .isSameAs(bootFailure);
    verify(opaAuthorizer).get();
  }

  @Test
  public void opaNotActive_neverResolvesOpaAuthorizer() {
    OpaProductionReadinessChecks checks = new OpaProductionReadinessChecks();
    @SuppressWarnings("unchecked")
    Instance<PolarisAuthorizer> opaAuthorizer = mock(Instance.class);
    OpaAuthorizationConfig config = mock(OpaAuthorizationConfig.class);

    // type != opa: only the selected authorizer is realized, so OPA is never constructed.
    ProductionReadinessCheck result =
        checks.checkOpaAuthorization("internal", config, opaAuthorizer);

    assertThat(result).isSameAs(ProductionReadinessCheck.OK);
    verify(opaAuthorizer, never()).get();
  }

  @Test
  public void opaActive_verifySslDisabled_addsSevereWarning() {
    OpaProductionReadinessChecks checks = new OpaProductionReadinessChecks();
    @SuppressWarnings("unchecked")
    Instance<PolarisAuthorizer> opaAuthorizer = mock(Instance.class);
    when(opaAuthorizer.get()).thenReturn(mock(PolarisAuthorizer.class));
    OpaAuthorizationConfig config = mock(OpaAuthorizationConfig.class);
    OpaAuthorizationConfig.HttpConfig http = mock(OpaAuthorizationConfig.HttpConfig.class);
    when(config.http()).thenReturn(http);
    when(http.verifySsl()).thenReturn(false);

    ProductionReadinessCheck result = checks.checkOpaAuthorization("opa", config, opaAuthorizer);

    // The authorizer is still resolved eagerly (fail-fast), and verify-ssl=false is a severe error
    // -> the aggregate readiness observer aborts startup on it.
    verify(opaAuthorizer).get();
    assertThat(result.getErrors())
        .anySatisfy(
            e -> {
              assertThat(e.severe()).isTrue();
              assertThat(e.offendingProperty())
                  .isEqualTo("polaris.authorization.opa.http.verify-ssl");
            });
  }

  @Test
  public void opaActive_verifySslEnabled_onlyNonSevereBetaWarning() {
    OpaProductionReadinessChecks checks = new OpaProductionReadinessChecks();
    @SuppressWarnings("unchecked")
    Instance<PolarisAuthorizer> opaAuthorizer = mock(Instance.class);
    when(opaAuthorizer.get()).thenReturn(mock(PolarisAuthorizer.class));
    OpaAuthorizationConfig config = mock(OpaAuthorizationConfig.class);
    OpaAuthorizationConfig.HttpConfig http = mock(OpaAuthorizationConfig.HttpConfig.class);
    when(config.http()).thenReturn(http);
    when(http.verifySsl()).thenReturn(true);

    ProductionReadinessCheck result = checks.checkOpaAuthorization("opa", config, opaAuthorizer);

    // Only the non-severe Beta notice remains, so startup proceeds.
    assertThat(result.getErrors()).hasSize(1);
    assertThat(result.getErrors().get(0).severe()).isFalse();
    assertThat(result.getErrors().get(0).offendingProperty())
        .isEqualTo("polaris.authorization.type");
  }
}
