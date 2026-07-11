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

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import org.apache.polaris.core.config.ProductionReadinessCheck;
import org.junit.jupiter.api.Test;

/**
 * Regression guard for the eager-at-boot readiness of the dissolved authorizer factory (the former
 * {@code RangerPolarisAuthorizerFactory}, dissolved into per-{@code @Identifier} CDI beans).
 *
 * <p>When Ranger is the active authorization type, {@code RangerProductionReadinessChecks} MUST
 * resolve the application-scoped {@code RangerPolarisAuthorizerProducer} at startup, which forces
 * its constructor (config validation + embedded Ranger authorizer init) to run at boot. This is the
 * fail-fast the factory constructor used to give. If someone reverts the check to lazy (drops the
 * eager resolve), a Ranger init failure would only surface on the first request instead of at boot.
 * These tests go red on that revert.
 */
public class RangerProductionReadinessChecksTest {

  @Test
  public void rangerActive_eagerlyResolvesProducer_propagatingInitFailureAtBoot() {
    RangerProductionReadinessChecks checks = new RangerProductionReadinessChecks();
    @SuppressWarnings("unchecked")
    Instance<RangerPolarisAuthorizerProducer> producer = mock(Instance.class);
    // A failed embedded-Ranger init throws when the producer's constructor runs on first resolve.
    IllegalStateException bootFailure =
        new IllegalStateException("Ranger authorizer was not initialized successfully");
    when(producer.get()).thenThrow(bootFailure);

    // type == ranger: the check must resolve the producer eagerly, so the failure surfaces at boot.
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class, () -> checks.checkRangerAuthorization("ranger", producer));
    assertSame(bootFailure, thrown);
    verify(producer).get();
  }

  @Test
  public void rangerNotActive_neverResolvesProducer() {
    RangerProductionReadinessChecks checks = new RangerProductionReadinessChecks();
    @SuppressWarnings("unchecked")
    Instance<RangerPolarisAuthorizerProducer> producer = mock(Instance.class);

    // type != ranger: the Ranger authorizer is never initialized.
    ProductionReadinessCheck result = checks.checkRangerAuthorization("internal", producer);

    assertSame(ProductionReadinessCheck.OK, result);
    verify(producer, never()).get();
  }
}
