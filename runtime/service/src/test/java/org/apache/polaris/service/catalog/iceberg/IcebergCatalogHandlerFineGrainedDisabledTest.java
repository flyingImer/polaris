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
package org.apache.polaris.service.catalog.iceberg;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.catalog.FederatedCatalogFactory;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.events.EventAttributeMap;
import org.apache.polaris.service.Profiles;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.catalog.AccessDelegationModeResolver;
import org.apache.polaris.service.reporting.PolarisMetricsReporter;
import org.apache.polaris.spi.feature.CatalogPrefixParser;
import org.apache.polaris.spi.substrate.TaskExecutor;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

/**
 * Test class specifically for testing fine-grained authorization when the feature is DISABLED. This
 * ensures that fine-grained privileges are properly ignored when the feature flag is off.
 *
 * <p>Drives the merged Iceberg catalog feature-SPI implementation ({@link LocalIcebergCatalog}),
 * which absorbed the retired {@code IcebergCatalogHandler} (Issue 29), via a thin {@link
 * MergedCatalogHandle} shim (a smaller copy of the one in {@code
 * AbstractIcebergCatalogHandlerAuthzTest}). The FGAC-disabled behavior is driven by the real {@link
 * Profile} config override, so {@code updateTable} runs the merged catalog's coarse-grained authz
 * path against the real metastore.
 */
@SuppressWarnings("resource")
@QuarkusTest
@TestProfile(IcebergCatalogHandlerFineGrainedDisabledTest.Profile.class)
public class IcebergCatalogHandlerFineGrainedDisabledTest extends PolarisAuthzTestBase {

  @Inject TaskExecutor taskExecutor;
  @Inject PolarisCredentialManager credentialManager;
  @Inject @Any Instance<FederatedCatalogFactory> federatedCatalogFactories;
  @Inject CatalogHandlerUtils catalogHandlerUtils;
  @Inject EventAttributeMap eventAttributeMap;
  @Inject Clock clock;
  @Inject AccessDelegationModeResolver accessDelegationModeResolver;
  @Inject PolarisMetricsReporter metricsReporter;
  @Inject CatalogPrefixParser prefixParser;

  private MergedCatalogHandle newHandler() {
    PolarisPrincipal authenticatedPrincipal = PolarisPrincipal.of(principalEntity, Set.of());
    return new MergedCatalogHandle(
        () -> buildMergedCatalog(CATALOG_NAME, authenticatedPrincipal, callContext));
  }

  /**
   * Builds the merged Iceberg catalog directly (mirroring {@link LocalIcebergCatalogFactory}) from
   * the substrate collaborators injected here plus the base class. The anonymous {@code initialize}
   * override forces the in-memory FileIO the base test fixtures write to.
   */
  private LocalIcebergCatalog buildMergedCatalog(
      String catalogName, PolarisPrincipal principal, CallContext ctx) {
    return new LocalIcebergCatalog(
        catalogName,
        principal,
        ctx,
        diagServices,
        entityResolver,
        polarisAuthorizer,
        metaStoreManager,
        taskExecutor,
        storageAccessConfigProvider,
        fileIOFactory,
        polarisEventDispatcher,
        eventMetadataFactory,
        credentialManager,
        federatedCatalogFactories,
        reservedProperties,
        catalogHandlerUtils,
        eventAttributeMap,
        clock,
        accessDelegationModeResolver,
        metricsReporter,
        prefixParser) {
      @Override
      public void initialize(String name, Map<String, String> properties) {
        Map<String, String> withInMemoryIo = new HashMap<>(properties);
        withInMemoryIo.put(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO");
        super.initialize(name, withInMemoryIo);
      }
    };
  }

  /**
   * Thin test shim over the merged {@link LocalIcebergCatalog}: exposes the retired {@code
   * IcebergCatalogHandler#updateTable} signature the test below calls and unwraps the feature-SPI
   * PolarisResult return type, so the authz assertions are preserved verbatim. A fresh merged
   * catalog per op matches production, where {@code IcebergCatalogAdapter} builds a new instance
   * per REST request.
   */
  static final class MergedCatalogHandle {
    private final Supplier<LocalIcebergCatalog> catalogSupplier;

    MergedCatalogHandle(Supplier<LocalIcebergCatalog> catalogSupplier) {
      this.catalogSupplier = catalogSupplier;
    }

    LoadTableResponse updateTable(TableIdentifier tableIdentifier, UpdateTableRequest request) {
      return catalogSupplier.get().updateTable(tableIdentifier, request).body();
    }
  }

  public static class Profile extends Profiles.PolarisAuthzBaseProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("polaris.features.\"ENABLE_FINE_GRAINED_UPDATE_TABLE_PRIVILEGES\"", "false")
          .build();
    }
  }

  @TestFactory
  Stream<DynamicNode> testUpdateTableFineGrainedPrivilegesIgnoredWhenFeatureDisabled() {
    // Test that when fine-grained authorization is disabled, fine-grained privileges alone are
    // insufficient
    // This ensures the feature flag properly controls behavior and fine-grained privileges don't
    // "leak through"
    UpdateTableRequest request =
        UpdateTableRequest.create(
            TABLE_NS1A_2,
            List.of(), // no requirements
            List.of(new MetadataUpdate.AssignUUID(UUID.randomUUID().toString())));

    // With fine-grained authorization disabled, even having the specific fine-grained privilege
    // should be insufficient - the system should require the broader privileges
    return authzTestsBuilder("updateTable")
        .action(() -> newHandler().updateTable(TABLE_NS1A_2, request))
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldFailWith(
            PolarisPrivilege
                .TABLE_ASSIGN_UUID) // This alone should be insufficient when feature disabled
        .createTests();
  }
}
