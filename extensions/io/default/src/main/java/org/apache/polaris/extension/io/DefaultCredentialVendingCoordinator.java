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

package org.apache.polaris.extension.io;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.CredentialVendingCoordinator;
import org.apache.polaris.core.storage.LocationGrant;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageCredentialVendor;
import org.apache.polaris.core.storage.StorageCredentialVendorFactory;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default entry point for vending scoped storage credentials. Resolves the storage-config-bearing
 * entity for the given entity path, derives a string key from its storage type, selects the
 * matching {@link StorageCredentialVendorFactory} out of the {@code @Any} CDI instances registered
 * for that key, builds a {@link CredentialVendingContext} from the per-call caller/realm state, and
 * delegates to the fresh vendor the factory returns.
 *
 * <p>Application-scoped rather than request-scoped: the only collaborator held at construction is
 * the {@code Instance<StorageCredentialVendorFactory>} lookup, which is itself request-agnostic
 * (each {@code createVendor} call is independent). The request-scoped state a given call needs
 * (call context, principal) travels as method parameters instead of injected fields.
 */
@ApplicationScoped
public class DefaultCredentialVendingCoordinator implements CredentialVendingCoordinator {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DefaultCredentialVendingCoordinator.class);

  private final Instance<StorageCredentialVendorFactory> vendorFactories;

  @Inject
  public DefaultCredentialVendingCoordinator(
      @Any Instance<StorageCredentialVendorFactory> vendorFactories) {
    this.vendorFactories = vendorFactories;
  }

  @Override
  public StorageAccessConfig getStorageAccessConfig(
      @NonNull TableIdentifier tableIdentifier,
      @NonNull Set<String> tableLocations,
      @NonNull Set<PolarisStorageActions> storageActions,
      @NonNull Optional<String> refreshCredentialsEndpoint,
      @NonNull PolarisResolvedPathWrapper resolvedPath,
      @NonNull CallContext callContext,
      @NonNull PolarisPrincipal principal) {
    LOGGER
        .atDebug()
        .addKeyValue("tableIdentifier", tableIdentifier)
        .addKeyValue("tableLocation", tableLocations)
        .log("Fetching client credentials for table");

    StorageAccessConfig accessConfig =
        getStorageAccessConfig(
            resolvedPath.getRawFullPath(),
            tableLocations,
            storageActions,
            refreshCredentialsEndpoint,
            callContext,
            principal);

    LOGGER
        .atDebug()
        .addKeyValue("tableIdentifier", tableIdentifier)
        .addKeyValue("credentialKeys", accessConfig.credentials().keySet())
        .addKeyValue("extraProperties", accessConfig.extraProperties())
        .log("Loaded scoped credentials for table");
    if (accessConfig.credentials().isEmpty()) {
      LOGGER.debug("No credentials found for table");
    }
    return accessConfig;
  }

  private StorageAccessConfig getStorageAccessConfig(
      @NonNull List<PolarisEntity> resolvedEntityPath,
      @NonNull Set<String> locations,
      @NonNull Set<PolarisStorageActions> storageActions,
      @NonNull Optional<String> refreshCredentialsEndpoint,
      @NonNull CallContext callContext,
      @NonNull PolarisPrincipal principal) {

    boolean skipCredentialSubscopingIndirection =
        callContext
            .getRealmConfig()
            .getConfig(FeatureConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION);
    if (skipCredentialSubscopingIndirection) {
      return StorageAccessConfig.builder().supportsCredentialVending(false).build();
    }

    Optional<PolarisEntity> resolvedStorageEntity =
        PolarisStorageConfigurationInfo.findStorageInfoFromHierarchy(resolvedEntityPath);
    if (resolvedStorageEntity.isEmpty()) {
      return StorageAccessConfig.builder().supportsCredentialVending(false).build();
    }

    PolarisEntity storageEntity = resolvedStorageEntity.get();
    PolarisStorageConfigurationInfo storageConfig =
        PolarisStorageConfigurationInfo.deserialize(
            storageEntity
                .getInternalPropertiesAsMap()
                .get(PolarisEntityConstants.getStorageConfigInfoPropertyName()));
    String storageTypeKey = storageConfig.getStorageType().name().toLowerCase(Locale.ROOT);

    Instance<StorageCredentialVendorFactory> selectedFactory =
        vendorFactories.select(Identifier.Literal.of(storageTypeKey));
    if (selectedFactory.isUnsatisfied()) {
      return StorageAccessConfig.builder().supportsCredentialVending(false).build();
    }

    StorageCredentialVendor vendor = selectedFactory.get().createVendor(storageEntity);

    CredentialVendingContext credentialVendingContext =
        buildCredentialVendingContext(resolvedEntityPath, callContext, principal);

    // Non-delegated loadTable still calls in here to fetch storage extra-properties
    // (endpoint/region/path-style) and passes no actions. An empty-actions grant is normalized to
    // a READ grant by the vendor contract itself (see
    // StorageCredentialVendor#normalizeEmptyActionsToRead), so it is passed straight through here.
    return vendor.getStorageAccessConfig(
        List.of(new LocationGrant(locations, storageActions)),
        refreshCredentialsEndpoint,
        credentialVendingContext);
  }

  private CredentialVendingContext buildCredentialVendingContext(
      List<PolarisEntity> resolvedEntityPath, CallContext callContext, PolarisPrincipal principal) {
    CredentialVendingContext.Builder builder = CredentialVendingContext.builder();

    List<String> sessionTagFields =
        callContext
            .getRealmConfig()
            .getConfig(FeatureConfiguration.SESSION_TAGS_IN_SUBSCOPED_CREDENTIAL);

    builder.realm(Optional.of(callContext.getRealmContext().getRealmIdentifier()));

    if (!resolvedEntityPath.isEmpty()) {
      // First entity is the catalog
      builder.catalogName(Optional.of(resolvedEntityPath.get(0).getName()));

      // Last entity is the table/view
      PolarisEntity leaf = resolvedEntityPath.get(resolvedEntityPath.size() - 1);
      if (leaf.getType() == PolarisEntityType.TABLE_LIKE
          || leaf.getType() == PolarisEntityType.TASK) {
        builder.tableName(Optional.of(leaf.getName()));
      }

      // Namespace entities are between catalog and leaf
      if (resolvedEntityPath.size() > 2) {
        String namespace =
            resolvedEntityPath.subList(1, resolvedEntityPath.size() - 1).stream()
                .map(PolarisEntity::getName)
                .collect(Collectors.joining("."));
        builder.namespace(Optional.of(namespace));
      }
    }

    builder.principalName(Optional.of(principal.getName()));

    Set<String> roles = principal.getRoles();
    if (roles != null && !roles.isEmpty()) {
      String rolesString = roles.stream().sorted().collect(Collectors.joining(","));
      builder.activatedRoles(Optional.of(rolesString));
    }

    if (sessionTagFields.contains(FeatureConfiguration.SESSION_TAG_FIELD_TRACE_ID)) {
      builder.traceId(getCurrentTraceId());
    }

    return builder.build();
  }

  private Optional<String> getCurrentTraceId() {
    SpanContext spanContext = Span.current().getSpanContext();
    if (spanContext.isValid()) {
      return Optional.of(spanContext.getTraceId());
    }
    return Optional.empty();
  }
}
