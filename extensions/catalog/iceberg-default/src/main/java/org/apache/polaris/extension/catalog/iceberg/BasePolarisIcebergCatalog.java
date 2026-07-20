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
package org.apache.polaris.extension.catalog.iceberg;

import static org.apache.polaris.core.catalog.ExceptionUtils.alreadyExistsExceptionForTableLikeEntity;
import static org.apache.polaris.core.catalog.ExceptionUtils.noSuchNamespaceException;
import static org.apache.polaris.core.catalog.ExceptionUtils.notFoundExceptionForTableLikeEntity;
import static org.apache.polaris.extension.catalog.iceberg.StorageProviderExceptionClassifier.isStorageProviderRetryableException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.inject.Instance;
import java.io.Closeable;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterViewRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.catalog.CatalogUtils;
import org.apache.polaris.core.catalog.FederatedCatalogFactory;
import org.apache.polaris.core.config.BehaviorChangeConfiguration;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ConnectionType;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisEntityUtils;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.events.EventAttributeMap;
import org.apache.polaris.core.events.IcebergEventAttributes;
import org.apache.polaris.core.exceptions.CommitConflictException;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.TransactionWorkspaceMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.EntityResolverManifestView;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolutionRequest;
import org.apache.polaris.core.persistence.resolver.ResolutionResult;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.rest.IcebergHttpUtil;
import org.apache.polaris.core.rest.NamespaceUtils;
import org.apache.polaris.core.rest.PolarisEndpoints;
import org.apache.polaris.core.storage.LocationUtils;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.core.storage.StorageUtil;
import org.apache.polaris.spi.durable.DurableManager;
import org.apache.polaris.spi.feature.CatalogPrefixParser;
import org.apache.polaris.spi.feature.catalog.AccessDelegationMode;
import org.apache.polaris.spi.feature.catalog.AccessDelegationModeResolver;
import org.apache.polaris.spi.feature.catalog.ETagCarrier;
import org.apache.polaris.spi.feature.catalog.ExtensionPayload;
import org.apache.polaris.spi.feature.catalog.IcebergCatalogOps;
import org.apache.polaris.spi.feature.catalog.IcebergViewCatalogOps;
import org.apache.polaris.spi.feature.catalog.IfNoneMatch;
import org.apache.polaris.spi.feature.catalog.NotificationRequest;
import org.apache.polaris.spi.feature.catalog.PolarisResult;
import org.apache.polaris.spi.feature.catalog.SupportsNotifications;
import org.apache.polaris.spi.substrate.EntityResolver;
import org.apache.polaris.spi.substrate.PolarisAuthorizer;
import org.apache.polaris.spi.substrate.PolarisEventDispatcher;
import org.apache.polaris.spi.substrate.PolarisEventMetadataFactory;
import org.apache.polaris.spi.substrate.PolarisMetricsReporter;
import org.apache.polaris.spi.substrate.ReservedProperties;
import org.apache.polaris.spi.substrate.StorageAccessConfigProvider;
import org.apache.polaris.spi.substrate.StorageIoProvider;
import org.apache.polaris.spi.substrate.TaskExecutor;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The OSS-default Iceberg catalog feature-SPI implementation (Issue 29): implements {@link
 * IcebergCatalogOps} and {@link IcebergViewCatalogOps} directly (no longer extends the Iceberg
 * SDK's own {@code Catalog}/{@code ViewCatalog}/{@code SupportsNamespaces} -- see {@link
 * BridgeBaseMetastoreViewCatalog} for the composed Iceberg-mechanics delegate that handles that
 * half). Generic over {@code E} so a provider can extend this class with its own {@link
 * ExtensionPayload} subtype to carry operation metadata the plain Iceberg response cannot express;
 * the OSS default concrete subclass ({@code PolarisIcebergCatalog}) instantiates {@code E =
 * ETagPayload}. {@code E} is bound to {@link ETagCarrier} too (Issue 32) so the three operations
 * with a per-call ETag -- {@code loadTable}, {@code createTableDirect}, {@code registerTable} --
 * can always attach it via {@link #withEtag}; every other operation just threads the
 * constructor-supplied {@link #extensionValue} through unchanged. Defines the relationship between
 * PolarisEntities and Iceberg's business logic.
 */
public abstract class BasePolarisIcebergCatalog<E extends ExtensionPayload & ETagCarrier>
    implements IcebergCatalogOps<E>, IcebergViewCatalogOps<E>, Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BasePolarisIcebergCatalog.class);

  private static final Joiner SLASH = Joiner.on("/");

  public static final Predicate<Exception> SHOULD_RETRY_REFRESH_PREDICATE =
      ex -> {
        // Default arguments from BaseMetastoreTableOperation only stop retries on
        // NotFoundException. We should more carefully identify the set of retriable
        // and non-retriable exceptions here.
        return !(ex instanceof NotFoundException)
            && !(ex instanceof IllegalArgumentException)
            && !(ex instanceof AlreadyExistsException)
            && !(ex instanceof ForbiddenException)
            && !(ex instanceof UnprocessableEntityException)
            && (isStorageProviderRetryableException(ex)
                || isStorageProviderRetryableException(Throwables.getRootCause(ex)));
      };

  private final PolarisDiagnostics diagnostics;
  private final EntityResolver entityResolver;
  private final CallContext callContext;
  private final RealmConfig realmConfig;
  protected PolarisResolutionManifestCatalogView resolvedEntityView;
  private CatalogEntity catalogEntity;
  private final TaskExecutor taskExecutor;
  private final PolarisPrincipal principal;
  private final PolarisEventDispatcher polarisEventDispatcher;
  private final PolarisEventMetadataFactory eventMetadataFactory;
  private final AtomicBoolean loggedPrefixOverlapWarning = new AtomicBoolean(false);

  private String ioImplClassName;
  private FileIO catalogFileIO;
  private CloseableGroup closeableGroup;
  private Map<String, String> tableDefaultProperties;

  private String catalogName;
  private long catalogId;
  private String defaultBaseLocation;
  private Map<String, String> catalogProperties;
  private final StorageAccessConfigProvider storageAccessConfigProvider;
  private final StorageIoProvider storageIoProvider;
  private DurableManager metaStoreManager;

  /**
   * The provider-private extension payload this instance attaches to every {@link PolarisResult} it
   * returns, for every operation EXCEPT {@code loadTable}/{@code createTableDirect}/{@code
   * registerTable} (which each need a fresh, per-call value -- see {@link #withEtag}). Supplied by
   * the constructor so a subclass fixes its own {@code E}; the OSS default concrete subclass always
   * passes {@code ETagPayload.NONE}.
   */
  protected final E extensionValue;

  /**
   * Constructs a fresh {@code E} carrying {@code etag}, for the three operations ({@code
   * loadTable}, {@code createTableDirect}, {@code registerTable}) that need a per-call extension
   * value instead of the constructor-supplied {@link #extensionValue} (Issue 32). A generic method
   * body can never construct an arbitrary instance of its own type parameter, so this hook exists
   * purely to let the concrete subclass -- where {@code E} is bound to an actual type -- supply
   * one; it needs no other collaborator, business, or authz logic.
   */
  protected abstract E withEtag(Optional<String> etag);

  // --- Issue 29: merged Iceberg catalog feature-SPI collaborators + dispatch state. Set only via
  // the feature-SPI constructor below; they stay null on the legacy view-taking constructor path,
  // whose instances never receive feature-SPI op calls (the adapter still routes through the old
  // IcebergCatalogHandler until Inc6). ---
  protected CatalogAuthorizer authz;
  private PolarisCredentialManager credentialManager;
  private Instance<FederatedCatalogFactory> federatedCatalogFactories;
  private ReservedProperties reservedProperties;
  private CatalogHandlerUtils catalogHandlerUtils;
  private EventAttributeMap eventAttributeMap;
  private Clock clock;
  private AccessDelegationModeResolver accessDelegationModeResolver;
  private PolarisMetricsReporter polarisMetricsReporter;
  private CatalogPrefixParser prefixParser;

  // Local-vs-federated dispatch state, established by ensureBaseInitialized() after authorization.
  // Local: baseCatalog/namespaceCatalog/viewCatalog point at the composed polarisIcebergCatalog
  // delegate below (Issue 29 Rework R4 — this class no longer extends the Iceberg SDK's own
  // Catalog/ViewCatalog/SupportsNamespaces, so it can no longer serve as its own local delegate);
  // federated: a narrow remote delegate.
  private BridgeBaseMetastoreViewCatalog polarisIcebergCatalog;
  private Catalog federatedDelegate;
  protected boolean isFederated = false;
  protected boolean baseInitialized = false;
  protected Catalog baseCatalog;
  protected SupportsNamespaces namespaceCatalog;
  protected ViewCatalog viewCatalog;

  /**
   * @param callContext the current CallContext
   * @param resolvedEntityView accessor to resolved entity paths that have been pre-vetted to ensure
   *     this catalog instance only interacts with authorized resolved paths.
   * @param taskExecutor Executor we use to register cleanup task handlers
   * @param extensionValue the provider-private extension payload this instance attaches to every
   *     result it returns (the OSS default concrete subclass always passes {@code
   *     ETagPayload.NONE})
   */
  public BasePolarisIcebergCatalog(
      PolarisDiagnostics diagnostics,
      EntityResolver entityResolver,
      DurableManager metaStoreManager,
      CallContext callContext,
      PolarisResolutionManifestCatalogView resolvedEntityView,
      PolarisPrincipal principal,
      TaskExecutor taskExecutor,
      StorageAccessConfigProvider storageAccessConfigProvider,
      StorageIoProvider storageIoProvider,
      PolarisEventDispatcher polarisEventDispatcher,
      PolarisEventMetadataFactory eventMetadataFactory,
      E extensionValue) {
    this.diagnostics = diagnostics;
    this.entityResolver = entityResolver;
    this.callContext = callContext;
    this.realmConfig = callContext.getRealmConfig();
    this.resolvedEntityView = resolvedEntityView;
    this.catalogEntity = resolvedEntityView.getResolvedCatalogEntity();
    this.principal = principal;
    this.taskExecutor = taskExecutor;
    this.catalogId = catalogEntity.getId();
    this.catalogName = catalogEntity.getName();
    this.storageAccessConfigProvider = storageAccessConfigProvider;
    this.storageIoProvider = storageIoProvider;
    this.metaStoreManager = metaStoreManager;
    this.polarisEventDispatcher = polarisEventDispatcher;
    this.eventMetadataFactory = eventMetadataFactory;
    this.extensionValue = extensionValue;
    // Legacy path: the view is supplied eagerly, so base construction is already complete.
    this.baseInitialized = true;
  }

  /**
   * Feature-SPI constructor (Issue 29): builds the catalog EARLY from the catalog name + principal
   * + collaborators, with NO resolved-entity view yet. {@link #ensureBaseInitialized()} completes
   * construction after the composed {@link CatalogAuthorizer} resolves the entity view during the
   * first authorized operation, deciding local vs federated there (mirroring the retired handler's
   * lazy {@code initializeCatalog()}).
   */
  public BasePolarisIcebergCatalog(
      String catalogName,
      PolarisPrincipal principal,
      CallContext callContext,
      PolarisDiagnostics diagnostics,
      EntityResolver entityResolver,
      PolarisAuthorizer authorizer,
      DurableManager metaStoreManager,
      TaskExecutor taskExecutor,
      StorageAccessConfigProvider storageAccessConfigProvider,
      StorageIoProvider storageIoProvider,
      PolarisEventDispatcher polarisEventDispatcher,
      PolarisEventMetadataFactory eventMetadataFactory,
      PolarisCredentialManager credentialManager,
      Instance<FederatedCatalogFactory> federatedCatalogFactories,
      ReservedProperties reservedProperties,
      CatalogHandlerUtils catalogHandlerUtils,
      EventAttributeMap eventAttributeMap,
      Clock clock,
      AccessDelegationModeResolver accessDelegationModeResolver,
      PolarisMetricsReporter polarisMetricsReporter,
      CatalogPrefixParser prefixParser,
      E extensionValue) {
    this.diagnostics = diagnostics;
    this.entityResolver = entityResolver;
    this.callContext = callContext;
    this.realmConfig = callContext.getRealmConfig();
    this.principal = principal;
    this.taskExecutor = taskExecutor;
    this.catalogName = catalogName;
    this.storageAccessConfigProvider = storageAccessConfigProvider;
    this.storageIoProvider = storageIoProvider;
    this.metaStoreManager = metaStoreManager;
    this.polarisEventDispatcher = polarisEventDispatcher;
    this.eventMetadataFactory = eventMetadataFactory;
    this.credentialManager = credentialManager;
    this.federatedCatalogFactories = federatedCatalogFactories;
    this.reservedProperties = reservedProperties;
    this.catalogHandlerUtils = catalogHandlerUtils;
    this.eventAttributeMap = eventAttributeMap;
    this.clock = clock;
    this.accessDelegationModeResolver = accessDelegationModeResolver;
    this.polarisMetricsReporter = polarisMetricsReporter;
    this.prefixParser = prefixParser;
    this.extensionValue = extensionValue;
    this.authz = new CatalogAuthorizer(entityResolver, authorizer, principal, catalogName);
    // resolvedEntityView / catalogEntity / catalogId are established by ensureBaseInitialized().
  }

  @Override
  public void close() throws IOException {
    if (closeableGroup != null) {
      closeableGroup.close();
    }
    if (polarisIcebergCatalog != null) {
      polarisIcebergCatalog.close();
    }
  }

  private String buildPrefixedLocation(TableIdentifier tableIdentifier) {
    StringBuilder locationBuilder = new StringBuilder();
    locationBuilder.append(defaultBaseLocation);
    if (!defaultBaseLocation.endsWith("/")) {
      locationBuilder.append("/");
    }

    locationBuilder.append(LocationUtils.computeHash(tableIdentifier.toString()));

    for (String ns : tableIdentifier.namespace().levels()) {
      locationBuilder.append("/").append(URLEncoder.encode(ns, Charset.defaultCharset()));
    }
    locationBuilder
        .append("/")
        .append(URLEncoder.encode(tableIdentifier.name(), Charset.defaultCharset()))
        .append("/");
    return locationBuilder.toString();
  }

  /**
   * Applies the rule controlled by DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED to a tablelike
   * location
   */
  private String applyDefaultLocationObjectStoragePrefix(
      TableIdentifier tableIdentifier, String location) {
    boolean prefixEnabled =
        realmConfig.getConfig(
            FeatureConfiguration.DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED, catalogEntity);
    boolean allowUnstructuredTableLocation =
        realmConfig.getConfig(
            FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION, catalogEntity);
    boolean allowTableLocationOverlap =
        realmConfig.getConfig(FeatureConfiguration.ALLOW_TABLE_LOCATION_OVERLAP, catalogEntity);
    boolean optimizedSiblingCheck =
        realmConfig.getConfig(FeatureConfiguration.OPTIMIZED_SIBLING_CHECK, catalogEntity);
    if (location != null) {
      return location;
    } else if (!prefixEnabled) {
      return location;
    } else if (!allowUnstructuredTableLocation) {
      throw new IllegalStateException(
          String.format(
              "The configuration %s is enabled, but %s is not enabled",
              FeatureConfiguration.DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED.key(),
              FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.key()));
    } else if (!allowTableLocationOverlap) {
      // TODO consider doing this check any time ALLOW_EXTERNAL_TABLE_LOCATION is enabled, not just
      // here
      if (!optimizedSiblingCheck) {
        throw new IllegalStateException(
            String.format(
                "%s and %s are both disabled, which means that table location overlap checks are being"
                    + " performed, but only within each namespace. However, %s is enabled, which indicates"
                    + " that tables may be created outside of their parent namespace. This is not a safe"
                    + " combination of configurations.",
                FeatureConfiguration.ALLOW_TABLE_LOCATION_OVERLAP.key(),
                FeatureConfiguration.OPTIMIZED_SIBLING_CHECK.key(),
                FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.key()));
      } else if (!loggedPrefixOverlapWarning.getAndSet(true)) {
        LOGGER.warn(
            "A table is being created with {} and {} enabled, but with {} disabled. "
                + "This is a safe combination of configurations which may prevent table overlap, but only if the "
                + "underlying persistence actually implements %s. Exercise caution.",
            FeatureConfiguration.DEFAULT_LOCATION_OBJECT_STORAGE_PREFIX_ENABLED.key(),
            FeatureConfiguration.OPTIMIZED_SIBLING_CHECK.key(),
            FeatureConfiguration.ALLOW_TABLE_LOCATION_OVERLAP.key());
      }
      return buildPrefixedLocation(tableIdentifier);
    } else {
      return buildPrefixedLocation(tableIdentifier);
    }
  }

  /**
   * Based on configuration settings, for callsites that need to handle potentially setting a new
   * base location for a TableLike entity, produces the transformed location if applicable, or else
   * the unaltered specified location.
   */
  public String transformTableLikeLocation(TableIdentifier tableIdentifier, String location) {
    return applyDefaultLocationObjectStoragePrefix(tableIdentifier, location);
  }

  void validateStagedTableCreate(TableIdentifier tableIdentifier, TableMetadata tableMetadata) {
    PolarisResolvedPathWrapper resolvedStorageEntity =
        CatalogUtils.findResolvedStorageEntity(resolvedEntityView, tableIdentifier);
    if (resolvedStorageEntity == null) {
      throw noSuchNamespaceException(tableIdentifier.namespace());
    }
    Set<String> dataLocations =
        StorageUtil.getLocationsUsedByTable(tableMetadata.location(), tableMetadata.properties());
    CatalogUtils.validateLocationsForTableLike(
        realmConfig, tableIdentifier, dataLocations, resolvedStorageEntity);
    List<PolarisEntity> resolvedNamespace = resolvedStorageEntity.getRawFullPath();
    PolarisEntity storageLeafEntity = resolvedStorageEntity.getRawLeafEntity();
    dataLocations.forEach(
        location ->
            validateNoLocationOverlap(
                catalogEntity, tableIdentifier, resolvedNamespace, location, storageLeafEntity));
  }

  /**
   * Validates the table location has no overlap with other entities after checking the
   * configuration of the service
   */
  private void validateNoLocationOverlap(
      CatalogEntity catalog,
      TableIdentifier identifier,
      List<PolarisEntity> resolvedNamespace,
      String location,
      PolarisEntity entity) {
    boolean validateViewOverlap =
        realmConfig.getConfig(BehaviorChangeConfiguration.VALIDATE_VIEW_LOCATION_OVERLAP);

    if (realmConfig.getConfig(FeatureConfiguration.ALLOW_TABLE_LOCATION_OVERLAP, catalog)) {
      LOGGER.debug("Skipping location overlap validation for identifier '{}'", identifier);
    } else if (validateViewOverlap
        || entity.getSubType().equals(PolarisEntitySubType.ICEBERG_TABLE)) {
      LOGGER.debug("Validating no overlap with sibling tables or namespaces");

      // Create a fake IcebergTableLikeEntity to check for overlap, since no real entity
      // has been created yet.
      var lastNamespace = resolvedNamespace.getLast();
      IcebergTableLikeEntity virtualEntity =
          IcebergTableLikeEntity.of(
              new PolarisEntity.Builder()
                  .setName(identifier.name())
                  .setType(PolarisEntityType.TABLE_LIKE)
                  .setSubType(PolarisEntitySubType.ICEBERG_TABLE)
                  .setParentId(lastNamespace.getId())
                  .setCatalogId(lastNamespace.getCatalogId())
                  .setProperties(Map.of(PolarisEntityConstants.ENTITY_BASE_LOCATION, location))
                  .build());

      validateNoLocationOverlap(virtualEntity, resolvedNamespace);
    }
  }

  /**
   * Validate no location overlap exists between the entity path and its sibling entities. This
   * resolves all siblings at the same level as the target entity (namespaces if the target entity
   * is a namespace whose parent is the catalog, namespaces and tables otherwise) and checks the
   * base-location property of each. The target entity's base location may not be a prefix or a
   * suffix of any sibling entity's base location.
   */
  private <T extends PolarisEntity & LocationBasedEntity> void validateNoLocationOverlap(
      T entity, List<PolarisEntity> parentPath) {

    String location = entity.getBaseLocation();
    String name = entity.getName();

    // Attempt to directly query for siblings
    boolean useOptimizedSiblingCheck =
        realmConfig.getConfig(FeatureConfiguration.OPTIMIZED_SIBLING_CHECK);
    if (useOptimizedSiblingCheck) {
      Optional<Optional<String>> directSiblingCheckResult =
          getMetaStoreManager().hasOverlappingSiblings(getCurrentPolarisContext(), entity);
      if (directSiblingCheckResult.isPresent()) {
        if (directSiblingCheckResult.get().isPresent()) {
          throw new org.apache.iceberg.exceptions.ForbiddenException(
              "Unable to create entity at location '%s' because it conflicts with existing table or namespace at %s",
              location, directSiblingCheckResult.get().get());
        } else {
          return;
        }
      }
    }

    // if the entity path has more than just the catalog, check for tables as well as other
    // namespaces
    Optional<NamespaceEntity> parentNamespace =
        parentPath.size() > 1
            ? Optional.of(NamespaceEntity.of(parentPath.getLast()))
            : Optional.empty();

    // Fall through by listing everything:
    ListEntitiesResult siblingNamespacesResult =
        getMetaStoreManager()
            .listEntities(
                getCurrentPolarisContext(),
                PolarisEntity.toCoreList(parentPath),
                PolarisEntityType.NAMESPACE,
                PolarisEntitySubType.ANY_SUBTYPE,
                PageToken.readEverything());
    if (!siblingNamespacesResult.isSuccess()) {
      throw new IllegalStateException(
          "Unable to resolve siblings entities to validate location - could not list namespaces");
    }

    List<TableIdentifier> siblingTables =
        parentNamespace
            .map(
                ns -> {
                  ListEntitiesResult siblingTablesResult =
                      getMetaStoreManager()
                          .listEntities(
                              getCurrentPolarisContext(),
                              PolarisEntity.toCoreList(parentPath),
                              PolarisEntityType.TABLE_LIKE,
                              PolarisEntitySubType.ANY_SUBTYPE,
                              PageToken.readEverything());
                  if (!siblingTablesResult.isSuccess()) {
                    throw new IllegalStateException(
                        "Unable to resolve siblings entities to validate location - could not list tables");
                  }
                  return siblingTablesResult.getEntities().stream()
                      .map(tbl -> TableIdentifier.of(ns.asNamespace(), tbl.getName()))
                      .collect(Collectors.toList());
                })
            .orElse(List.of());

    List<Namespace> siblingNamespaces =
        siblingNamespacesResult.getEntities().stream()
            .map(
                ns -> {
                  String[] nsLevels =
                      parentNamespace
                          .map(parent -> parent.asNamespace().levels())
                          .orElse(new String[0]);
                  String[] newLevels = Arrays.copyOf(nsLevels, nsLevels.length + 1);
                  newLevels[nsLevels.length] = ns.getName();
                  return Namespace.of(newLevels);
                })
            .toList();
    List<ResolvedPathKey> pathsToResolve =
        new ArrayList<>(siblingTables.size() + siblingNamespaces.size());
    siblingTables.forEach(
        tbl -> {
          if (!tbl.name().equals(name)) {
            pathsToResolve.add(ResolvedPathKey.ofTableLike(tbl));
          }
        });
    siblingNamespaces.forEach(
        ns -> {
          if (!ns.level(ns.length() - 1).equals(name)) {
            pathsToResolve.add(ResolvedPathKey.ofNamespace(ns));
          }
        });

    StorageLocation targetLocation = StorageLocation.of(location);
    for (PolarisEntity entityToCheck :
        resolveOptionalPaths(pathsToResolve, parentPath.getFirst().getName())) {
      PolarisEntityUtils.asLocationBasedEntity(entityToCheck)
          .map(LocationBasedEntity::getBaseLocation)
          .map(StorageLocation::of)
          .ifPresent(
              siblingLocation -> {
                if (targetLocation.isChildOf(siblingLocation)
                    || siblingLocation.isChildOf(targetLocation)) {
                  throw new ForbiddenException(
                      "Unable to create entity at location '%s' because it conflicts with existing table or namespace at "
                          + "location '%s'",
                      targetLocation, siblingLocation);
                }
              });
    }
  }

  @VisibleForTesting
  public List<PolarisEntity> resolveOptionalPaths(List<ResolvedPathKey> keys, String catalogName) {
    LOGGER.debug("Resolving {} sibling entities to validate location", keys.size());

    List<ResolverPath> paths = new ArrayList<>(keys.size());
    keys.forEach(k -> paths.add(new ResolverPath(k, true))); // optional path

    ResolutionResult resolution =
        entityResolver.resolve(new ResolutionRequest(principal, catalogName, paths, List.of()));
    ResolverStatus status = resolution.status();

    if (status.getStatus() != ResolverStatus.StatusEnum.SUCCESS) {
      String message =
          "Unable to resolve sibling entities to validate location - " + status.getStatus();
      if (status.getStatus().equals(ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED)) {
        message += ". Could not resolve entity: " + status.getFailedToResolvedEntityName();
      } else if (status
          .getStatus()
          .equals(ResolverStatus.StatusEnum.PATH_COULD_NOT_BE_FULLY_RESOLVED)) {
        ResolverPath path = status.getFailedToResolvePath();
        if (path != null) {
          message += ". path: " + String.join(".", path.entityNames());
          message += ", failed index: " + status.getFailedToResolvedEntityIndex();
        }
      }

      throw new CommitConflictException(message);
    }

    // Read leaves through the same view semantics the retired manifest used: a
    // partially-resolved optional sibling (one that does not fully exist) yields null and is
    // skipped; getResolvedPath prepends the reference catalog but the leaf is unchanged.
    PolarisResolutionManifestCatalogView view =
        new EntityResolverManifestView(entityResolver, principal, catalogName, resolution);
    List<PolarisEntity> result = new ArrayList<>(keys.size());
    keys.forEach(
        k -> {
          PolarisResolvedPathWrapper path = view.getResolvedPath(k);
          if (path != null) {
            PolarisEntity entity = path.getRawLeafEntity();
            if (entity != null) {
              result.add(entity);
            }
          }
        });

    return result;
  }

  private PolarisCallContext getCurrentPolarisContext() {
    return callContext.getPolarisCallContext();
  }

  private DurableManager getMetaStoreManager() {
    return metaStoreManager;
  }

  // ===============================================================================================
  // Issue 29: merged Iceberg catalog feature-SPI implementation, generic over E (the OSS default
  // concrete subclass instantiates E = NoExtension; see Rework S Stage S3b).
  //
  // The public REST operations below are transcribed from the retired IcebergCatalogHandler.
  // Authorization is composed via the CatalogAuthorizer helper (never a base class); the local data
  // mechanics are the composed polarisIcebergCatalog delegate's Iceberg machinery (baseCatalog ==
  // polarisIcebergCatalog, per Rework R4 — this class no longer extends the Iceberg SDK's own
  // Catalog/ViewCatalog/SupportsNamespaces directly), and the federated path forwards to a narrow
  // remote delegate. These overrides are wired directly into IcebergCatalogAdapter (since Inc6);
  // the
  // legacy view-taking construction path never reaches them.
  // ===============================================================================================

  private static final Set<Endpoint> DEFAULT_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(Endpoint.V1_LIST_NAMESPACES)
          .add(Endpoint.V1_LOAD_NAMESPACE)
          .add(Endpoint.V1_NAMESPACE_EXISTS)
          .add(Endpoint.V1_CREATE_NAMESPACE)
          .add(Endpoint.V1_UPDATE_NAMESPACE)
          .add(Endpoint.V1_DELETE_NAMESPACE)
          .add(Endpoint.V1_LIST_TABLES)
          .add(Endpoint.V1_LOAD_TABLE)
          .add(Endpoint.V1_TABLE_EXISTS)
          .add(Endpoint.V1_CREATE_TABLE)
          .add(Endpoint.V1_UPDATE_TABLE)
          .add(Endpoint.V1_DELETE_TABLE)
          .add(Endpoint.V1_RENAME_TABLE)
          .add(Endpoint.V1_REGISTER_TABLE)
          .add(Endpoint.V1_REPORT_METRICS)
          .add(Endpoint.V1_COMMIT_TRANSACTION)
          .build();

  private static final Set<Endpoint> VIEW_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(Endpoint.V1_LIST_VIEWS)
          .add(Endpoint.V1_LOAD_VIEW)
          .add(Endpoint.V1_VIEW_EXISTS)
          .add(Endpoint.V1_CREATE_VIEW)
          .add(Endpoint.V1_UPDATE_VIEW)
          .add(Endpoint.V1_DELETE_VIEW)
          .add(Endpoint.V1_RENAME_VIEW)
          .add(Endpoint.V1_REGISTER_VIEW)
          .build();

  /**
   * Completes construction after authorization: reads the resolved-entity view populated by {@link
   * #authz} and establishes local-vs-federated dispatch state. Idempotent (guarded on {@link
   * #baseInitialized}). Moved (originally verbatim, since Rework R4 adapted to build the composed
   * {@link #polarisIcebergCatalog} delegate) from the retired {@code
   * IcebergCatalogHandler.initializeCatalog()}, folding in {@code
   * PolarisLocalCatalogFactory.createCatalog}'s local-initialize step (constructs and initializes
   * the {@link #polarisIcebergCatalog} delegate instead of building a separate catalog instance).
   */
  protected void ensureBaseInitialized() {
    if (baseInitialized) {
      return;
    }
    this.resolvedEntityView = authz.resolvedEntityView();
    CatalogEntity resolvedCatalogEntity = resolvedEntityView.getResolvedCatalogEntity();
    diagnostics.checkNotNull(resolvedCatalogEntity, "No catalog available");
    this.catalogEntity = resolvedCatalogEntity;
    this.catalogId = resolvedCatalogEntity.getId();

    ConnectionConfigInfoDpo connectionConfigInfoDpo =
        resolvedCatalogEntity.getConnectionConfigInfoDpo();
    if (connectionConfigInfoDpo != null) {
      LOGGER
          .atInfo()
          .addKeyValue("remoteUrl", connectionConfigInfoDpo.getUri())
          .log("Initializing federated catalog");
      FeatureConfiguration.enforceFeatureEnabledOrThrow(
          realmConfig, FeatureConfiguration.ENABLE_CATALOG_FEDERATION);

      ConnectionType connectionType =
          ConnectionType.fromCode(connectionConfigInfoDpo.getConnectionTypeCode());
      Instance<FederatedCatalogFactory> federatedCatalogFactory =
          federatedCatalogFactories.select(
              Identifier.Literal.of(connectionType.getFactoryIdentifier()));
      if (federatedCatalogFactory.isResolvable()) {
        Map<String, String> federatedProperties = resolvedCatalogEntity.getPropertiesAsMap();
        this.federatedDelegate =
            federatedCatalogFactory
                .get()
                .createCatalog(connectionConfigInfoDpo, credentialManager, federatedProperties);
      } else {
        throw new UnsupportedOperationException(
            "External catalog factory for type '" + connectionType + "' is unavailable.");
      }
      this.isFederated = true;
      this.baseCatalog = federatedDelegate;
      this.namespaceCatalog =
          (federatedDelegate instanceof SupportsNamespaces)
              ? (SupportsNamespaces) federatedDelegate
              : null;
      this.viewCatalog =
          (federatedDelegate instanceof ViewCatalog) ? (ViewCatalog) federatedDelegate : null;
    } else {
      LOGGER.debug("Initializing non-federated catalog");
      this.isFederated = false;
      Map<String, String> localCatalogProperties =
          new HashMap<>(resolvedCatalogEntity.getPropertiesAsMap());
      String warehouseLocation = resolvedCatalogEntity.getBaseLocation();
      if (warehouseLocation == null) {
        throw new IllegalStateException(
            String.format(
                "Catalog '%s' does not have a configured warehouse location. "
                    + "Please configure a default base location for this catalog.",
                catalogName));
      }
      localCatalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
      this.polarisIcebergCatalog =
          createBridgeBaseMetastoreViewCatalog(
              diagnostics,
              entityResolver,
              metaStoreManager,
              callContext,
              resolvedEntityView,
              principal,
              taskExecutor,
              storageAccessConfigProvider,
              storageIoProvider,
              polarisEventDispatcher,
              eventMetadataFactory);
      this.polarisIcebergCatalog.initialize(
          catalogName, finalizeLocalCatalogProperties(localCatalogProperties));
      this.baseCatalog = this.polarisIcebergCatalog;
      this.namespaceCatalog = this.polarisIcebergCatalog;
      this.viewCatalog = this.polarisIcebergCatalog;
    }
    this.baseInitialized = true;
  }

  /**
   * Constructs the composed {@link BridgeBaseMetastoreViewCatalog} delegate for the non-federated
   * (local) case. A provider whose Iceberg-SDK-mechanics need customizing (e.g. a different {@code
   * defaultWarehouseLocation}, or wrapped {@code TableOperations}/{@code ViewOperations} via {@code
   * newTableOps}/{@code newViewOps}) overrides this to return a {@code
   * BridgeBaseMetastoreViewCatalog} subclass instead of the plain OSS default — those SDK-mechanics
   * methods can no longer be overridden by subclassing {@code BasePolarisIcebergCatalog} itself
   * since Issue 29 Rework R4 (this class no longer extends the Iceberg SDK types {@code
   * BridgeBaseMetastoreViewCatalog} implements).
   */
  protected BridgeBaseMetastoreViewCatalog createBridgeBaseMetastoreViewCatalog(
      PolarisDiagnostics diagnostics,
      EntityResolver entityResolver,
      DurableManager metaStoreManager,
      CallContext callContext,
      PolarisResolutionManifestCatalogView resolvedEntityView,
      PolarisPrincipal principal,
      TaskExecutor taskExecutor,
      StorageAccessConfigProvider storageAccessConfigProvider,
      StorageIoProvider storageIoProvider,
      PolarisEventDispatcher polarisEventDispatcher,
      PolarisEventMetadataFactory eventMetadataFactory) {
    return new BridgeBaseMetastoreViewCatalog(
        diagnostics,
        entityResolver,
        metaStoreManager,
        callContext,
        resolvedEntityView,
        principal,
        taskExecutor,
        storageAccessConfigProvider,
        storageIoProvider,
        polarisEventDispatcher,
        eventMetadataFactory);
  }

  /**
   * Hook for tests to mutate the local catalog's bootstrap properties before {@link
   * BridgeBaseMetastoreViewCatalog#initialize} runs (e.g. to force an in-memory FileIO) — this
   * catalog no longer overrides {@code initialize} directly since Issue 29 Rework R4 moved it to
   * the composed {@link #polarisIcebergCatalog} delegate.
   */
  protected Map<String, String> finalizeLocalCatalogProperties(Map<String, String> properties) {
    return properties;
  }

  protected CatalogEntity getResolvedCatalogEntity() {
    // Read from the authorizer's resolved view (populated by the authorize/resolve call), matching
    // the retired handler's getResolvedCatalogEntity(). Equivalent to the catalogEntity field once
    // ensureBaseInitialized() has run, but also valid between a resolve and ensureBaseInitialized()
    // (used by updateTable, which reads catalog config before initializing the catalog instance).
    CatalogEntity resolved = authz.resolvedEntityView().getResolvedCatalogEntity();
    diagnostics.checkNotNull(resolved, "No catalog available");
    return resolved;
  }

  private boolean shouldDecodeToken() {
    return realmConfig.getConfig(
        FeatureConfiguration.LIST_PAGINATION_ENABLED, getResolvedCatalogEntity());
  }

  @Override
  public PolarisResult<ListNamespacesResponse, E> listNamespaces(
      Namespace parent, String pageToken, Integer pageSize) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_NAMESPACES;
    authz.authorizeBasicNamespaceOperationOrThrow(op, parent);
    ensureBaseInitialized();

    ListNamespacesResponse response;
    if (isFederated) {
      response = catalogHandlerUtils.listNamespaces(namespaceCatalog, parent, pageToken, pageSize);
    } else {
      PageToken pageRequest = PageToken.build(pageToken, pageSize, this::shouldDecodeToken);
      var results = this.polarisIcebergCatalog.listNamespaces(parent, pageRequest);
      response =
          ListNamespacesResponse.builder()
              .addAll(results.items())
              .nextPageToken(results.encodedResponseToken())
              .build();
    }
    return new PolarisResult<>(response, this.extensionValue);
  }

  @Override
  public PolarisResult<CreateNamespaceResponse, E> createNamespace(CreateNamespaceRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_NAMESPACE;

    Namespace namespace = request.namespace();
    if (namespace.isEmpty()) {
      throw new AlreadyExistsException(
          "Cannot create root namespace, as it already exists implicitly.");
    }
    authz.authorizeCreateNamespaceUnderNamespaceOperationOrThrow(op, namespace);
    ensureBaseInitialized();

    CreateNamespaceResponse response;
    if (isFederated) {
      response = catalogHandlerUtils.createNamespace(namespaceCatalog, request);
    } else {
      // Note: The CatalogHandlers' default implementation will non-atomically create the
      // namespace and then fetch its properties using loadNamespaceMetadata for the response.
      // However, the latest namespace metadata technically isn't the same authorized instance,
      // so we don't want all cals to loadNamespaceMetadata to automatically use the manifest
      // in "passthrough" mode.
      //
      // For CreateNamespace, we consider this a special case in that the creator is able to
      // retrieve the latest namespace metadata for the duration of the CreateNamespace
      // operation, even if the entityVersion and/or grantsVersion update in the interim.
      namespaceCatalog.createNamespace(
          namespace, reservedProperties.removeReservedProperties(request.properties()));
      Map<String, String> filteredProperties =
          reservedProperties.removeReservedProperties(
              resolvedEntityView
                  .getPassthroughResolvedPath(ResolvedPathKey.ofNamespace(namespace))
                  .getRawLeafEntity()
                  .getPropertiesAsMap());
      response =
          CreateNamespaceResponse.builder()
              .withNamespace(namespace)
              .setProperties(filteredProperties)
              .build();
    }
    return new PolarisResult<>(response, this.extensionValue);
  }

  @Override
  public PolarisResult<GetNamespaceResponse, E> loadNamespaceMetadata(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_NAMESPACE_METADATA;
    authz.authorizeBasicNamespaceOperationOrThrow(op, namespace);
    ensureBaseInitialized();

    return new PolarisResult<>(
        catalogHandlerUtils.loadNamespace(namespaceCatalog, namespace), this.extensionValue);
  }

  @Override
  public PolarisResult<Void, E> namespaceExists(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.NAMESPACE_EXISTS;

    // TODO: This authz check doesn't accomplish true authz in terms of blocking the ability
    // for a caller to ascertain whether the namespace exists or not, but instead just behaves
    // according to convention -- if existence is going to be privileged, we must instead
    // add a base layer that throws NotFound exceptions instead of NotAuthorizedException
    // for *all* operations in which we determine that the basic privilege for determining
    // existence is also missing.
    authz.authorizeBasicNamespaceOperationOrThrow(op, namespace);
    ensureBaseInitialized();

    // TODO: Just skip CatalogHandlers for this one maybe
    catalogHandlerUtils.loadNamespace(namespaceCatalog, namespace);
    return new PolarisResult<>(null, this.extensionValue);
  }

  @Override
  public PolarisResult<Void, E> dropNamespace(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_NAMESPACE;
    authz.authorizeBasicNamespaceOperationOrThrow(op, namespace);
    ensureBaseInitialized();

    catalogHandlerUtils.dropNamespace(namespaceCatalog, namespace);
    return new PolarisResult<>(null, this.extensionValue);
  }

  @Override
  public PolarisResult<UpdateNamespacePropertiesResponse, E> updateNamespaceProperties(
      Namespace namespace, UpdateNamespacePropertiesRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_NAMESPACE_PROPERTIES;
    authz.authorizeBasicNamespaceOperationOrThrow(op, namespace);
    ensureBaseInitialized();

    return new PolarisResult<>(
        catalogHandlerUtils.updateNamespaceProperties(namespaceCatalog, namespace, request),
        this.extensionValue);
  }

  @Override
  public PolarisResult<ConfigResponse, E> getConfig() {
    // Resolve the reference catalog through the EntityResolver SPI (ADR-0008). This carries no
    // paths or top-level names, so it resolves the caller principal, activated roles, and the
    // reference catalog, matching the prior raw-resolver resolveAll() with nothing added. A
    // provider's EntityResolver (e.g. managed's cache-populating wrapper) runs here as it does on
    // the authorize path, so any request-scoped resolution side effects are preserved. No authz and
    // no ensureBaseInitialized(): getConfig never resolves a catalog instance.
    ResolutionResult resolution =
        entityResolver.resolve(ResolutionRequest.of(principal, catalogName));
    if (!resolution.isSuccess()) {
      throw new NotFoundException("Unable to find warehouse %s", catalogName);
    }
    ResolvedPolarisEntity resolvedReferenceCatalog = resolution.resolvedReferenceCatalog();
    Map<String, String> properties =
        PolarisEntity.of(resolvedReferenceCatalog.getEntity()).getPropertiesAsMap();

    ConfigResponse response =
        ConfigResponse.builder()
            .withDefaults(properties) // catalog properties are defaults
            .withOverrides(
                ImmutableMap.of(
                    "prefix",
                    prefixParser.catalogNameToPrefix(catalogName),
                    // Polaris does not handle custom namespace separators;
                    // always communicate the default namespace separator to clients.
                    RESTCatalogProperties.NAMESPACE_SEPARATOR,
                    NamespaceUtils.DEFAULT_NAMESPACE_SEPARATOR_ENCODED))
            .withEndpoints(
                ImmutableList.<Endpoint>builder()
                    .addAll(DEFAULT_ENDPOINTS)
                    .addAll(VIEW_ENDPOINTS)
                    .addAll(PolarisEndpoints.getSupportedGenericTableEndpoints(realmConfig))
                    .addAll(PolarisEndpoints.getSupportedPolicyEndpoints(realmConfig))
                    .build())
            .build();
    return new PolarisResult<>(response, this.extensionValue);
  }

  // ---- Issue 29 Inc 4c: table ops (transcribed from IcebergCatalogHandler). ----

  private static final String SNAPSHOTS_ALL = "all";
  private static final String SNAPSHOTS_REFS = "refs";

  @Override
  public PolarisResult<ListTablesResponse, E> listTables(
      Namespace namespace, String pageToken, Integer pageSize) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_TABLES;
    authz.authorizeBasicNamespaceOperationOrThrow(op, namespace);
    ensureBaseInitialized();

    ListTablesResponse response;
    if (isFederated) {
      response = catalogHandlerUtils.listTables(baseCatalog, namespace, pageToken, pageSize);
    } else {
      PageToken pageRequest = PageToken.build(pageToken, pageSize, this::shouldDecodeToken);
      var results = this.polarisIcebergCatalog.listTables(namespace, pageRequest);
      response =
          ListTablesResponse.builder()
              .addAll(results.items())
              .nextPageToken(results.encodedResponseToken())
              .build();
    }
    return new PolarisResult<>(response, this.extensionValue);
  }

  @Override
  public PolarisResult<LoadTableResponse, E> createTableDirect(
      Namespace namespace,
      CreateTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint) {
    authorizeCreateTableDirect(namespace, request, !delegationModes.isEmpty());
    Optional<AccessDelegationMode> resolvedMode = resolveAccessDelegationModes(delegationModes);

    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, request.name());
    if (baseCatalog.tableExists(tableIdentifier)) {
      throw alreadyExistsExceptionForTableLikeEntity(
          tableIdentifier, PolarisEntitySubType.ICEBERG_TABLE);
    }

    Map<String, String> properties = Maps.newHashMap();
    properties.put("created-at", OffsetDateTime.now(ZoneOffset.UTC).toString());
    properties.putAll(reservedProperties.removeReservedProperties(request.properties()));

    Table table =
        baseCatalog
            .buildTable(tableIdentifier, request.schema())
            .withLocation(request.location())
            .withPartitionSpec(request.spec())
            .withSortOrder(request.writeOrder())
            .withProperties(properties)
            .create();

    if (table instanceof BaseTable baseTable) {
      TableMetadata tableMetadata = baseTable.operations().current();
      LoadTableResponse response =
          buildLoadTableResponseWithDelegationCredentials(
                  tableIdentifier,
                  tableMetadata,
                  resolvedMode,
                  Set.of(
                      PolarisStorageActions.READ,
                      PolarisStorageActions.WRITE,
                      PolarisStorageActions.LIST),
                  refreshCredentialsEndpoint)
              .build();
      return new PolarisResult<>(
          response, withEtag(etagForCreatedTable(tableIdentifier, response)));
    } else if (table instanceof BaseMetadataTable) {
      // metadata tables are loaded on the client side, return NoSuchTableException for now
      throw notFoundExceptionForTableLikeEntity(
          tableIdentifier, PolarisEntitySubType.ICEBERG_TABLE);
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }

  private void authorizeCreateTableDirect(
      Namespace namespace, CreateTableRequest request, boolean delegationRequested) {
    if (delegationRequested) {
      authz.authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
          PolarisAuthorizableOperation.CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION,
          TableIdentifier.of(namespace, request.name()));
    } else {
      TableIdentifier identifier = TableIdentifier.of(namespace, request.name());
      authz.authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
          PolarisAuthorizableOperation.CREATE_TABLE_DIRECT, identifier);
    }
    ensureBaseInitialized();

    CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
    if (resolvedCatalog.isStaticFacade()) {
      throw new BadRequestException("Cannot create table on static-facade external catalogs.");
    }
  }

  @Override
  public PolarisResult<LoadTableResponse, E> createTableStaged(
      Namespace namespace,
      CreateTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint) {
    authorizeCreateTableStaged(namespace, request, !delegationModes.isEmpty());

    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    TableMetadata metadata = stageTableCreateHelper(namespace, request);

    if (!isFederated) {
      validateStagedTableCreate(ident, metadata);
    }

    Optional<AccessDelegationMode> resolvedMode = resolveAccessDelegationModes(delegationModes);

    LoadTableResponse response =
        buildLoadTableResponseWithDelegationCredentials(
                ident,
                metadata,
                resolvedMode,
                Set.of(PolarisStorageActions.ALL),
                refreshCredentialsEndpoint)
            .build();
    return new PolarisResult<>(response, this.extensionValue);
  }

  private void authorizeCreateTableStaged(
      Namespace namespace, CreateTableRequest request, boolean delegationRequested) {
    if (delegationRequested) {
      authz.authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
          PolarisAuthorizableOperation.CREATE_TABLE_STAGED_WITH_WRITE_DELEGATION,
          TableIdentifier.of(namespace, request.name()));
    } else {
      authz.authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
          PolarisAuthorizableOperation.CREATE_TABLE_STAGED,
          TableIdentifier.of(namespace, request.name()));
    }
    ensureBaseInitialized();

    CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
    if (resolvedCatalog.isStaticFacade()) {
      throw new BadRequestException("Cannot create table on static-facade external catalogs.");
    }
  }

  private TableMetadata stageTableCreateHelper(Namespace namespace, CreateTableRequest request) {
    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    if (baseCatalog.tableExists(ident)) {
      throw alreadyExistsExceptionForTableLikeEntity(ident, PolarisEntitySubType.ICEBERG_TABLE);
    }

    Map<String, String> properties = Maps.newHashMap();
    properties.put("created-at", OffsetDateTime.now(ZoneOffset.UTC).toString());
    properties.putAll(reservedProperties.removeReservedProperties(request.properties()));

    String location;
    if (request.location() != null) {
      // Even if the request provides a location, run it through the catalog's TableBuilder
      // to inherit any override behaviors if applicable.
      if (isFederated) {
        location = request.location();
      } else {
        location = transformTableLikeLocation(ident, request.location());
      }
    } else {
      location =
          baseCatalog
              .buildTable(ident, request.schema())
              .withPartitionSpec(request.spec())
              .withSortOrder(request.writeOrder())
              .withProperties(properties)
              .createTransaction()
              .table()
              .location();
    }

    return TableMetadata.newTableMetadata(
        request.schema(),
        request.spec() != null ? request.spec() : PartitionSpec.unpartitioned(),
        request.writeOrder() != null ? request.writeOrder() : SortOrder.unsorted(),
        location,
        properties);
  }

  @Override
  public PolarisResult<LoadTableResponse, E> registerTable(
      Namespace namespace,
      RegisterTableRequest request,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint) {

    request.validate();
    TableIdentifier identifier = TableIdentifier.of(namespace, request.name());
    boolean overwrite = request.overwrite();

    Set<PolarisStorageActions> actionsRequested =
        authorizeRegisterTable(identifier, delegationModes, overwrite);
    ensureBaseInitialized();

    if (overwrite) {
      // For non-Polaris/federated catalogs, reject overwrite until this is
      // supported by a common catalog contract.
      CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
      if (resolvedCatalog.isExternal()) {
        throw new BadRequestException(
            "Register table overwrite is only supported for internal Polaris catalogs");
      }
    }

    // Resolve the mode before registering the table to avoid registering a table and then failing
    // to return credentials if the mode is invalid
    Optional<AccessDelegationMode> resolvedMode = resolveAccessDelegationModes(delegationModes);

    Table table = baseCatalog.registerTable(identifier, request.metadataLocation(), overwrite);

    if (table instanceof BaseTable baseTable) {
      TableMetadata tableMetadata = baseTable.operations().current();
      LoadTableResponse response =
          buildLoadTableResponseWithDelegationCredentials(
                  identifier,
                  tableMetadata,
                  resolvedMode,
                  actionsRequested,
                  refreshCredentialsEndpoint)
              .build();
      return new PolarisResult<>(response, withEtag(etagForCreatedTable(identifier, response)));
    }

    throw new IllegalStateException(
        "Cannot register table %s: unknown table format".formatted(identifier));
  }

  private Set<PolarisStorageActions> authorizeRegisterTable(
      TableIdentifier tableIdentifier,
      EnumSet<AccessDelegationMode> delegationModes,
      boolean overwrite) {

    if (delegationModes.isEmpty()) {

      if (overwrite) {
        authz.authorizeRegisterTableOverwriteOrThrow(
            PolarisAuthorizableOperation.REGISTER_TABLE_OVERWRITE,
            PolarisAuthorizableOperation.REGISTER_TABLE,
            tableIdentifier);
      } else {
        authz.authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
            PolarisAuthorizableOperation.REGISTER_TABLE, tableIdentifier);
      }

      return Set.of();

    } else {

      Set<PolarisStorageActions> actionsRequested =
          EnumSet.of(PolarisStorageActions.READ, PolarisStorageActions.LIST);

      try {
        if (overwrite) {
          authz.authorizeRegisterTableOverwriteOrThrow(
              PolarisAuthorizableOperation.REGISTER_TABLE_OVERWRITE_WITH_WRITE_DELEGATION,
              PolarisAuthorizableOperation.REGISTER_TABLE_WITH_WRITE_DELEGATION,
              tableIdentifier);
        } else {
          authz.authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
              PolarisAuthorizableOperation.REGISTER_TABLE_WITH_WRITE_DELEGATION, tableIdentifier);
        }
        actionsRequested.add(PolarisStorageActions.WRITE);
      } catch (ForbiddenException e) {
        if (overwrite) {
          authz.authorizeRegisterTableOverwriteOrThrow(
              PolarisAuthorizableOperation.REGISTER_TABLE_OVERWRITE_WITH_READ_DELEGATION,
              PolarisAuthorizableOperation.REGISTER_TABLE_WITH_READ_DELEGATION,
              tableIdentifier);
        } else {
          authz.authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
              PolarisAuthorizableOperation.REGISTER_TABLE_WITH_READ_DELEGATION, tableIdentifier);
        }
      }

      return actionsRequested;
    }
  }

  @Override
  public PolarisResult<LoadTableResponse, E> loadTable(
      TableIdentifier tableIdentifier,
      String snapshots,
      IfNoneMatch ifNoneMatch,
      EnumSet<AccessDelegationMode> delegationModes,
      Optional<String> refreshCredentialsEndpoint) {

    Set<PolarisStorageActions> actionsRequested =
        authorizeLoadTable(tableIdentifier, !delegationModes.isEmpty());
    ensureBaseInitialized();
    Optional<AccessDelegationMode> resolvedMode = resolveAccessDelegationModes(delegationModes);

    if (ifNoneMatch != null) {
      // Perform freshness-aware table loading if caller specified ifNoneMatch.
      IcebergTableLikeEntity tableEntity = getTableEntity(tableIdentifier);
      if (tableEntity == null || tableEntity.getMetadataLocation() == null) {
        LOGGER
            .atWarn()
            .addKeyValue("tableIdentifier", tableIdentifier)
            .addKeyValue("tableEntity", tableEntity)
            .log("Failed to getMetadataLocation to generate ETag when loading table");
      } else {
        // TODO: Refactor null-checking into the helper method once we create a more canonical
        // interface for associate etags with entities.
        String tableETag =
            IcebergHttpUtil.generateETagForMetadataFileLocation(tableEntity.getMetadataLocation());
        if (ifNoneMatch.anyMatch(tableETag)) {
          return new PolarisResult<>(null, withEtag(Optional.of(tableETag)));
        }
      }
    }

    // TODO: Find a way for the configuration or caller to better express whether to fail or omit
    // when data-access is specified but access delegation grants are not found.
    Table table = baseCatalog.loadTable(tableIdentifier);

    if (table instanceof BaseTable baseTable) {
      TableMetadata tableMetadata = baseTable.operations().current();
      LoadTableResponse response =
          buildLoadTableResponseWithDelegationCredentials(
                  tableIdentifier,
                  tableMetadata,
                  resolvedMode,
                  actionsRequested,
                  refreshCredentialsEndpoint)
              .build();
      LoadTableResponse filteredResponse = filterResponseToSnapshots(response, snapshots);

      // Derive the ETag from the POST-load response actually being returned -- this is the same
      // input IcebergCatalogAdapter.tryInsertETagHeader used before this migration
      // (response.metadataLocation()), deliberately NOT the pre-load entity value above (that one
      // backs only the staleness comparison / the NotModified case; the two can legitimately
      // diverge under concurrent metadata updates).
      Optional<String> loadedEtag;
      if (filteredResponse.metadataLocation() != null) {
        loadedEtag =
            Optional.of(
                IcebergHttpUtil.generateETagForMetadataFileLocation(
                    filteredResponse.metadataLocation()));
      } else {
        LOGGER
            .atWarn()
            .addKeyValue("tableIdentifier", tableIdentifier)
            .log("Response has null metadataLocation; omitting etag");
        loadedEtag = Optional.empty();
      }
      return new PolarisResult<>(filteredResponse, withEtag(loadedEtag));
    } else if (table instanceof BaseMetadataTable) {
      // metadata tables are loaded on the client side, return NoSuchTableException for now
      throw notFoundExceptionForTableLikeEntity(
          tableIdentifier, PolarisEntitySubType.ICEBERG_TABLE);
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }

  protected Set<PolarisStorageActions> authorizeLoadTable(
      TableIdentifier tableIdentifier, boolean delegationRequested) {
    if (!delegationRequested) {
      authz.authorizeBasicTableLikeOperationOrThrow(
          PolarisAuthorizableOperation.LOAD_TABLE,
          PolarisEntitySubType.ICEBERG_TABLE,
          tableIdentifier);
      return Set.of();
    }

    // Here we have a single method that falls through multiple candidate
    // PolarisAuthorizableOperations because instead of identifying the desired operation up-front
    // and
    // failing the authz check if grants aren't found, we find the first most-privileged authz match
    // and respond according to that.
    PolarisAuthorizableOperation read =
        PolarisAuthorizableOperation.LOAD_TABLE_WITH_READ_DELEGATION;
    PolarisAuthorizableOperation write =
        PolarisAuthorizableOperation.LOAD_TABLE_WITH_WRITE_DELEGATION;

    Set<PolarisStorageActions> actionsRequested =
        new HashSet<>(Set.of(PolarisStorageActions.READ, PolarisStorageActions.LIST));
    // Probe for write delegation without exception-driven control flow: branch on the decision-
    // native authorizer instead of try/catch(ForbiddenException). If write is not granted, fall
    // back to requiring read (which still throws 403 when read is also denied), preserving the
    // most-privileged-match behavior.
    if (authz
        .authorizeBasicTableLikeOperation(
            write, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier)
        .isAllowed()) {
      actionsRequested.add(PolarisStorageActions.WRITE);
    } else {
      authz.authorizeBasicTableLikeOperationOrThrow(
          read, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);
    }

    return actionsRequested;
  }

  @Override
  public PolarisResult<LoadTableResponse, E> updateTable(
      TableIdentifier tableIdentifier, UpdateTableRequest request) {

    // Ensure resolution manifest is initialized so we can determine whether
    // fine grained authz model is enabled at the catalog level
    authz.ensureResolutionManifestForTable(tableIdentifier);

    EnumSet<PolarisAuthorizableOperation> authorizableOperations =
        getUpdateTableAuthorizableOperations(request);

    authz.authorizeBasicTableLikeOperationsOrThrow(
        authorizableOperations, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);
    ensureBaseInitialized();

    CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
    if (resolvedCatalog.isStaticFacade()) {
      throw new BadRequestException("Cannot update table on static-facade external catalogs.");
    }
    return new PolarisResult<>(
        catalogHandlerUtils.updateTable(baseCatalog, tableIdentifier, applyUpdateFilters(request)),
        this.extensionValue);
  }

  @Override
  public PolarisResult<LoadTableResponse, E> updateTableForStagedCreate(
      TableIdentifier tableIdentifier, UpdateTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_TABLE_FOR_STAGED_CREATE;
    authz.authorizeCreateTableLikeUnderNamespaceOperationOrThrow(op, tableIdentifier);
    ensureBaseInitialized();

    CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
    if (resolvedCatalog.isStaticFacade()) {
      throw new BadRequestException("Cannot update table on static-facade external catalogs.");
    }
    return new PolarisResult<>(
        catalogHandlerUtils.updateTable(baseCatalog, tableIdentifier, applyUpdateFilters(request)),
        this.extensionValue);
  }

  private UpdateTableRequest applyUpdateFilters(UpdateTableRequest request) {
    // Certain MetadataUpdates need to be explicitly transformed to achieve the same behavior
    // as using a local Catalog client via TableBuilder.
    TableIdentifier identifier = request.identifier();
    List<UpdateRequirement> requirements = request.requirements();
    List<MetadataUpdate> updates =
        request.updates().stream()
            .map(
                update -> {
                  if (!isFederated && update instanceof MetadataUpdate.SetLocation setLocation) {
                    String requestedLocation = setLocation.location();
                    String filteredLocation =
                        transformTableLikeLocation(identifier, requestedLocation);
                    return new MetadataUpdate.SetLocation(filteredLocation);
                  } else {
                    return update;
                  }
                })
            .toList();
    return UpdateTableRequest.create(identifier, requirements, updates);
  }

  private EnumSet<PolarisAuthorizableOperation> getUpdateTableAuthorizableOperations(
      UpdateTableRequest request) {
    boolean useFineGrainedOperations =
        realmConfig.getConfig(
            FeatureConfiguration.ENABLE_FINE_GRAINED_UPDATE_TABLE_PRIVILEGES,
            getResolvedCatalogEntity());

    if (useFineGrainedOperations) {
      EnumSet<PolarisAuthorizableOperation> actions =
          request.updates().stream()
              .map(
                  update ->
                      switch (update) {
                        case MetadataUpdate.AssignUUID assignUuid ->
                            PolarisAuthorizableOperation.ASSIGN_TABLE_UUID;
                        case MetadataUpdate.UpgradeFormatVersion upgradeFormat ->
                            PolarisAuthorizableOperation.UPGRADE_TABLE_FORMAT_VERSION;
                        case MetadataUpdate.AddSchema addSchema ->
                            PolarisAuthorizableOperation.ADD_TABLE_SCHEMA;
                        case MetadataUpdate.SetCurrentSchema setCurrentSchema ->
                            PolarisAuthorizableOperation.SET_TABLE_CURRENT_SCHEMA;
                        case MetadataUpdate.AddPartitionSpec addPartitionSpec ->
                            PolarisAuthorizableOperation.ADD_TABLE_PARTITION_SPEC;
                        case MetadataUpdate.AddSortOrder addSortOrder ->
                            PolarisAuthorizableOperation.ADD_TABLE_SORT_ORDER;
                        case MetadataUpdate.SetDefaultSortOrder setDefaultSortOrder ->
                            PolarisAuthorizableOperation.SET_TABLE_DEFAULT_SORT_ORDER;
                        case MetadataUpdate.AddSnapshot addSnapshot ->
                            PolarisAuthorizableOperation.ADD_TABLE_SNAPSHOT;
                        case MetadataUpdate.SetSnapshotRef setSnapshotRef ->
                            PolarisAuthorizableOperation.SET_TABLE_SNAPSHOT_REF;
                        case MetadataUpdate.RemoveSnapshots removeSnapshots ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_SNAPSHOTS;
                        case MetadataUpdate.RemoveSnapshotRef removeSnapshotRef ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_SNAPSHOT_REF;
                        case MetadataUpdate.SetLocation setLocation ->
                            PolarisAuthorizableOperation.SET_TABLE_LOCATION;
                        case MetadataUpdate.SetProperties setProperties ->
                            PolarisAuthorizableOperation.SET_TABLE_PROPERTIES;
                        case MetadataUpdate.RemoveProperties removeProperties ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_PROPERTIES;
                        case MetadataUpdate.SetStatistics setStatistics ->
                            PolarisAuthorizableOperation.SET_TABLE_STATISTICS;
                        case MetadataUpdate.RemoveStatistics removeStatistics ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_STATISTICS;
                        case MetadataUpdate.RemovePartitionSpecs removePartitionSpecs ->
                            PolarisAuthorizableOperation.REMOVE_TABLE_PARTITION_SPECS;
                        default ->
                            PolarisAuthorizableOperation
                                .UPDATE_TABLE; // Fallback for unknown update types
                      })
              .collect(
                  () -> EnumSet.noneOf(PolarisAuthorizableOperation.class),
                  EnumSet::add,
                  EnumSet::addAll);

      // If there are no MetadataUpdates, then default to the UPDATE_TABLE operation.
      if (actions.isEmpty()) {
        actions.add(PolarisAuthorizableOperation.UPDATE_TABLE);
      }

      return actions;
    } else {
      return EnumSet.of(PolarisAuthorizableOperation.UPDATE_TABLE);
    }
  }

  @Override
  public PolarisResult<Void, E> dropTableWithoutPurge(TableIdentifier tableIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_TABLE_WITHOUT_PURGE;
    authz.authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);
    ensureBaseInitialized();

    catalogHandlerUtils.dropTable(baseCatalog, tableIdentifier);
    return new PolarisResult<>(null, this.extensionValue);
  }

  @Override
  public PolarisResult<Void, E> dropTableWithPurge(TableIdentifier tableIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_TABLE_WITH_PURGE;
    authz.authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);
    ensureBaseInitialized();

    CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
    if (resolvedCatalog.isStaticFacade()) {
      throw new BadRequestException("Cannot drop table on static-facade external catalogs.");
    }
    catalogHandlerUtils.purgeTable(baseCatalog, tableIdentifier);
    return new PolarisResult<>(null, this.extensionValue);
  }

  @Override
  public PolarisResult<Void, E> tableExists(TableIdentifier tableIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.TABLE_EXISTS;
    authz.authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);
    ensureBaseInitialized();

    // TODO: Just skip CatalogHandlers for this one maybe
    catalogHandlerUtils.loadTable(baseCatalog, tableIdentifier);
    return new PolarisResult<>(null, this.extensionValue);
  }

  @Override
  public PolarisResult<Void, E> renameTable(RenameTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.RENAME_TABLE;
    authz.authorizeRenameTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_TABLE, request.source(), request.destination());
    ensureBaseInitialized();

    CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
    if (resolvedCatalog.isStaticFacade()) {
      throw new BadRequestException("Cannot rename table on static-facade external catalogs.");
    }
    catalogHandlerUtils.renameTable(baseCatalog, request);
    return new PolarisResult<>(null, this.extensionValue);
  }

  @Override
  public PolarisResult<Void, E> commitTransaction(
      CommitTransactionRequest commitTransactionRequest) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.COMMIT_TRANSACTION;
    // TODO: The authz actually needs to detect hidden updateForStagedCreate UpdateTableRequests
    // and have some kind of per-item conditional privilege requirement if we want to make it
    // so that only the stageCreate updates need TABLE_CREATE whereas everything else only
    // needs TABLE_WRITE_PROPERTIES.
    authz.authorizeCollectionOfTableLikeOperationOrThrow(
        op,
        PolarisEntitySubType.ICEBERG_TABLE,
        commitTransactionRequest.tableChanges().stream()
            .map(UpdateTableRequest::identifier)
            .toList());
    ensureBaseInitialized();
    CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
    if (resolvedCatalog.isStaticFacade()) {
      throw new BadRequestException("Cannot update table on static-facade external catalogs.");
    }

    if (isFederated) {
      throw new BadRequestException(
          "Unsupported operation: commitTransaction with baseCatalog type: %s",
          baseCatalog.getClass().getName());
    }

    // The retired handler kept its own (real) DurableManager separate from the catalog's swappable
    // one. This class's own metaStoreManager field captures the real manager here (never swapped);
    // the swap is applied to the composed polarisIcebergCatalog delegate instead, since that is the
    // instance whose data mechanics (tableOps.commit()/createTableLike()/etc, via its own
    // metaStoreManager field) actually need to route mutations into the in-memory transaction
    // workspace collection during this method. The collected updates are committed as a single
    // atomic unit after all validations, through the captured real manager.
    DurableManager realMetaStoreManager = metaStoreManager;
    TransactionWorkspaceMetaStoreManager transactionMetaStoreManager =
        new TransactionWorkspaceMetaStoreManager(diagnostics, realMetaStoreManager);
    polarisIcebergCatalog.setMetaStoreManager(transactionMetaStoreManager);

    // Group all changes by table identifier to handle them atomically.
    // This prevents conflicts when multiple changes target the same table entity.
    // LinkedHashMap preserves insertion order for deterministic processing.
    Map<TableIdentifier, List<UpdateTableRequest>> changesByTable = new LinkedHashMap<>();
    for (UpdateTableRequest change : commitTransactionRequest.tableChanges()) {
      if (CatalogHandlerUtils.isCreate(change)) {
        throw new BadRequestException(
            "Unsupported operation: commitTranaction with updateForStagedCreate: %s", change);
      }
      changesByTable.computeIfAbsent(change.identifier(), k -> new ArrayList<>()).add(change);
    }

    // Process each table's changes in order.
    // Note: All UpdateTableRequests for a given table are coalesced into a single metadata
    // update and a single tableOps.commit(), which results in one Polaris entity update per
    // table. This is subtly different from applying each UpdateTableRequest as an independent
    // commit (as if each were under a lock). Requirements are still validated sequentially
    // against the evolving metadata, so conflicts are detected correctly.
    // See also the TODO in TransactionWorkspaceMetaStoreManager for a more general (but more
    // complex) alternative that would intercept at the MetaStoreManager layer.
    List<TableMetadata> tableMetadataObjs = new ArrayList<>();
    changesByTable.forEach(
        (tableIdentifier, changes) -> {
          Table table = baseCatalog.loadTable(tableIdentifier);
          if (!(table instanceof BaseTable baseTable)) {
            throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
          }

          TableOperations tableOps = baseTable.operations();
          TableMetadata baseMetadata = tableOps.current();

          // Apply each change sequentially: validate requirements against current state,
          // then apply updates. This ensures conflicts are detected (e.g., if two changes
          // both expect schema ID 0, the second will fail after the first increments it).
          TableMetadata currentMetadata = baseMetadata;
          for (UpdateTableRequest change : changes) {
            // Validate requirements against the current metadata state
            final TableMetadata metadataForValidation = currentMetadata;
            change
                .requirements()
                .forEach(requirement -> requirement.validate(metadataForValidation));

            // TODO: Refactor to share/reconcile the update-application logic below with
            // CatalogHandlerUtils to avoid divergence as complexity grows.
            TableMetadata.Builder metadataBuilder = TableMetadata.buildFrom(currentMetadata);
            for (MetadataUpdate singleUpdate : change.updates()) {
              // Note: If location-overlap checking is refactored to be atomic, we could
              // support validation within a single multi-table transaction as well, but
              // will need to update the TransactionWorkspaceMetaStoreManager to better
              // expose the concept of being able to read uncommitted updates.
              if (singleUpdate instanceof MetadataUpdate.SetLocation setLocation) {
                if (!currentMetadata.location().equals(setLocation.location())
                    && !realmConfig.getConfig(
                        FeatureConfiguration.ALLOW_NAMESPACE_LOCATION_OVERLAP)) {
                  throw new BadRequestException(
                      "Unsupported operation: commitTransaction containing SetLocation"
                          + " for table '%s' and new location '%s'",
                      change.identifier(), ((MetadataUpdate.SetLocation) singleUpdate).location());
                }
              }

              // Apply updates to builder
              singleUpdate.applyTo(metadataBuilder);
            }

            // Update currentMetadata to reflect this change for subsequent requirement validation
            currentMetadata = metadataBuilder.build();
          }

          // Commit all accumulated changes for this table in a single atomic operation
          if (!currentMetadata.changes().isEmpty()) {
            tableOps.commit(baseMetadata, currentMetadata);
          }

          tableMetadataObjs.add(currentMetadata);
        });

    // Commit the collected updates in a single atomic operation
    List<EntityWithPath> pendingUpdates = transactionMetaStoreManager.getPendingUpdates();
    EntitiesResult result =
        realMetaStoreManager.updateEntitiesPropertiesIfNotChanged(
            callContext.getPolarisCallContext(), pendingUpdates);
    if (!result.isSuccess()) {
      // TODO: Retries and server-side cleanup on failure, review possible exceptions
      throw new CommitFailedException(
          "Transaction commit failed with status: %s, extraInfo: %s",
          result.getReturnStatus(), result.getExtraInformation());
    }

    eventAttributeMap.put(IcebergEventAttributes.TABLE_METADATAS, tableMetadataObjs);
    return new PolarisResult<>(null, this.extensionValue);
  }

  @Override
  public PolarisResult<ImmutableLoadCredentialsResponse, E> loadCredentials(
      TableIdentifier tableIdentifier, Optional<String> refreshCredentialsEndpoint) {

    Set<PolarisStorageActions> actionsRequested = authorizeLoadTable(tableIdentifier, true);
    ensureBaseInitialized();

    // Optimized credential vending is only supported for native Polaris catalogs.
    // Federated/external catalogs are passthrough — writes happen directly on the
    // remote catalog independently of Polaris, so there is no guarantee that entity
    // internal properties (e.g. location) in the Polaris metastore are in sync with
    // the remote catalog's actual table metadata.
    // Note: this check must come after authorizeLoadTable because baseCatalog is
    // initialized lazily during authorization.
    if (isFederated) {
      return new PolarisResult<>(
          fallbackToFullLoadTable(tableIdentifier, refreshCredentialsEndpoint),
          this.extensionValue);
    }

    IcebergTableLikeEntity entity = getTableEntity(tableIdentifier);
    if (entity == null) {
      throw new NoSuchTableException("Table does not exist: %s", tableIdentifier);
    }

    Map<String, String> internalProperties = entity.getInternalPropertiesAsMap();
    String baseLocation = internalProperties.get(IcebergTableLikeEntity.LOCATION);

    if (baseLocation == null) {
      LOGGER
          .atDebug()
          .addKeyValue("tableIdentifier", tableIdentifier)
          .log(
              "Entity missing location in internal properties, requires backfill "
                  + "as it was likely not updated with stored property changes. "
                  + "Falling back to full loadTable path");
      return new PolarisResult<>(
          fallbackToFullLoadTable(tableIdentifier, refreshCredentialsEndpoint),
          this.extensionValue);
    }

    Set<String> tableLocations =
        StorageUtil.getLocationsUsedByTable(baseLocation, internalProperties);

    StorageAccessConfig storageAccessConfig =
        vendCredentials(
            tableIdentifier, tableLocations, actionsRequested, refreshCredentialsEndpoint);
    if (storageAccessConfig == null) {
      storageAccessConfig = StorageAccessConfig.builder().build();
    }

    Map<String, String> credentialConfig = storageAccessConfig.credentials();
    ImmutableLoadCredentialsResponse.Builder responseBuilder =
        ImmutableLoadCredentialsResponse.builder();

    if (!credentialConfig.isEmpty()) {
      responseBuilder.addCredentials(
          ImmutableCredential.builder().prefix(baseLocation).config(credentialConfig).build());
    } else {
      Boolean skipCredIndirection =
          realmConfig.getConfig(FeatureConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION);
      Preconditions.checkArgument(
          !storageAccessConfig.supportsCredentialVending() || skipCredIndirection,
          "Credential vending was requested for table %s, but no credentials are available",
          tableIdentifier);
    }

    return new PolarisResult<>(responseBuilder.build(), this.extensionValue);
  }

  private ImmutableLoadCredentialsResponse fallbackToFullLoadTable(
      TableIdentifier tableIdentifier, Optional<String> refreshCredentialsEndpoint) {
    // ifNoneMatch=null means loadTable can never return the not-modified (null-body) outcome.
    LoadTableResponse response =
        loadTable(
                tableIdentifier,
                SNAPSHOTS_ALL,
                null,
                EnumSet.of(AccessDelegationMode.VENDED_CREDENTIALS),
                refreshCredentialsEndpoint)
            .body();
    if (response == null) {
      throw new IllegalStateException(
          "loadTable returned not-modified with ifNoneMatch=null; this is unreachable by construction");
    }
    return ImmutableLoadCredentialsResponse.builder().credentials(response.credentials()).build();
  }

  @Override
  public PolarisResult<Void, E> reportMetrics(
      TableIdentifier identifier, ReportMetricsRequest request) {

    PolarisAuthorizableOperation op =
        request.report() instanceof ScanReport
            ? PolarisAuthorizableOperation.REPORT_READ_METRICS
            : PolarisAuthorizableOperation.REPORT_WRITE_METRICS;

    authz.authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_TABLE, identifier);
    ensureBaseInitialized();

    // Get catalog and table IDs from resolved entities (already resolved during authorization)
    CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
    long resolvedCatalogId = resolvedCatalog.getId();

    // Get the table ID from the resolved path
    PolarisResolvedPathWrapper resolvedTable =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofTableLike(identifier));
    PolarisEntity tableEntity = resolvedTable.getRawLeafEntity();
    long tableId = tableEntity.getId();

    polarisMetricsReporter.reportMetric(
        catalogName, resolvedCatalogId, identifier, tableId, request.report(), clock.instant());
    return new PolarisResult<>(null, this.extensionValue);
  }

  @Override
  public PolarisResult<Boolean, E> sendNotification(
      TableIdentifier identifier, NotificationRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.SEND_NOTIFICATIONS;

    // For now, just require the full set of privileges on the base Catalog entity, which we can
    // also express just as the "root" Namespace for purposes of the BridgeBaseMetastoreViewCatalog
    // being
    // able to fetch Namespace.empty() as path key.
    List<TableIdentifier> extraPassthroughTableLikes = List.of(identifier);
    List<Namespace> extraPassthroughNamespaces = new ArrayList<>();
    extraPassthroughNamespaces.add(Namespace.empty());
    for (int i = 1; i <= identifier.namespace().length(); i++) {
      Namespace nsLevel =
          Namespace.of(
              Arrays.stream(identifier.namespace().levels()).limit(i).toArray(String[]::new));
      extraPassthroughNamespaces.add(nsLevel);
    }
    authz.authorizeBasicNamespaceOperationOrThrow(
        op, Namespace.empty(), extraPassthroughNamespaces, extraPassthroughTableLikes, null);
    ensureBaseInitialized();

    CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
    if (resolvedCatalog
        .getCatalogType()
        .equals(org.apache.polaris.core.admin.model.Catalog.TypeEnum.INTERNAL)) {
      LOGGER
          .atWarn()
          .addKeyValue("catalog", resolvedCatalog)
          .addKeyValue("notification", request)
          .log("Attempted notification on internal catalog");
      throw new BadRequestException("Cannot update internal catalog via notifications");
    }
    boolean result =
        baseCatalog instanceof SupportsNotifications notificationCatalog
            && notificationCatalog.sendNotification(identifier, request);
    return new PolarisResult<>(result, this.extensionValue);
  }

  protected @Nullable IcebergTableLikeEntity getTableEntity(TableIdentifier tableIdentifier) {
    PolarisResolvedPathWrapper target =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofTableLike(tableIdentifier));
    PolarisEntity rawLeafEntity = target.getRawLeafEntity();
    if (rawLeafEntity.getType() == PolarisEntityType.TABLE_LIKE) {
      return IcebergTableLikeEntity.of(rawLeafEntity);
    }
    return null; // could be an external catalog
  }

  protected Optional<AccessDelegationMode> resolveAccessDelegationModes(
      EnumSet<AccessDelegationMode> requestedModes) {

    CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
    Optional<AccessDelegationMode> resolvedMode =
        accessDelegationModeResolver.resolve(requestedModes, resolvedCatalog);

    // TODO remove when remote signing is implemented
    // Reject if the resolved mode is REMOTE_SIGNING since it's not yet supported
    Preconditions.checkArgument(
        resolvedMode.orElse(null) != AccessDelegationMode.REMOTE_SIGNING,
        "Unsupported access delegation mode: %s",
        AccessDelegationMode.REMOTE_SIGNING);

    return resolvedMode;
  }

  private @NonNull LoadTableResponse filterResponseToSnapshots(
      LoadTableResponse loadTableResponse, String snapshots) {
    if (snapshots == null || snapshots.equalsIgnoreCase(SNAPSHOTS_ALL)) {
      return loadTableResponse;
    } else if (snapshots.equalsIgnoreCase(SNAPSHOTS_REFS)) {
      TableMetadata metadata = loadTableResponse.tableMetadata();

      Set<Long> referencedSnapshotIds =
          metadata.refs().values().stream()
              .map(SnapshotRef::snapshotId)
              .collect(Collectors.toSet());

      TableMetadata filteredMetadata =
          metadata.removeSnapshotsIf(s -> !referencedSnapshotIds.contains(s.snapshotId()));

      return LoadTableResponse.builder()
          .withTableMetadata(filteredMetadata)
          .addAllConfig(loadTableResponse.config())
          .addAllCredentials(loadTableResponse.credentials())
          .build();
    } else {
      throw new IllegalArgumentException("Unrecognized snapshots: " + snapshots);
    }
  }

  protected LoadTableResponse.Builder buildLoadTableResponseWithDelegationCredentials(
      TableIdentifier tableIdentifier,
      TableMetadata tableMetadata,
      Optional<AccessDelegationMode> delegationMode,
      Set<PolarisStorageActions> actions,
      Optional<String> refreshCredentialsEndpoint) {
    LoadTableResponse.Builder responseBuilder =
        LoadTableResponse.builder().withTableMetadata(tableMetadata);
    PolarisResolvedPathWrapper resolvedStoragePath =
        CatalogUtils.findResolvedStorageEntity(resolvedEntityView, tableIdentifier);

    if (resolvedStoragePath == null) {
      LOGGER.debug(
          "Unable to find storage configuration information for table {}", tableIdentifier);
      return responseBuilder;
    }

    if (!isFederated
        || realmConfig.getConfig(
            FeatureConfiguration.ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING,
            getResolvedCatalogEntity())) {

      Set<String> tableLocations = StorageUtil.getLocationsUsedByTable(tableMetadata);

      // Validate that the table's locations are still within the catalog's current
      // allowedLocations before vending credentials. This protects against cases where
      // allowedLocations were tightened after the table was created.
      validateTableLocations(tableIdentifier, tableLocations, resolvedStoragePath);

      StorageAccessConfig storageAccessConfig =
          storageAccessConfigProvider.getStorageAccessConfig(
              tableIdentifier,
              tableLocations,
              actions,
              refreshCredentialsEndpoint,
              resolvedStoragePath);
      Map<String, String> credentialConfig = storageAccessConfig.credentials();
      if (AccessDelegationMode.VENDED_CREDENTIALS.equals(delegationMode.orElse(null))) {
        if (!credentialConfig.isEmpty()) {
          responseBuilder.addAllConfig(credentialConfig);
          responseBuilder.addCredential(
              ImmutableCredential.builder()
                  .prefix(tableMetadata.location())
                  .config(credentialConfig)
                  .build());
        } else {
          Boolean skipCredIndirection =
              realmConfig.getConfig(FeatureConfiguration.SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION);
          Preconditions.checkArgument(
              !storageAccessConfig.supportsCredentialVending() || skipCredIndirection,
              "Credential vending was requested for table %s, but no credentials are available",
              tableIdentifier);
        }
      }
      responseBuilder.addAllConfig(storageAccessConfig.extraProperties());
    }

    return responseBuilder;
  }

  private void validateTableLocations(
      TableIdentifier tableIdentifier,
      Set<String> tableLocations,
      PolarisResolvedPathWrapper resolvedStoragePath) {

    try {
      // Delegate to common validation logic. This is called for both native and federated
      // catalogs before vending credentials to ensure locations are still within the
      // current catalog's allowedLocations (defense against policy changes after table creation).
      CatalogUtils.validateLocationsForTableLike(
          realmConfig, tableIdentifier, tableLocations, resolvedStoragePath);

      LOGGER
          .atInfo()
          .addKeyValue("tableIdentifier", tableIdentifier)
          .addKeyValue("tableLocations", tableLocations)
          .log("Validated table locations for credential vending");
    } catch (ForbiddenException e) {
      LOGGER
          .atError()
          .addKeyValue("tableIdentifier", tableIdentifier)
          .addKeyValue("tableLocations", tableLocations)
          .log("Table locations validation failed for credential vending");
      throw new ForbiddenException(
          "Table '%s' has locations outside the catalog's current allowed locations: %s",
          tableIdentifier, e.getMessage());
    }
  }

  private StorageAccessConfig vendCredentials(
      TableIdentifier tableIdentifier,
      Set<String> tableLocations,
      Set<PolarisStorageActions> actionsRequested,
      Optional<String> refreshCredentialsEndpoint) {
    PolarisResolvedPathWrapper resolvedStoragePath =
        CatalogUtils.findResolvedStorageEntity(resolvedEntityView, tableIdentifier);
    if (resolvedStoragePath == null) {
      LOGGER.debug(
          "Unable to find storage configuration information for table {}", tableIdentifier);
      return null;
    }

    // Re-validate before vending in case this is called from other paths in the future.
    // Primary validation for loadCredentials and delegation happens at call sites.
    validateTableLocations(tableIdentifier, tableLocations, resolvedStoragePath);

    return storageAccessConfigProvider.getStorageAccessConfig(
        tableIdentifier,
        tableLocations,
        actionsRequested,
        refreshCredentialsEndpoint,
        resolvedStoragePath);
  }

  private Optional<String> etagForCreatedTable(
      TableIdentifier tableIdentifier, LoadTableResponse response) {
    // ETag derivation moved from IcebergCatalogAdapter.tryInsertETagHeader: the merged impl now
    // supplies the ETag first-class in the result so the adapter can become pass-through (Issue
    // 29 Inc6). createTableDirect/registerTable have carried this since pre-PoC OSS (#1037); only
    // the channel it rides changed (Issue 32: withEtag, not a dedicated PolarisResult.etag slot).
    if (response.metadataLocation() != null) {
      return Optional.of(
          IcebergHttpUtil.generateETagForMetadataFileLocation(response.metadataLocation()));
    }
    LOGGER
        .atWarn()
        .addKeyValue("tableIdentifier", tableIdentifier)
        .log("Response has null metadataLocation; omitting etag");
    return Optional.empty();
  }

  // ---- Issue 29 Inc 4d: view ops (transcribed from IcebergCatalogHandler). ----

  @Override
  public PolarisResult<ListTablesResponse, E> listViews(
      Namespace namespace, String pageToken, Integer pageSize) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_VIEWS;
    authz.authorizeBasicNamespaceOperationOrThrow(op, namespace);
    ensureBaseInitialized();

    ListTablesResponse response;
    if (isFederated) {
      if (baseCatalog instanceof ViewCatalog federatedViewCatalog) {
        response =
            catalogHandlerUtils.listViews(federatedViewCatalog, namespace, pageToken, pageSize);
      } else {
        throw new BadRequestException(
            "Unsupported operation: listViews with baseCatalog type: %s",
            baseCatalog.getClass().getName());
      }
    } else {
      PageToken pageRequest = PageToken.build(pageToken, pageSize, this::shouldDecodeToken);
      var results = this.polarisIcebergCatalog.listViews(namespace, pageRequest);
      response =
          ListTablesResponse.builder()
              .addAll(results.items())
              .nextPageToken(results.encodedResponseToken())
              .build();
    }
    return new PolarisResult<>(response, this.extensionValue);
  }

  @Override
  public PolarisResult<LoadViewResponse, E> createView(
      Namespace namespace, CreateViewRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_VIEW;
    authz.authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        op, TableIdentifier.of(namespace, request.name()));
    ensureBaseInitialized();

    CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
    if (resolvedCatalog.isStaticFacade()) {
      throw new BadRequestException("Cannot create view on static-facade external catalogs.");
    }
    return new PolarisResult<>(
        catalogHandlerUtils.createView(viewCatalog, namespace, request), this.extensionValue);
  }

  @Override
  public PolarisResult<LoadViewResponse, E> registerView(
      Namespace namespace, RegisterViewRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.REGISTER_VIEW;
    authz.authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        op, TableIdentifier.of(namespace, request.name()));
    ensureBaseInitialized();

    return new PolarisResult<>(
        catalogHandlerUtils.registerView(viewCatalog, namespace, request), this.extensionValue);
  }

  @Override
  public PolarisResult<LoadViewResponse, E> loadView(TableIdentifier viewIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_VIEW;
    authz.authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);
    ensureBaseInitialized();

    return new PolarisResult<>(
        catalogHandlerUtils.loadView(viewCatalog, viewIdentifier), this.extensionValue);
  }

  @Override
  public PolarisResult<LoadViewResponse, E> replaceView(
      TableIdentifier viewIdentifier, UpdateTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.REPLACE_VIEW;
    authz.authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);
    ensureBaseInitialized();

    CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
    if (resolvedCatalog.isStaticFacade()) {
      throw new BadRequestException("Cannot replace view on static-facade external catalogs.");
    }
    return new PolarisResult<>(
        catalogHandlerUtils.updateView(viewCatalog, viewIdentifier, applyUpdateFilters(request)),
        this.extensionValue);
  }

  @Override
  public PolarisResult<Void, E> dropView(TableIdentifier viewIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_VIEW;
    authz.authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);
    ensureBaseInitialized();

    catalogHandlerUtils.dropView(viewCatalog, viewIdentifier);
    return new PolarisResult<>(null, this.extensionValue);
  }

  @Override
  public PolarisResult<Void, E> viewExists(TableIdentifier viewIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.VIEW_EXISTS;
    authz.authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);
    ensureBaseInitialized();

    // TODO: Just skip CatalogHandlers for this one maybe
    catalogHandlerUtils.loadView(viewCatalog, viewIdentifier);
    return new PolarisResult<>(null, this.extensionValue);
  }

  @Override
  public PolarisResult<Void, E> renameView(RenameTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.RENAME_VIEW;
    authz.authorizeRenameTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_VIEW, request.source(), request.destination());
    ensureBaseInitialized();

    CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
    if (resolvedCatalog.isStaticFacade()) {
      throw new BadRequestException("Cannot rename view on static-facade external catalogs.");
    }
    catalogHandlerUtils.renameView(viewCatalog, request);
    return new PolarisResult<>(null, this.extensionValue);
  }
}
