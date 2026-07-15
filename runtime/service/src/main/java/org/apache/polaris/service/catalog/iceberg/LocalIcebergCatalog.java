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

import static org.apache.polaris.service.catalog.common.ExceptionUtils.alreadyExistsExceptionForTableLikeEntity;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.alreadyExistsExceptionWithSameNameForTableLikeEntity;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.entityNameForSubType;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.noSuchNamespaceException;
import static org.apache.polaris.service.catalog.common.ExceptionUtils.notFoundExceptionForTableLikeEntity;
import static org.apache.polaris.service.exception.IcebergExceptionMapper.isStorageProviderRetryableException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
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
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.view.BaseMetastoreViewCatalog;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;
import org.apache.iceberg.view.ViewOperations;
import org.apache.iceberg.view.ViewProperties;
import org.apache.iceberg.view.ViewUtil;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.catalog.FederatedCatalogFactory;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
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
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.events.EventAttributeMap;
import org.apache.polaris.core.events.PolarisEvent;
import org.apache.polaris.core.events.PolarisEventType;
import org.apache.polaris.core.exceptions.CommitConflictException;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.TransactionWorkspaceMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.resolver.EntityResolverManifestView;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifestCatalogView;
import org.apache.polaris.core.persistence.resolver.ResolutionRequest;
import org.apache.polaris.core.persistence.resolver.ResolutionResult;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.rest.NamespaceUtils;
import org.apache.polaris.core.rest.PolarisEndpoints;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.core.storage.StorageUtil;
import org.apache.polaris.service.catalog.SupportsNotifications;
import org.apache.polaris.service.catalog.common.CatalogAuthorizer;
import org.apache.polaris.service.catalog.common.CatalogUtils;
import org.apache.polaris.service.catalog.common.LocationUtils;
import org.apache.polaris.service.catalog.io.FileIOUtil;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.catalog.validation.IcebergPropertiesValidation;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEventDispatcher;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.http.IcebergHttpUtil;
import org.apache.polaris.service.reporting.PolarisMetricsReporter;
import org.apache.polaris.service.types.NotificationRequest;
import org.apache.polaris.service.types.NotificationType;
import org.apache.polaris.spi.durable.DurableManager;
import org.apache.polaris.spi.feature.CatalogPrefixParser;
import org.apache.polaris.spi.feature.catalog.AccessDelegationMode;
import org.apache.polaris.spi.feature.catalog.AccessDelegationModeResolver;
import org.apache.polaris.spi.feature.catalog.ConditionalLoadOutcome;
import org.apache.polaris.spi.feature.catalog.IcebergCatalogOps;
import org.apache.polaris.spi.feature.catalog.IcebergViewCatalogOps;
import org.apache.polaris.spi.feature.catalog.IfNoneMatch;
import org.apache.polaris.spi.feature.catalog.NoExtension;
import org.apache.polaris.spi.feature.catalog.PolarisResult;
import org.apache.polaris.spi.substrate.EntityResolver;
import org.apache.polaris.spi.substrate.PolarisAuthorizer;
import org.apache.polaris.spi.substrate.StorageIoProvider;
import org.apache.polaris.spi.substrate.TaskExecutor;
import org.apache.polaris.storage.model.VendedClientStorageAccess;
import org.apache.polaris.storage.model.VendedServerStorageAccess;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Defines the relationship between PolarisEntities and Iceberg's business logic. */
public class LocalIcebergCatalog extends BaseMetastoreViewCatalog
    implements SupportsNamespaces,
        SupportsNotifications,
        Closeable,
        IcebergCatalogOps<NoExtension>,
        IcebergViewCatalogOps<NoExtension> {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalIcebergCatalog.class);

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
  // Local: baseCatalog/namespaceCatalog/viewCatalog are this instance; federated: a narrow remote
  // delegate.
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
   */
  public LocalIcebergCatalog(
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
  public LocalIcebergCatalog(
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
      CatalogPrefixParser prefixParser) {
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
    this.authz = new CatalogAuthorizer(entityResolver, authorizer, principal, catalogName);
    // resolvedEntityView / catalogEntity / catalogId are established by ensureBaseInitialized().
  }

  @Override
  public String name() {
    return catalogName;
  }

  @VisibleForTesting
  public void setCatalogFileIo(FileIO fileIO) {
    catalogFileIO = fileIO;
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    Preconditions.checkState(
        this.catalogName.equals(name),
        "Tried to initialize catalog as name %s but already constructed with name %s",
        name,
        this.catalogName);

    // Ensure catalogProperties is assigned before calling metricsReporter() for proper
    // functionality.
    catalogProperties = properties;

    // Base location from catalogEntity is primary source of truth, otherwise fall through
    // to the same key from the properties map, and finally fall through to WAREHOUSE_LOCATION.
    String baseLocation =
        Optional.ofNullable(catalogEntity.getBaseLocation())
            .orElse(
                properties.getOrDefault(
                    CatalogEntity.DEFAULT_BASE_LOCATION_KEY,
                    properties.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, "")));
    this.defaultBaseLocation = baseLocation.replaceAll("/*$", "");

    var storageConfigurationInfo = catalogEntity.getStorageConfigurationInfo();
    ioImplClassName =
        IcebergPropertiesValidation.determineFileIOClassName(
            realmConfig, properties, storageConfigurationInfo);

    if (ioImplClassName == null) {
      LOGGER.warn(
          "Cannot resolve property '{}' for null storageConfiguration.",
          CatalogProperties.FILE_IO_IMPL);
    }

    this.closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(metricsReporter());
    closeableGroup.setSuppressCloseFailure(true);

    tableDefaultProperties =
        PropertyUtil.propertiesWithPrefix(properties, CatalogProperties.TABLE_DEFAULT_PREFIX);
  }

  public void setMetaStoreManager(DurableManager newMetaStoreManager) {
    this.metaStoreManager = newMetaStoreManager;
  }

  @Override
  protected Map<String, String> properties() {
    return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
  }

  @Override
  public Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
    return registerTable(identifier, metadataFileLocation, false);
  }

  /**
   * Register a table with optional overwrite semantics.
   *
   * <p>When {@code overwrite} is false (the default) this behaves like a normal register and will
   * fail if the table already exists. When {@code overwrite} is true and the named table already
   * exists, this method updates the table's stored metadata-location to point at the provided
   * metadata file. The overwrite path performs additional validation to ensure the supplied
   * metadata file and its location are consistent with the table's resolved storage configuration.
   *
   * @param identifier the table identifier
   * @param metadataFileLocation the metadata file location
   * @param overwrite if true, update existing table metadata; if false, throw exception if table
   *     exists
   * @return the registered table
   */
  @Override
  public Table registerTable(
      TableIdentifier identifier, String metadataFileLocation, boolean overwrite) {
    Preconditions.checkArgument(
        identifier != null && isValidIdentifier(identifier), "Invalid identifier: %s", identifier);
    Preconditions.checkArgument(
        metadataFileLocation != null && !metadataFileLocation.isEmpty(),
        "Cannot register an empty metadata file location as a table");

    int lastSlashIndex = metadataFileLocation.lastIndexOf("/");
    Preconditions.checkArgument(
        lastSlashIndex != -1,
        "Invalid metadata file location; metadata file location must be absolute and contain a '/': %s",
        metadataFileLocation);

    if (viewExists(identifier)) {
      throw alreadyExistsExceptionWithSameNameForTableLikeEntity(
          identifier, PolarisEntitySubType.ICEBERG_VIEW);
    }

    boolean tableExists = tableExists(identifier);
    if (!overwrite && tableExists) {
      throw alreadyExistsExceptionForTableLikeEntity(
          identifier, PolarisEntitySubType.ICEBERG_TABLE);
    }

    String locationDir = metadataFileLocation.substring(0, lastSlashIndex);
    if (tableExists) {
      return overwriteRegisteredTable(identifier, metadataFileLocation, locationDir);
    } else {
      return registerNewTable(identifier, metadataFileLocation, locationDir);
    }
  }

  private Table registerNewTable(
      TableIdentifier identifier, String metadataFileLocation, String locationDir) {
    TableOperations ops = newTableOps(identifier);

    PolarisResolvedPathWrapper resolvedParent =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(identifier.namespace()));
    if (resolvedParent == null) {
      // Illegal state because the namespace should've already been in the static resolution set.
      throw new IllegalStateException(
          String.format("Failed to fetch resolved parent for TableIdentifier '%s'", identifier));
    }

    validateLocationForTableLike(identifier, metadataFileLocation, resolvedParent);

    FileIO fileIO =
        loadFileIOForTableLike(
            identifier,
            Set.of(locationDir),
            resolvedParent,
            new HashMap<>(tableDefaultProperties),
            Set.of(PolarisStorageActions.READ, PolarisStorageActions.LIST));

    InputFile metadataFile = fileIO.newInputFile(metadataFileLocation);
    TableMetadata metadata = TableMetadataParser.read(metadataFile);
    validateMetadataFileInTableDir(identifier, metadata);
    ops.commit(null, metadata);

    return new BaseTable(ops, fullTableName(name(), identifier), metricsReporter());
  }

  private Table overwriteRegisteredTable(
      TableIdentifier identifier, String metadataFileLocation, String locationDir) {
    PolarisResolvedPathWrapper resolvedPath =
        resolvedEntityView.getPassthroughResolvedPath(
            ResolvedPathKey.ofTableLike(identifier), PolarisEntitySubType.ANY_SUBTYPE);
    if (resolvedPath == null || resolvedPath.getRawLeafEntity() == null) {
      throw new NoSuchTableException("Table does not exist: %s", identifier);
    }

    validateLocationForTableLike(identifier, metadataFileLocation, resolvedPath);

    FileIO fileIO =
        loadFileIOForTableLike(
            identifier,
            Set.of(locationDir),
            resolvedPath,
            new HashMap<>(tableDefaultProperties),
            Set.of(PolarisStorageActions.READ, PolarisStorageActions.LIST));

    TableMetadata metadata = TableMetadataParser.read(fileIO, metadataFileLocation);
    validateMetadataFileInTableDir(identifier, metadata);

    List<PolarisEntity> resolvedNamespace = resolvedPath.getRawParentPath();
    var tableLocations = StorageUtil.getLocationsUsedByTable(metadata);
    CatalogUtils.validateLocationsForTableLike(
        realmConfig, identifier, tableLocations, resolvedPath);
    tableLocations.forEach(
        location ->
            validateNoLocationOverlap(
                catalogEntity,
                identifier,
                resolvedNamespace,
                location,
                resolvedPath.getRawLeafEntity()));

    PolarisEntity rawEntity = resolvedPath.getRawLeafEntity();
    if (rawEntity.getSubType() != PolarisEntitySubType.ICEBERG_TABLE) {
      throw alreadyExistsExceptionForTableLikeEntity(identifier, rawEntity.getSubType());
    }

    IcebergTableLikeEntity existingEntity = IcebergTableLikeEntity.of(rawEntity);

    Map<String, String> storedProperties = buildTableMetadataPropertiesMap(metadata);
    IcebergTableLikeEntity updatedEntity =
        new IcebergTableLikeEntity.Builder(existingEntity)
            .setInternalProperties(storedProperties)
            .setBaseLocation(metadata.location())
            .setMetadataLocation(metadataFileLocation)
            .build();

    updateTableLike(identifier, updatedEntity);

    TableOperations ops = newTableOps(identifier);
    return new BaseTable(ops, fullTableName(name(), identifier), metricsReporter());
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new PolarisIcebergCatalogTableBuilder(identifier, schema);
  }

  @Override
  public ViewBuilder buildView(TableIdentifier identifier) {
    return new PolarisIcebergCatalogViewBuilder(identifier);
  }

  @VisibleForTesting
  public TableOperations newTableOps(
      TableIdentifier tableIdentifier, boolean makeMetadataCurrentOnCommit) {
    return new BasePolarisTableOperations(
        catalogFileIO, tableIdentifier, makeMetadataCurrentOnCommit);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    boolean makeMetadataCurrentOnCommit =
        realmConfig.getConfig(
            BehaviorChangeConfiguration.TABLE_OPERATIONS_MAKE_METADATA_CURRENT_ON_COMMIT);
    return newTableOps(tableIdentifier, makeMetadataCurrentOnCommit);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    if (tableIdentifier.namespace().isEmpty()) {
      return SLASH.join(
          defaultNamespaceLocation(tableIdentifier.namespace()), tableIdentifier.name());
    } else {
      PolarisResolvedPathWrapper resolvedNamespace =
          resolvedEntityView.getResolvedPath(
              ResolvedPathKey.ofNamespace(tableIdentifier.namespace()));
      if (resolvedNamespace == null) {
        throw noSuchNamespaceException(tableIdentifier.namespace());
      }
      List<PolarisEntity> namespacePath = resolvedNamespace.getRawFullPath();
      String namespaceLocation = resolveLocationForPath(diagnostics, namespacePath);
      return SLASH.join(namespaceLocation, tableIdentifier.name());
    }
  }

  private String defaultNamespaceLocation(Namespace namespace) {
    if (namespace.isEmpty()) {
      return defaultBaseLocation;
    } else {
      return SLASH.join(defaultBaseLocation, SLASH.join(namespace.levels()));
    }
  }

  @Override
  public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
    TableOperations ops = newTableOps(tableIdentifier);
    TableMetadata lastMetadata;
    if (purge && ops.current() != null) {
      lastMetadata = ops.current();
    } else {
      lastMetadata = null;
    }

    Optional<PolarisEntity> storageInfoEntity =
        FileIOUtil.findStorageInfoFromHierarchy(
            CatalogUtils.findResolvedStorageEntity(resolvedEntityView, tableIdentifier));

    // The storageProperties we stash away in the Task should be the superset of the
    // internalProperties of the StorageInfoEntity to be able to use its StorageIntegration
    // combined with other miscellaneous FileIO-related initialization properties defined
    // by the Table.
    Map<String, String> storageProperties =
        storageInfoEntity
            .map(PolarisEntity::getInternalPropertiesAsMap)
            .map(
                properties -> {
                  if (lastMetadata == null) {
                    return Map.<String, String>of();
                  }
                  Map<String, String> clone = new HashMap<>();

                  // The user-configurable table properties are the baseline, but then override
                  // with our restricted properties so that table properties can't clobber the
                  // more restricted ones.
                  clone.putAll(lastMetadata.properties());
                  clone.put(CatalogProperties.FILE_IO_IMPL, ioImplClassName);
                  clone.putAll(properties);
                  clone.put(PolarisTaskConstants.STORAGE_LOCATION, lastMetadata.location());
                  return clone;
                })
            .orElse(Map.of());
    DropEntityResult dropEntityResult =
        dropTableLike(
            PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier, storageProperties, purge);
    if (!dropEntityResult.isSuccess()) {
      switch (dropEntityResult.getReturnStatus()) {
        case BaseResult.ReturnStatus.ENTITY_NOT_FOUND:
          return false;

        case BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED:
          LOGGER.debug(
              "Catalog path cannot be resolved for {}, treating as dropped; extraInfo={}",
              tableIdentifier,
              dropEntityResult.getExtraInformation());
          return false;

        case BaseResult.ReturnStatus.ENTITY_UNDROPPABLE:
          throw new ForbiddenException(
              "Table %s cannot be dropped: %s",
              tableIdentifier, dropEntityResult.getExtraInformation());

        default:
          throw new ServiceFailureException(
              "Failed to drop table %s, status=%s, extraInfo=%s",
              tableIdentifier,
              dropEntityResult.getReturnStatus(),
              dropEntityResult.getExtraInformation());
      }
    }

    if (purge && lastMetadata != null && dropEntityResult.getCleanupTaskId() != null) {
      LOGGER.info(
          "Scheduled cleanup task {} for table {}",
          dropEntityResult.getCleanupTaskId(),
          tableIdentifier);
      taskExecutor.addTaskHandlerContext(dropEntityResult.getCleanupTaskId(), callContext);
    }

    return true;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return listTables(namespace, PageToken.readEverything()).items();
  }

  public Page<TableIdentifier> listTables(Namespace namespace, PageToken pageToken) {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException(
          "Cannot list tables for namespace. Namespace does not exist: '%s'", namespace);
    }

    return listTableLike(PolarisEntitySubType.ICEBERG_TABLE, namespace, pageToken);
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    if (from.equals(to)) {
      return;
    }

    renameTableLike(PolarisEntitySubType.ICEBERG_TABLE, from, to);
  }

  @Override
  public void createNamespace(Namespace namespace) {
    createNamespace(namespace, Collections.emptyMap());
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    LOGGER.debug("Creating namespace {} with metadata {}", namespace, metadata);
    if (namespace.isEmpty()) {
      throw new AlreadyExistsException(
          "Cannot create root namespace, as it already exists implicitly.");
    }

    // TODO: These should really be helpers in core Iceberg Namespace.
    Namespace parentNamespace = PolarisCatalogHelpers.getParentNamespace(namespace);

    PolarisResolvedPathWrapper resolvedParent =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(parentNamespace));
    if (resolvedParent == null) {
      throw new NoSuchNamespaceException(
          "Cannot create namespace %s. Parent namespace does not exist.", namespace);
    }
    createNamespaceInternal(namespace, metadata, resolvedParent);
  }

  private void createNamespaceInternal(
      Namespace namespace,
      Map<String, String> metadata,
      PolarisResolvedPathWrapper resolvedParent) {
    String baseLocation = resolveNamespaceLocation(namespace, metadata);

    // Set / suffix
    boolean requireTrailingSlash =
        realmConfig.getConfig(FeatureConfiguration.ADD_TRAILING_SLASH_TO_LOCATION);
    if (requireTrailingSlash && !baseLocation.endsWith("/")) {
      baseLocation += "/";
    }

    NamespaceEntity entity =
        new NamespaceEntity.Builder(namespace)
            .setCatalogId(getCatalogId())
            .setId(getMetaStoreManager().generateNewEntityId(getCurrentPolarisContext()).getId())
            .setParentId(resolvedParent.getRawLeafEntity().getId())
            .setProperties(metadata)
            .setCreateTimestamp(System.currentTimeMillis())
            .setBaseLocation(baseLocation)
            .build();
    if (!realmConfig.getConfig(FeatureConfiguration.ALLOW_NAMESPACE_LOCATION_OVERLAP)) {
      LOGGER.debug("Validating no overlap for {} with sibling tables or namespaces", namespace);
      validateNoLocationOverlap(entity, resolvedParent.getRawFullPath());
    } else {
      LOGGER.debug("Skipping location overlap validation for namespace '{}'", namespace);
    }
    if (!realmConfig.getConfig(
        BehaviorChangeConfiguration.ALLOW_NAMESPACE_CUSTOM_LOCATION, catalogEntity)) {
      validateNamespaceLocation(entity, resolvedParent);
    }
    EntityResult result =
        getMetaStoreManager()
            .createEntityIfNotExists(
                getCurrentPolarisContext(),
                PolarisEntity.toCoreList(resolvedParent.getRawFullPath()),
                entity);
    if (!result.isSuccess()) {
      if (result.alreadyExists()) {
        throw new AlreadyExistsException(
            "Cannot create namespace %s. Namespace already exists", namespace);
      } else {
        throw new ServiceFailureException(
            "Unexpected error trying to create namespace %s. Status: %s ExtraInfo: %s",
            namespace, result.getReturnStatus(), result.getExtraInformation());
      }
    }
  }

  private String resolveNamespaceLocation(Namespace namespace, Map<String, String> properties) {
    if (properties.containsKey(PolarisEntityConstants.ENTITY_BASE_LOCATION)) {
      return properties.get(PolarisEntityConstants.ENTITY_BASE_LOCATION);
    } else {
      List<PolarisEntity> parentPath =
          namespace.length() > 1
              ? getResolvedParentNamespace(namespace).getRawFullPath()
              : List.of(resolvedEntityView.getResolvedCatalogEntity());

      String parentLocation = resolveLocationForPath(diagnostics, parentPath);

      return parentLocation + "/" + namespace.level(namespace.length() - 1);
    }
  }

  private static @NonNull String resolveLocationForPath(
      @NonNull PolarisDiagnostics diagnostics, List<PolarisEntity> parentPath) {
    // always take the first object. If it has the base-location, stop there
    AtomicBoolean foundBaseLocation = new AtomicBoolean(false);
    return parentPath.reversed().stream()
        .takeWhile(
            entity ->
                !foundBaseLocation.getAndSet(
                    entity
                        .getPropertiesAsMap()
                        .containsKey(PolarisEntityConstants.ENTITY_BASE_LOCATION)))
        .toList()
        .reversed()
        .stream()
        .map(entity -> baseLocation(diagnostics, entity))
        .map(LocalIcebergCatalog::stripLeadingTrailingSlash)
        .collect(Collectors.joining("/"));
  }

  private static @Nullable String baseLocation(
      @NonNull PolarisDiagnostics diagnostics, PolarisEntity entity) {
    if (entity.getType().equals(PolarisEntityType.CATALOG)) {
      CatalogEntity catEntity = CatalogEntity.of(entity);
      String catalogDefaultBaseLocation = catEntity.getBaseLocation();
      diagnostics.checkNotNull(
          catalogDefaultBaseLocation,
          "Tried to resolve location with catalog with null default base location",
          "catalog = {}",
          catEntity);
      return catalogDefaultBaseLocation;
    } else {
      String baseLocation =
          entity.getPropertiesAsMap().get(PolarisEntityConstants.ENTITY_BASE_LOCATION);
      if (baseLocation != null) {
        return baseLocation;
      } else {
        String entityName = entity.getName();
        diagnostics.checkNotNull(
            entityName,
            "Tried to resolve location with entity without base location or name",
            "entity = {}",
            entity);
        return entityName;
      }
    }
  }

  private static String stripLeadingTrailingSlash(String location) {
    if (location.startsWith("/")) {
      return stripLeadingTrailingSlash(location.substring(1));
    }
    if (location.endsWith("/")) {
      return location.substring(0, location.length() - 1);
    } else {
      return location;
    }
  }

  private PolarisResolvedPathWrapper getResolvedParentNamespace(Namespace namespace) {
    Namespace parentNamespace =
        Namespace.of(Arrays.copyOf(namespace.levels(), namespace.length() - 1));
    PolarisResolvedPathWrapper resolvedParent =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(parentNamespace));
    if (resolvedParent == null) {
      return resolvedEntityView.getPassthroughResolvedPath(
          ResolvedPathKey.ofNamespace(parentNamespace));
    }
    return resolvedParent;
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    return Optional.ofNullable(namespace)
        .filter(ns -> !ns.isEmpty())
        .map(ns -> resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(ns)))
        .isPresent();
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    if (namespace.isEmpty()) {
      throw new IllegalArgumentException("Cannot drop root namespace");
    }
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(namespace));
    if (resolvedEntities == null) {
      return false;
    }

    List<PolarisEntity> catalogPath = resolvedEntities.getRawParentPath();
    PolarisEntity leafEntity = resolvedEntities.getRawLeafEntity();

    // drop if exists and is empty
    DropEntityResult dropEntityResult =
        getMetaStoreManager()
            .dropEntityIfExists(
                getCurrentPolarisContext(),
                PolarisEntity.toCoreList(catalogPath),
                leafEntity,
                Map.of(),
                realmConfig.getConfig(FeatureConfiguration.CLEANUP_ON_NAMESPACE_DROP));

    if (!dropEntityResult.isSuccess()) {
      switch (dropEntityResult.getReturnStatus()) {
        case BaseResult.ReturnStatus.NAMESPACE_NOT_EMPTY:
        case BaseResult.ReturnStatus.CATALOG_NOT_EMPTY:
          throw new NamespaceNotEmptyException("Namespace %s is not empty", namespace);

        case BaseResult.ReturnStatus.ENTITY_NOT_FOUND:
          return false;

        case BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED:
          LOGGER.debug(
              "Catalog path cannot be resolved for {}, treating as dropped; extraInfo={}",
              namespace,
              dropEntityResult.getExtraInformation());
          return false;

        default:
          throw new ServiceFailureException(
              "Failed to drop namespace %s, status=%s, extraInfo=%s",
              namespace,
              dropEntityResult.getReturnStatus(),
              dropEntityResult.getExtraInformation());
      }
    }

    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(namespace));
    if (resolvedEntities == null) {
      throw noSuchNamespaceException(namespace);
    }
    PolarisEntity entity = resolvedEntities.getRawLeafEntity();
    Map<String, String> newProperties = new HashMap<>(entity.getPropertiesAsMap());

    // Merge new properties into existing map.
    newProperties.putAll(properties);
    PolarisEntity updatedEntity =
        new PolarisEntity.Builder(entity).setProperties(newProperties).build();

    if (!realmConfig.getConfig(FeatureConfiguration.ALLOW_NAMESPACE_LOCATION_OVERLAP)) {
      LOGGER.debug("Validating no overlap with sibling tables or namespaces");
      validateNoLocationOverlap(
          NamespaceEntity.of(updatedEntity), resolvedEntities.getRawParentPath());
    } else {
      LOGGER.debug("Skipping location overlap validation for namespace '{}'", namespace);
    }
    if (!realmConfig.getConfig(
        BehaviorChangeConfiguration.ALLOW_NAMESPACE_CUSTOM_LOCATION, catalogEntity)) {
      if (properties.containsKey(PolarisEntityConstants.ENTITY_BASE_LOCATION)) {
        validateNamespaceLocation(NamespaceEntity.of(entity), resolvedEntities);
      }
    }

    List<PolarisEntity> parentPath = resolvedEntities.getRawFullPath();
    PolarisEntity returnedEntity =
        Optional.ofNullable(
                getMetaStoreManager()
                    .updateEntityPropertiesIfNotChanged(
                        getCurrentPolarisContext(),
                        PolarisEntity.toCoreList(parentPath),
                        updatedEntity)
                    .getEntity())
            .map(PolarisEntity::new)
            .orElse(null);
    if (returnedEntity == null) {
      throw new CommitConflictException("Concurrent modification of namespace: %s", namespace);
    }
    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(namespace));
    if (resolvedEntities == null) {
      throw noSuchNamespaceException(namespace);
    }
    PolarisEntity entity = resolvedEntities.getRawLeafEntity();

    Map<String, String> updatedProperties = new HashMap<>(entity.getPropertiesAsMap());
    properties.forEach(updatedProperties::remove);

    PolarisEntity updatedEntity =
        new PolarisEntity.Builder(entity).setProperties(updatedProperties).build();

    List<PolarisEntity> parentPath = resolvedEntities.getRawFullPath();
    PolarisEntity returnedEntity =
        Optional.ofNullable(
                getMetaStoreManager()
                    .updateEntityPropertiesIfNotChanged(
                        getCurrentPolarisContext(),
                        PolarisEntity.toCoreList(parentPath),
                        updatedEntity)
                    .getEntity())
            .map(PolarisEntity::new)
            .orElse(null);
    if (returnedEntity == null) {
      throw new CommitConflictException("Concurrent modification of namespace: %s", namespace);
    }
    return true;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(namespace));
    if (resolvedEntities == null) {
      throw noSuchNamespaceException(namespace);
    }
    NamespaceEntity entity = NamespaceEntity.of(resolvedEntities.getRawLeafEntity());
    Preconditions.checkState(
        entity.getParentNamespace().equals(PolarisCatalogHelpers.getParentNamespace(namespace)),
        "Mismatched stored parentNamespace '%s' vs looked up parentNamespace '%s",
        entity.getParentNamespace(),
        PolarisCatalogHelpers.getParentNamespace(namespace));

    return entity.getPropertiesAsMap();
  }

  @Override
  public List<Namespace> listNamespaces() {
    return listNamespaces(Namespace.empty());
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    return listNamespaces(namespace, PageToken.readEverything()).items();
  }

  public Page<Namespace> listNamespaces(Namespace namespace, PageToken pageToken)
      throws NoSuchNamespaceException {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(namespace));
    if (resolvedEntities == null) {
      throw noSuchNamespaceException(namespace);
    }

    List<PolarisEntity> catalogPath = resolvedEntities.getRawFullPath();
    ListEntitiesResult listResult =
        getMetaStoreManager()
            .listEntities(
                getCurrentPolarisContext(),
                PolarisEntity.toCoreList(catalogPath),
                PolarisEntityType.NAMESPACE,
                PolarisEntitySubType.NULL_SUBTYPE,
                pageToken);
    return listResult
        .getPage()
        .map(
            record ->
                PolarisCatalogHelpers.nameAndIdToNamespace(
                    catalogPath, new PolarisEntity.NameAndId(record.getName(), record.getId())));
  }

  @Override
  public void close() throws IOException {
    if (closeableGroup != null) {
      closeableGroup.close();
    }
  }

  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    return listViews(namespace, PageToken.readEverything()).items();
  }

  public Page<TableIdentifier> listViews(Namespace namespace, PageToken pageToken) {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException(
          "Cannot list views for namespace. Namespace does not exist: '%s'", namespace);
    }

    return listTableLike(PolarisEntitySubType.ICEBERG_VIEW, namespace, pageToken);
  }

  @VisibleForTesting
  @Override
  protected ViewOperations newViewOps(TableIdentifier identifier) {
    return new BasePolarisViewOperations(catalogFileIO, identifier);
  }

  /**
   * Override to fix a bug in {@link BaseMetastoreViewCatalog#loadView} where {@link #newViewOps} is
   * called twice, causing redundant metadata fetches. This implementation reuses the same {@link
   * ViewOperations} instance.
   */
  @Override
  public View loadView(TableIdentifier identifier) {
    if (isValidIdentifier(identifier)) {
      ViewOperations ops = newViewOps(identifier);
      if (ops.current() == null) {
        throw notFoundExceptionForTableLikeEntity(identifier, PolarisEntitySubType.ICEBERG_VIEW);
      }
      return new BaseView(ops, ViewUtil.fullViewName(name(), identifier));
    }

    throw new NoSuchViewException("Invalid view identifier: %s", identifier);
  }

  @Override
  public View registerView(TableIdentifier identifier, String metadataFileLocation) {
    Preconditions.checkArgument(
        identifier != null && isValidIdentifier(identifier), "Invalid identifier: %s", identifier);
    Preconditions.checkArgument(
        metadataFileLocation != null && !metadataFileLocation.isEmpty(),
        "Cannot register an empty metadata file location as a view");

    int lastSlashIndex = metadataFileLocation.lastIndexOf("/");
    Preconditions.checkArgument(
        lastSlashIndex != -1,
        "Invalid metadata file location; metadata file location must be absolute and contain a '/': %s",
        metadataFileLocation);

    // Throw an exception if this view already exists in the catalog.
    if (viewExists(identifier)) {
      throw new AlreadyExistsException("View already exists: %s", identifier);
    }

    if (tableExists(identifier)) {
      throw new AlreadyExistsException("Table with same name already exists: %s", identifier);
    }

    String locationDir = metadataFileLocation.substring(0, lastSlashIndex);

    ViewOperations ops = newViewOps(identifier);

    PolarisResolvedPathWrapper resolvedParent =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(identifier.namespace()));
    if (resolvedParent == null) {
      // Illegal state because the namespace should've already been in the static resolution set.
      throw new IllegalStateException(
          String.format("Failed to fetch resolved parent for TableIdentifier '%s'", identifier));
    }
    FileIO fileIO =
        loadFileIOForTableLike(
            identifier,
            Set.of(locationDir),
            resolvedParent,
            new HashMap<>(tableDefaultProperties),
            Set.of(PolarisStorageActions.READ, PolarisStorageActions.LIST));

    InputFile metadataFile = fileIO.newInputFile(metadataFileLocation);
    ViewMetadata metadata = ViewMetadataParser.read(metadataFile);
    ops.commit(null, metadata);

    return new BaseView(ops, ViewUtil.fullViewName(name(), identifier));
  }

  @Override
  public boolean dropView(TableIdentifier identifier) {
    boolean purge =
        realmConfig.getConfig(FeatureConfiguration.PURGE_VIEW_METADATA_ON_DROP, catalogEntity);

    Map<String, String> storageProperties = Map.of();
    ViewMetadata lastMetadata = null;

    if (purge) {
      ViewOperations ops = newViewOps(identifier);
      ViewMetadata currentMetadata = ops.current();
      if (currentMetadata != null && currentMetadata.location() != null) {
        lastMetadata = currentMetadata;

        Map<String, String> clone = new HashMap<>();
        clone.putAll(lastMetadata.properties());
        clone.put(CatalogProperties.FILE_IO_IMPL, ioImplClassName);

        PolarisResolvedPathWrapper resolvedViewEntities =
            resolvedEntityView.getResolvedPath(
                ResolvedPathKey.ofTableLike(identifier), PolarisEntitySubType.ICEBERG_VIEW);
        PolarisResolvedPathWrapper storageHierarchy =
            resolvedViewEntities != null
                ? resolvedViewEntities
                : resolvedEntityView.getResolvedPath(
                    ResolvedPathKey.ofNamespace(identifier.namespace()));
        Optional<PolarisEntity> storageInfoEntity =
            FileIOUtil.findStorageInfoFromHierarchy(storageHierarchy);

        storageInfoEntity.map(PolarisEntity::getInternalPropertiesAsMap).ifPresent(clone::putAll);
        clone.put(PolarisTaskConstants.STORAGE_LOCATION, lastMetadata.location());

        storageProperties = clone;
      }
    }

    DropEntityResult dropEntityResult =
        dropTableLike(PolarisEntitySubType.ICEBERG_VIEW, identifier, storageProperties, purge);
    if (!dropEntityResult.isSuccess()) {
      switch (dropEntityResult.getReturnStatus()) {
        case BaseResult.ReturnStatus.ENTITY_NOT_FOUND:
          return false;

        case BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED:
          LOGGER.debug(
              "Catalog path cannot be resolved for {}, treating as dropped; extraInfo={}",
              identifier,
              dropEntityResult.getExtraInformation());
          return false;

        case BaseResult.ReturnStatus.ENTITY_UNDROPPABLE:
          throw new ForbiddenException(
              "View %s cannot be dropped: %s", identifier, dropEntityResult.getExtraInformation());

        default:
          throw new ServiceFailureException(
              "Failed to drop view %s, status=%s, extraInfo=%s",
              identifier,
              dropEntityResult.getReturnStatus(),
              dropEntityResult.getExtraInformation());
      }
    }

    if (purge && lastMetadata != null && dropEntityResult.getCleanupTaskId() != null) {
      LOGGER.info(
          "Scheduled cleanup task {} for view {}", dropEntityResult.getCleanupTaskId(), identifier);
      taskExecutor.addTaskHandlerContext(dropEntityResult.getCleanupTaskId(), callContext);
    }

    return true;
  }

  @Override
  public void renameView(TableIdentifier from, TableIdentifier to) {
    if (from.equals(to)) {
      return;
    }

    renameTableLike(PolarisEntitySubType.ICEBERG_VIEW, from, to);
  }

  @Override
  public boolean sendNotification(
      TableIdentifier identifier, NotificationRequest notificationRequest) {
    return sendNotificationForTableLike(
        PolarisEntitySubType.ICEBERG_TABLE, identifier, notificationRequest);
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
   * Validates that the specified {@code location} is valid for whatever storage config is found for
   * this TableLike's parent hierarchy.
   */
  private void validateLocationForTableLike(TableIdentifier identifier, String location) {
    PolarisResolvedPathWrapper resolvedStorageEntity =
        resolvedEntityView.getResolvedPath(
            ResolvedPathKey.ofTableLike(identifier), PolarisEntitySubType.ANY_SUBTYPE);
    if (resolvedStorageEntity == null) {
      resolvedStorageEntity =
          resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(identifier.namespace()));
    }
    if (resolvedStorageEntity == null) {
      resolvedStorageEntity =
          resolvedEntityView.getPassthroughResolvedPath(
              ResolvedPathKey.ofNamespace(identifier.namespace()));
    }

    validateLocationForTableLike(identifier, location, resolvedStorageEntity);
  }

  /**
   * Validates that the specified {@code location} is valid for whatever storage config is found for
   * this TableLike's parent hierarchy.
   */
  private void validateLocationForTableLike(
      TableIdentifier identifier,
      String location,
      PolarisResolvedPathWrapper resolvedStorageEntity) {
    CatalogUtils.validateLocationsForTableLike(
        realmConfig, identifier, Set.of(location), resolvedStorageEntity);
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

  /** Checks whether the location of a namespace is valid given its parent */
  private void validateNamespaceLocation(
      NamespaceEntity namespace, PolarisResolvedPathWrapper resolvedParent) {
    StorageLocation namespaceLocation =
        StorageLocation.of(
            StorageLocation.ensureTrailingSlash(
                resolveNamespaceLocation(namespace.asNamespace(), namespace.getPropertiesAsMap())));
    PolarisEntity parent = resolvedParent.getResolvedLeafEntity().getEntity();
    Preconditions.checkArgument(
        parent.getType().equals(PolarisEntityType.CATALOG)
            || parent.getType().equals(PolarisEntityType.NAMESPACE),
        "Invalid parent type");
    if (parent.getType().equals(PolarisEntityType.CATALOG)) {
      CatalogEntity parentEntity = CatalogEntity.of(parent);
      LOGGER.debug(
          "Validating namespace {} given parent catalog {}",
          namespace.getName(),
          parentEntity.getName());
      var storageConfigInfo = parentEntity.getStorageConfigurationInfo();
      if (storageConfigInfo == null) {
        throw new IllegalArgumentException(
            "Cannot create namespace without a parent storage configuration");
      }
      List<StorageLocation> defaultLocations =
          parentEntity.getStorageConfigurationInfo().getAllowedLocations().stream()
              .filter(java.util.Objects::nonNull)
              .map(
                  l ->
                      StorageLocation.ensureTrailingSlash(
                          StorageLocation.ensureTrailingSlash(l) + namespace.getName()))
              .map(StorageLocation::of)
              .toList();
      if (!defaultLocations.contains(namespaceLocation)) {
        throw new IllegalArgumentException(
            "Namespace "
                + namespace.getName()
                + " has a custom location, "
                + "which is not enabled. Expected a location in: ["
                + String.join(
                    ", ", defaultLocations.stream().map(StorageLocation::toString).toList())
                + "]. Got location: "
                + namespaceLocation
                + "]");
      }
    } else if (parent.getType().equals(PolarisEntityType.NAMESPACE)) {
      NamespaceEntity parentEntity = NamespaceEntity.of(parent);
      LOGGER.debug(
          "Validating namespace {} given parent namespace {}",
          namespace.getName(),
          parentEntity.getName());
      String parentLocation =
          resolveNamespaceLocation(parentEntity.asNamespace(), parentEntity.getPropertiesAsMap());
      StorageLocation defaultLocation =
          StorageLocation.of(
              StorageLocation.ensureTrailingSlash(
                  StorageLocation.ensureTrailingSlash(parentLocation) + namespace.getName()));
      if (!defaultLocation.equals(namespaceLocation)) {
        throw new IllegalArgumentException(
            "Namespace "
                + namespace.getName()
                + " has a custom location, "
                + "which is not enabled. Expected location: ["
                + defaultLocation
                + "]. Got location: ["
                + namespaceLocation
                + "]");
      }
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
  List<PolarisEntity> resolveOptionalPaths(List<ResolvedPathKey> keys, String catalogName) {
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

  private class PolarisIcebergCatalogTableBuilder
      extends BaseMetastoreViewCatalog.BaseMetastoreViewCatalogTableBuilder {
    private final TableIdentifier identifier;

    public PolarisIcebergCatalogTableBuilder(TableIdentifier identifier, Schema schema) {
      super(identifier, schema);
      this.identifier = identifier;
    }

    @Override
    public TableBuilder withLocation(String newLocation) {
      return super.withLocation(transformTableLikeLocation(identifier, newLocation));
    }
  }

  private class PolarisIcebergCatalogViewBuilder extends BaseMetastoreViewCatalog.BaseViewBuilder {
    private final TableIdentifier identifier;

    public PolarisIcebergCatalogViewBuilder(TableIdentifier identifier) {
      super(identifier);
      withProperties(
          PropertyUtil.propertiesWithPrefix(
              LocalIcebergCatalog.this.properties(), "table-default."));
      this.identifier = identifier;
    }

    @Override
    public ViewBuilder withLocation(String newLocation) {
      return super.withLocation(transformTableLikeLocation(identifier, newLocation));
    }
  }

  /**
   * An implementation of {@link TableOperations} that integrates with {@link LocalIcebergCatalog}.
   * Much of this code was originally copied from {@link
   * org.apache.iceberg.BaseMetastoreTableOperations}. CODE_COPIED_TO_POLARIS From Apache Iceberg
   * Version: 1.8
   */
  @VisibleForTesting
  public class BasePolarisTableOperations extends PolarisOperationsBase<TableMetadata>
      implements TableOperations {
    private final TableIdentifier tableIdentifier;
    private final String fullTableName;
    private final boolean makeMetadataCurrentOnCommit;

    private FileIO tableFileIO;

    BasePolarisTableOperations(
        FileIO defaultFileIO,
        TableIdentifier tableIdentifier,
        boolean makeMetadataCurrentOnCommit) {
      LOGGER.debug("new BasePolarisTableOperations for {}", tableIdentifier);
      this.tableIdentifier = tableIdentifier;
      this.fullTableName = fullTableName(catalogName, tableIdentifier);
      this.tableFileIO = defaultFileIO;
      this.makeMetadataCurrentOnCommit = makeMetadataCurrentOnCommit;
    }

    @Override
    public TableMetadata current() {
      if (shouldRefresh) {
        return refresh();
      }
      return currentMetadata;
    }

    @Override
    public TableMetadata refresh() {
      boolean currentMetadataWasAvailable = currentMetadata != null;
      try {
        doRefresh();
      } catch (NoSuchTableException e) {
        if (currentMetadataWasAvailable) {
          LOGGER.warn(
              "Could not find the table during refresh, setting current metadata to null", e);
          shouldRefresh = true;
        }

        currentMetadata = null;
        currentMetadataLocation = null;
        version = -1;
        throw e;
      }
      return current();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      // if the metadata is already out of date, reject it
      if (base != current()) {
        if (base != null) {
          throw new CommitFailedException("Cannot commit: stale table metadata");
        } else {
          // when current is non-null, the table exists. but when base is null, the commit is trying
          // to create the table
          throw alreadyExistsExceptionForTableLikeEntity(
              fullTableName, PolarisEntitySubType.ICEBERG_TABLE);
        }
      }
      // if the metadata is not changed, return early
      if (base == metadata) {
        LOGGER.info("Nothing to commit.");
        return;
      }

      long start = System.currentTimeMillis();
      doCommit(base, metadata);
      CatalogUtil.deleteRemovedMetadataFiles(io(), base, metadata);
      requestRefresh();

      LOGGER.info(
          "Successfully committed to table {} in {} ms",
          fullTableName,
          System.currentTimeMillis() - start);
    }

    @Override
    public FileIO io() {
      return tableFileIO;
    }

    @Override
    public String metadataFileLocation(String filename) {
      return metadataFileLocation(current(), filename);
    }

    @Override
    public LocationProvider locationProvider() {
      return LocationProviders.locationsFor(current().location(), current().properties());
    }

    public void doRefresh() {
      LOGGER.debug("doRefresh for tableIdentifier {}", tableIdentifier);
      // While doing refresh/commit protocols, we must fetch the fresh "passthrough" resolved
      // table entity instead of the statically-resolved authz resolution set.
      PolarisResolvedPathWrapper resolvedEntities =
          resolvedEntityView.getPassthroughResolvedPath(
              ResolvedPathKey.ofTableLike(tableIdentifier), PolarisEntitySubType.ICEBERG_TABLE);
      IcebergTableLikeEntity entity = null;

      if (resolvedEntities != null) {
        entity = IcebergTableLikeEntity.of(resolvedEntities.getRawLeafEntity());
        if (!tableIdentifier.equals(entity.getTableIdentifier())) {
          LOGGER
              .atError()
              .addKeyValue("entity.getTableIdentifier()", entity.getTableIdentifier())
              .addKeyValue("tableIdentifier", tableIdentifier)
              .log("Stored table identifier mismatches requested identifier");
        }
      }

      String latestLocation = entity != null ? entity.getMetadataLocation() : null;
      LOGGER.debug("Refreshing latestLocation: {}", latestLocation);
      if (latestLocation == null) {
        disableRefresh();
      } else {
        if (polarisEventDispatcher.hasListeners(PolarisEventType.BEFORE_REFRESH_TABLE)) {
          polarisEventDispatcher.dispatch(
              new PolarisEvent(
                  PolarisEventType.BEFORE_REFRESH_TABLE,
                  eventMetadataFactory.create(),
                  new EventAttributeMap()
                      .put(EventAttributes.CATALOG_NAME, catalogName)
                      .put(EventAttributes.TABLE_IDENTIFIER, tableIdentifier)));
        }
        refreshFromMetadataLocation(
            latestLocation,
            SHOULD_RETRY_REFRESH_PREDICATE,
            getMaxMetadataRefreshRetries(),
            metadataLocation -> {
              String latestLocationDir =
                  latestLocation.substring(0, latestLocation.lastIndexOf('/'));
              // TODO: Once we have the "current" table properties pulled into the resolvedEntity
              // then we should use the actual current table properties for IO refresh here
              // instead of the general tableDefaultProperties.
              FileIO fileIO =
                  loadFileIOForTableLike(
                      tableIdentifier,
                      Set.of(latestLocationDir),
                      resolvedEntities,
                      new HashMap<>(tableDefaultProperties),
                      Set.of(PolarisStorageActions.READ, PolarisStorageActions.LIST));
              return TableMetadataParser.read(fileIO, metadataLocation);
            });
        if (polarisEventDispatcher.hasListeners(PolarisEventType.AFTER_REFRESH_TABLE)) {
          polarisEventDispatcher.dispatch(
              new PolarisEvent(
                  PolarisEventType.AFTER_REFRESH_TABLE,
                  eventMetadataFactory.create(),
                  new EventAttributeMap()
                      .put(EventAttributes.CATALOG_NAME, catalogName)
                      .put(EventAttributes.TABLE_IDENTIFIER, tableIdentifier)));
        }
      }
    }

    public void doCommit(TableMetadata base, TableMetadata metadata) {
      LOGGER.debug(
          "doCommit for table {} with metadataBefore {}, metadataAfter {}",
          tableIdentifier,
          base,
          metadata);
      // TODO: Maybe avoid writing metadata if there's definitely a transaction conflict
      if (null == base && !namespaceExists(tableIdentifier.namespace())) {
        throw new NoSuchNamespaceException(
            "Cannot create table '%s'. Namespace does not exist: '%s'",
            tableIdentifier, tableIdentifier.namespace());
      }

      PolarisResolvedPathWrapper resolvedTableEntities =
          resolvedEntityView.getPassthroughResolvedPath(
              ResolvedPathKey.ofTableLike(tableIdentifier), PolarisEntitySubType.ICEBERG_TABLE);

      // Fetch credentials for the resolved entity. The entity could be the table itself (if it has
      // already been stored and credentials have been configured directly) or it could be the
      // table's namespace or catalog.
      PolarisResolvedPathWrapper resolvedStorageEntity =
          resolvedTableEntities == null
              ? resolvedEntityView.getResolvedPath(
                  ResolvedPathKey.ofNamespace(tableIdentifier.namespace()))
              : resolvedTableEntities;

      Set<String> requestedLocations = StorageUtil.getLocationsUsedByTable(metadata);

      List<PolarisEntity> resolvedNamespace =
          resolvedTableEntities == null
              ? resolvedEntityView
                  .getResolvedPath(ResolvedPathKey.ofNamespace(tableIdentifier.namespace()))
                  .getRawFullPath()
              : resolvedTableEntities.getRawParentPath();

      if (base == null || requestedTableLocationsChanged(base, metadata)) {
        // If location is changing then we must validate that the requested location is valid
        // for the storage configuration inherited under this entity's path.
        CatalogUtils.validateLocationsForTableLike(
            realmConfig, tableIdentifier, requestedLocations, resolvedStorageEntity);
        // also validate that the table location doesn't overlap an existing table
        requestedLocations.forEach(
            location ->
                validateNoLocationOverlap(
                    catalogEntity,
                    tableIdentifier,
                    resolvedNamespace,
                    location,
                    resolvedStorageEntity.getRawLeafEntity()));
        // and that the metadata file points to a location within the table's directory structure
        validateMetadataFileInTableDir(
            tableIdentifier, metadata.location(), nextMetadataFileLocation(metadata));
      }

      tableFileIO =
          loadFileIOForTableLike(
              tableIdentifier,
              requestedLocations,
              resolvedStorageEntity,
              new HashMap<>(metadata.properties()),
              Set.of(
                  PolarisStorageActions.READ,
                  PolarisStorageActions.WRITE,
                  PolarisStorageActions.LIST));

      String newLocation = writeNewMetadataIfRequired(base == null, metadata);
      String oldLocation = base == null ? null : base.metadataFileLocation();

      // TODO: Consider using the entity from doRefresh() directly to do the conflict detection
      // instead of a two-layer CAS (checking metadataLocation to detect concurrent modification
      // between doRefresh() and doCommit(), and then updateEntityPropertiesIfNotChanged to detect
      // concurrent
      // modification between our checking of unchanged metadataLocation here and actual
      // persistence-layer commit).
      PolarisResolvedPathWrapper resolvedPath =
          resolvedEntityView.getPassthroughResolvedPath(
              ResolvedPathKey.ofTableLike(tableIdentifier), PolarisEntitySubType.ANY_SUBTYPE);
      if (resolvedPath != null && resolvedPath.getRawLeafEntity() != null) {
        var subType = resolvedPath.getRawLeafEntity().getSubType();
        if (subType != PolarisEntitySubType.ICEBERG_TABLE) {
          throw alreadyExistsExceptionWithSameNameForTableLikeEntity(tableIdentifier, subType);
        }
      }
      Map<String, String> storedProperties = buildTableMetadataPropertiesMap(metadata);
      IcebergTableLikeEntity entity =
          IcebergTableLikeEntity.of(resolvedPath == null ? null : resolvedPath.getRawLeafEntity());
      String existingLocation;
      if (null == entity) {
        existingLocation = null;
        entity =
            new IcebergTableLikeEntity.Builder(
                    PolarisEntitySubType.ICEBERG_TABLE,
                    tableIdentifier,
                    Map.of(),
                    storedProperties,
                    newLocation)
                .setCatalogId(getCatalogId())
                .setBaseLocation(metadata.location())
                .setId(
                    getMetaStoreManager().generateNewEntityId(getCurrentPolarisContext()).getId())
                .build();
      } else {
        existingLocation = entity.getMetadataLocation();
        entity =
            new IcebergTableLikeEntity.Builder(entity)
                .setInternalProperties(storedProperties)
                .setBaseLocation(metadata.location())
                .setMetadataLocation(newLocation)
                .build();
      }
      if (!Objects.equal(existingLocation, oldLocation)) {
        if (null == base) {
          throw alreadyExistsExceptionForTableLikeEntity(
              fullTableName, PolarisEntitySubType.ICEBERG_TABLE);
        }

        if (null == existingLocation) {
          throw notFoundExceptionForTableLikeEntity(
              fullTableName, PolarisEntitySubType.ICEBERG_TABLE);
        }

        throw new CommitFailedException(
            "Cannot commit to table %s metadata location from %s to %s "
                + "because it has been concurrently modified to %s",
            tableIdentifier, oldLocation, newLocation, existingLocation);
      }

      // We diverge from `BaseMetastoreTableOperations` in the below code block
      if (makeMetadataCurrentOnCommit) {
        currentMetadata =
            TableMetadata.buildFrom(metadata)
                .withMetadataLocation(newLocation)
                .discardChanges()
                .build();
        currentMetadataLocation = newLocation;
      }

      if (null == existingLocation) {
        createTableLike(tableIdentifier, entity);
      } else {
        updateTableLike(tableIdentifier, entity);
      }
    }

    private boolean requestedTableLocationsChanged(TableMetadata base, TableMetadata metadata) {
      return !metadata.location().equals(base.location())
          || !Objects.equal(
              base.properties().get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY),
              metadata
                  .properties()
                  .get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY))
          || !Objects.equal(
              base.properties()
                  .get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY),
              metadata
                  .properties()
                  .get(IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY));
    }

    private String nextMetadataFileLocation(TableMetadata metadata) {
      return metadata.metadataFileLocation() != null
          ? metadata.metadataFileLocation()
          : metadataFileLocation(metadata, "metadata.json");
    }

    @Override
    public TableOperations temp(TableMetadata uncommittedMetadata) {
      return new TableOperations() {
        @Override
        public TableMetadata current() {
          return uncommittedMetadata;
        }

        @Override
        public TableMetadata refresh() {
          throw new UnsupportedOperationException(
              "Cannot call refresh on temporary table operations");
        }

        @Override
        public void commit(TableMetadata base, TableMetadata metadata) {
          throw new UnsupportedOperationException(
              "Cannot call commit on temporary table operations");
        }

        @Override
        public String metadataFileLocation(String fileName) {
          return BasePolarisTableOperations.this.metadataFileLocation(
              uncommittedMetadata, fileName);
        }

        @Override
        public LocationProvider locationProvider() {
          return LocationProviders.locationsFor(
              uncommittedMetadata.location(), uncommittedMetadata.properties());
        }

        @Override
        public FileIO io() {
          return BasePolarisTableOperations.this.io();
        }

        @Override
        public EncryptionManager encryption() {
          return BasePolarisTableOperations.this.encryption();
        }

        @Override
        public long newSnapshotId() {
          return BasePolarisTableOperations.this.newSnapshotId();
        }
      };
    }

    protected String writeNewMetadataIfRequired(boolean newTable, TableMetadata metadata) {
      return newTable && metadata.metadataFileLocation() != null
          ? metadata.metadataFileLocation()
          : writeNewMetadata(metadata, version + 1);
    }

    protected String writeNewMetadata(TableMetadata metadata, int newVersion) {
      String newTableMetadataFilePath = newTableMetadataFilePath(metadata, newVersion);
      OutputFile newMetadataLocation = io().newOutputFile(newTableMetadataFilePath);

      // write the new metadata
      // use overwrite to avoid negative caching in S3. this is safe because the metadata location
      // is
      // always unique because it includes a UUID.
      TableMetadataParser.overwrite(metadata, newMetadataLocation);

      return newMetadataLocation.location();
    }

    private String metadataFileLocation(TableMetadata metadata, String filename) {
      String metadataLocation = metadata.properties().get(TableProperties.WRITE_METADATA_LOCATION);

      if (metadataLocation != null) {
        return String.format("%s/%s", LocationUtil.stripTrailingSlash(metadataLocation), filename);
      } else {
        return String.format("%s/%s/%s", metadata.location(), METADATA_FOLDER_NAME, filename);
      }
    }

    private String newTableMetadataFilePath(TableMetadata meta, int newVersion) {
      String codecName =
          meta.property(
              TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
      String fileExtension = TableMetadataParser.getFileExtension(codecName);
      return metadataFileLocation(
          meta,
          String.format(Locale.ROOT, "%05d-%s%s", newVersion, UUID.randomUUID(), fileExtension));
    }
  }

  private static Map<String, String> buildTableMetadataPropertiesMap(TableMetadata metadata) {
    Map<String, String> storedProperties = new HashMap<>();
    // Location specific properties
    storedProperties.put(IcebergTableLikeEntity.LOCATION, metadata.location());
    if (metadata.properties().containsKey(TableProperties.WRITE_DATA_LOCATION)) {
      storedProperties.put(
          IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY,
          metadata.properties().get(TableProperties.WRITE_DATA_LOCATION));
    }
    if (metadata.properties().containsKey(TableProperties.WRITE_METADATA_LOCATION)) {
      storedProperties.put(
          IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY,
          metadata.properties().get(TableProperties.WRITE_METADATA_LOCATION));
    }
    storedProperties.put(
        IcebergTableLikeEntity.FORMAT_VERSION, String.valueOf(metadata.formatVersion()));
    storedProperties.put(IcebergTableLikeEntity.TABLE_UUID, metadata.uuid());
    storedProperties.put(
        IcebergTableLikeEntity.CURRENT_SCHEMA_ID, String.valueOf(metadata.currentSchemaId()));
    if (metadata.currentSnapshot() != null) {
      storedProperties.put(
          IcebergTableLikeEntity.CURRENT_SNAPSHOT_ID,
          String.valueOf(metadata.currentSnapshot().snapshotId()));
    }
    storedProperties.put(
        IcebergTableLikeEntity.LAST_COLUMN_ID, String.valueOf(metadata.lastColumnId()));
    storedProperties.put(IcebergTableLikeEntity.NEXT_ROW_ID, String.valueOf(metadata.nextRowId()));
    storedProperties.put(
        IcebergTableLikeEntity.LAST_SEQUENCE_NUMBER, String.valueOf(metadata.lastSequenceNumber()));
    storedProperties.put(
        IcebergTableLikeEntity.LAST_UPDATED_MILLIS, String.valueOf(metadata.lastUpdatedMillis()));
    if (metadata.sortOrder() != null) {
      storedProperties.put(
          IcebergTableLikeEntity.DEFAULT_SORT_ORDER_ID,
          String.valueOf(metadata.defaultSortOrderId()));
    }
    if (metadata.spec() != null) {
      storedProperties.put(
          IcebergTableLikeEntity.DEFAULT_SPEC_ID, String.valueOf(metadata.defaultSpecId()));
      storedProperties.put(
          IcebergTableLikeEntity.LAST_PARTITION_ID,
          String.valueOf(metadata.lastAssignedPartitionId()));
    }
    return storedProperties;
  }

  /**
   * An implementation of {@link ViewOperations} that integrates with {@link LocalIcebergCatalog}.
   * Much of this code was originally copied from {@link
   * org.apache.iceberg.view.BaseViewOperations}. CODE_COPIED_TO_POLARIS From Apache Iceberg
   * Version: 1.8
   */
  private class BasePolarisViewOperations extends PolarisOperationsBase<ViewMetadata>
      implements ViewOperations {
    private final TableIdentifier identifier;
    private final String fullViewName;
    private FileIO viewFileIO;

    BasePolarisViewOperations(FileIO defaultFileIO, TableIdentifier identifier) {
      this.viewFileIO = defaultFileIO;
      this.identifier = identifier;
      this.fullViewName = ViewUtil.fullViewName(catalogName, identifier);
    }

    @Override
    public ViewMetadata current() {
      if (shouldRefresh) {
        return refresh();
      }

      return currentMetadata;
    }

    @Override
    public ViewMetadata refresh() {
      boolean currentMetadataWasAvailable = currentMetadata != null;
      try {
        doRefresh();
      } catch (NoSuchViewException e) {
        if (currentMetadataWasAvailable) {
          LOGGER.warn(
              "Could not find the view during refresh, setting current metadata to null", e);
          shouldRefresh = true;
        }

        currentMetadata = null;
        currentMetadataLocation = null;
        version = -1;
        throw e;
      }

      return current();
    }

    @Override
    @SuppressWarnings("ImmutablesReferenceEquality")
    public void commit(ViewMetadata base, ViewMetadata metadata) {
      // if the metadata is already out of date, reject it
      if (base != current()) {
        if (base != null) {
          throw new CommitFailedException("Cannot commit: stale view metadata");
        } else {
          // when current is non-null, the view exists. but when base is null, the commit is trying
          // to create the view
          throw alreadyExistsExceptionForTableLikeEntity(
              identifier, PolarisEntitySubType.ICEBERG_VIEW);
        }
      }

      // if the metadata is not changed, return early
      if (base == metadata) {
        LOGGER.info("Nothing to commit.");
        return;
      }

      long start = System.currentTimeMillis();
      doCommit(base, metadata);
      requestRefresh();

      LOGGER.info(
          "Successfully committed to view {} in {} ms",
          viewName(),
          System.currentTimeMillis() - start);
    }

    public void doRefresh() {
      PolarisResolvedPathWrapper resolvedEntities =
          resolvedEntityView.getPassthroughResolvedPath(
              ResolvedPathKey.ofTableLike(identifier), PolarisEntitySubType.ICEBERG_VIEW);
      IcebergTableLikeEntity entity = null;

      if (resolvedEntities != null) {
        entity = IcebergTableLikeEntity.of(resolvedEntities.getRawLeafEntity());
        if (!identifier.equals(entity.getTableIdentifier())) {
          LOGGER
              .atError()
              .addKeyValue("entity.getTableIdentifier()", entity.getTableIdentifier())
              .addKeyValue("identifier", identifier)
              .log("Stored view identifier mismatches requested identifier");
        }
      }

      String latestLocation = entity != null ? entity.getMetadataLocation() : null;
      LOGGER.debug("Refreshing view latestLocation: {}", latestLocation);
      if (latestLocation == null) {
        disableRefresh();
      } else {
        if (polarisEventDispatcher.hasListeners(PolarisEventType.BEFORE_REFRESH_VIEW)) {
          polarisEventDispatcher.dispatch(
              new PolarisEvent(
                  PolarisEventType.BEFORE_REFRESH_VIEW,
                  eventMetadataFactory.create(),
                  new EventAttributeMap()
                      .put(EventAttributes.CATALOG_NAME, catalogName)
                      .put(EventAttributes.VIEW_IDENTIFIER, identifier)));
        }
        refreshFromMetadataLocation(
            latestLocation,
            SHOULD_RETRY_REFRESH_PREDICATE,
            getMaxMetadataRefreshRetries(),
            metadataLocation -> {
              String latestLocationDir =
                  latestLocation.substring(0, latestLocation.lastIndexOf('/'));

              // TODO: Once we have the "current" table properties pulled into the resolvedEntity
              // then we should use the actual current table properties for IO refresh here
              // instead of the general tableDefaultProperties.
              FileIO fileIO =
                  loadFileIOForTableLike(
                      identifier,
                      Set.of(latestLocationDir),
                      resolvedEntities,
                      new HashMap<>(tableDefaultProperties),
                      Set.of(PolarisStorageActions.READ, PolarisStorageActions.LIST));

              return ViewMetadataParser.read(fileIO.newInputFile(metadataLocation));
            });
        if (polarisEventDispatcher.hasListeners(PolarisEventType.AFTER_REFRESH_VIEW)) {
          polarisEventDispatcher.dispatch(
              new PolarisEvent(
                  PolarisEventType.AFTER_REFRESH_VIEW,
                  eventMetadataFactory.create(),
                  new EventAttributeMap()
                      .put(EventAttributes.CATALOG_NAME, catalogName)
                      .put(EventAttributes.VIEW_IDENTIFIER, identifier)));
        }
      }
    }

    public void doCommit(ViewMetadata base, ViewMetadata metadata) {
      // TODO: Maybe avoid writing metadata if there's definitely a transaction conflict
      LOGGER.debug(
          "doCommit for view {} with metadataBefore {}, metadataAfter {}",
          identifier,
          base,
          metadata);
      if (null == base && !namespaceExists(identifier.namespace())) {
        throw new NoSuchNamespaceException(
            "Cannot create view '%s'. Namespace does not exist: '%s'",
            identifier, identifier.namespace());
      }

      PolarisResolvedPathWrapper resolvedTable =
          resolvedEntityView.getPassthroughResolvedPath(
              ResolvedPathKey.ofTableLike(identifier), PolarisEntitySubType.ICEBERG_TABLE);
      if (resolvedTable != null) {
        throw alreadyExistsExceptionWithSameNameForTableLikeEntity(
            identifier, PolarisEntitySubType.ICEBERG_TABLE);
      }

      PolarisResolvedPathWrapper resolvedEntities =
          resolvedEntityView.getPassthroughResolvedPath(
              ResolvedPathKey.ofTableLike(identifier), PolarisEntitySubType.ICEBERG_VIEW);

      // Fetch credentials for the resolved entity. The entity could be the view itself (if it has
      // already been stored and credentials have been configured directly) or it could be the
      // table's namespace or catalog.
      PolarisResolvedPathWrapper resolvedStorageEntity =
          resolvedEntities == null
              ? resolvedEntityView.getResolvedPath(
                  ResolvedPathKey.ofNamespace(identifier.namespace()))
              : resolvedEntities;

      List<PolarisEntity> resolvedNamespace =
          resolvedEntities == null
              ? resolvedEntityView
                  .getResolvedPath(ResolvedPathKey.ofNamespace(identifier.namespace()))
                  .getRawFullPath()
              : resolvedEntities.getRawParentPath();
      if (base == null || !metadata.location().equals(base.location())) {
        // If location is changing then we must validate that the requested location is valid
        // for the storage configuration inherited under this entity's path.
        validateLocationForTableLike(identifier, metadata.location(), resolvedStorageEntity);
        validateNoLocationOverlap(
            catalogEntity,
            identifier,
            resolvedNamespace,
            metadata.location(),
            resolvedStorageEntity.getRawLeafEntity());
      }

      Map<String, String> tableProperties = new HashMap<>(metadata.properties());

      viewFileIO =
          loadFileIOForTableLike(
              identifier,
              StorageUtil.getLocationsUsedByTable(metadata),
              resolvedStorageEntity,
              tableProperties,
              Set.of(PolarisStorageActions.READ, PolarisStorageActions.WRITE));

      String newLocation = writeNewMetadataIfRequired(metadata);
      String oldLocation = base == null ? null : currentMetadataLocation;

      IcebergTableLikeEntity entity =
          IcebergTableLikeEntity.of(
              resolvedEntities == null ? null : resolvedEntities.getRawLeafEntity());
      String existingLocation;
      if (null == entity) {
        existingLocation = null;
        entity =
            new IcebergTableLikeEntity.Builder(
                    PolarisEntitySubType.ICEBERG_VIEW, identifier, newLocation)
                .setCatalogId(getCatalogId())
                .setId(
                    getMetaStoreManager().generateNewEntityId(getCurrentPolarisContext()).getId())
                .build();
      } else {
        existingLocation = entity.getMetadataLocation();
        entity =
            new IcebergTableLikeEntity.Builder(entity).setMetadataLocation(newLocation).build();
      }
      if (!Objects.equal(existingLocation, oldLocation)) {
        if (null == base) {
          throw alreadyExistsExceptionForTableLikeEntity(
              identifier, PolarisEntitySubType.ICEBERG_VIEW);
        }

        if (null == existingLocation) {
          throw notFoundExceptionForTableLikeEntity(identifier, PolarisEntitySubType.ICEBERG_VIEW);
        }

        throw new CommitFailedException(
            "Cannot commit to view %s metadata location from %s to %s "
                + "because it has been concurrently modified to %s",
            identifier, oldLocation, newLocation, existingLocation);
      }
      if (null == existingLocation) {
        createTableLike(identifier, entity);
      } else {
        updateTableLike(identifier, entity);
      }
    }

    protected String writeNewMetadataIfRequired(ViewMetadata metadata) {
      return null != metadata.metadataFileLocation()
          ? metadata.metadataFileLocation()
          : writeNewMetadata(metadata, version + 1);
    }

    private String writeNewMetadata(ViewMetadata metadata, int newVersion) {
      String newMetadataFilePath = newMetadataFilePath(metadata, newVersion);
      OutputFile newMetadataLocation = io().newOutputFile(newMetadataFilePath);

      // write the new metadata
      // use overwrite to avoid negative caching in S3. this is safe because the metadata location
      // is
      // always unique because it includes a UUID.
      ViewMetadataParser.overwrite(metadata, newMetadataLocation);

      return newMetadataLocation.location();
    }

    private String newMetadataFilePath(ViewMetadata metadata, int newVersion) {
      String codecName =
          metadata
              .properties()
              .getOrDefault(
                  ViewProperties.METADATA_COMPRESSION, ViewProperties.METADATA_COMPRESSION_DEFAULT);
      String fileExtension = TableMetadataParser.getFileExtension(codecName);
      return metadataFileLocation(
          metadata,
          String.format(Locale.ROOT, "%05d-%s%s", newVersion, UUID.randomUUID(), fileExtension));
    }

    private String metadataFileLocation(ViewMetadata metadata, String filename) {
      String metadataLocation = metadata.properties().get(ViewProperties.WRITE_METADATA_LOCATION);
      if (metadataLocation != null) {
        return String.format("%s/%s", LocationUtil.stripTrailingSlash(metadataLocation), filename);
      } else {
        return String.format(
            "%s/%s/%s",
            LocationUtil.stripTrailingSlash(metadata.location()), METADATA_FOLDER_NAME, filename);
      }
    }

    public FileIO io() {
      return viewFileIO;
    }

    protected String viewName() {
      return fullViewName;
    }
  }

  /**
   * An ABC for {@link BasePolarisTableOperations} and {@link BasePolarisViewOperations}. Much of
   * this code was originally copied from {@link org.apache.iceberg.BaseMetastoreTableOperations}.
   * CODE_COPIED_TO_POLARIS From Apache Iceberg Version: 1.8
   */
  private abstract static class PolarisOperationsBase<T> {

    protected static final String METADATA_FOLDER_NAME = "metadata";

    protected T currentMetadata = null;
    protected String currentMetadataLocation = null;
    protected boolean shouldRefresh = true;
    protected int version = -1;

    protected void requestRefresh() {
      this.shouldRefresh = true;
    }

    protected void disableRefresh() {
      this.shouldRefresh = false;
    }

    /**
     * Parse the version from table/view metadata file name.
     *
     * @param metadataLocation table/view metadata file location
     * @return version of the table/view metadata file in success case and -1 if the version is not
     *     parsable (as a sign that the metadata is not part of this catalog)
     */
    protected int parseVersion(String metadataLocation) {
      int versionStart =
          metadataLocation.lastIndexOf('/') + 1; // if '/' isn't found, this will be 0
      int versionEnd = metadataLocation.indexOf('-', versionStart);
      if (versionEnd < 0) {
        // found filesystem object's metadata
        return -1;
      }

      try {
        return Integer.parseInt(metadataLocation.substring(versionStart, versionEnd));
      } catch (NumberFormatException e) {
        LOGGER.warn("Unable to parse version from metadata location: {}", metadataLocation, e);
        return -1;
      }
    }

    protected void refreshFromMetadataLocation(
        String newLocation,
        Predicate<Exception> shouldRetry,
        int numRetries,
        Function<String, T> metadataLoader) {
      // use null-safe equality check because new tables have a null metadata location
      if (!Objects.equal(currentMetadataLocation, newLocation)) {
        LOGGER.info("Refreshing table metadata from new version: {}", newLocation);

        AtomicReference<T> newMetadata = new AtomicReference<>();
        Tasks.foreach(newLocation)
            .retry(numRetries)
            .exponentialBackoff(100, 5000, 600000, 4.0 /* 100, 400, 1600, ... */)
            .throwFailureWhenFinished()
            .stopRetryOn(NotFoundException.class) // overridden if shouldRetry is non-null
            .shouldRetryTest(shouldRetry)
            .run(metadataLocation -> newMetadata.set(metadataLoader.apply(metadataLocation)));

        if (newMetadata.get() instanceof TableMetadata tableMetadata) {
          if (currentMetadata instanceof TableMetadata currentTableMetadata) {
            String newUUID = tableMetadata.uuid();
            if (currentMetadata != null && currentTableMetadata.uuid() != null && newUUID != null) {
              Preconditions.checkState(
                  newUUID.equals(currentTableMetadata.uuid()),
                  "Table UUID does not match: current=%s != refreshed=%s",
                  currentTableMetadata.uuid(),
                  newUUID);
            }
          }
        }

        this.currentMetadata = newMetadata.get();
        this.currentMetadataLocation = newLocation;
        this.version = parseVersion(newLocation);
      }
      this.shouldRefresh = false;
    }
  }

  private void validateMetadataFileInTableDir(TableIdentifier identifier, TableMetadata metadata) {
    validateMetadataFileInTableDir(
        identifier, metadata.location(), metadata.metadataFileLocation());
  }

  private void validateMetadataFileInTableDir(
      TableIdentifier identifier, String tableLocation, String metadataLocation) {
    boolean allowEscape = realmConfig.getConfig(FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION);
    if (!allowEscape
        && !realmConfig.getConfig(FeatureConfiguration.ALLOW_EXTERNAL_METADATA_FILE_LOCATION)) {
      LOGGER.debug(
          "Validating base location {} for table {} in metadata file {}",
          tableLocation,
          identifier,
          metadataLocation);
      StorageLocation metadataFileLocation = StorageLocation.of(metadataLocation);
      StorageLocation baseLocation = StorageLocation.of(tableLocation);
      if (!metadataFileLocation.isChildOf(baseLocation)) {
        throw new BadRequestException(
            "Metadata location %s is not allowed outside of table location %s",
            metadataLocation, tableLocation);
      }
    }
  }

  private FileIO loadFileIOForTableLike(
      TableIdentifier identifier,
      Set<String> readLocations,
      PolarisResolvedPathWrapper resolvedStorageEntity,
      Map<String, String> tableProperties,
      Set<PolarisStorageActions> storageActions) {
    StorageAccessConfig cfg =
        storageAccessConfigProvider.getStorageAccessConfig(
            identifier, readLocations, storageActions, Optional.empty(), resolvedStorageEntity);
    // Reload fileIO based on table specific context
    VendedClientStorageAccess clientView =
        new VendedClientStorageAccess(cfg.credentials(), cfg.extraProperties(), cfg.expiresAt());
    Map<String, String> internalProps = new LinkedHashMap<>(tableProperties);
    internalProps.putAll(cfg.internalProperties());
    VendedServerStorageAccess access =
        new VendedServerStorageAccess(clientView, internalProps, ioImplClassName);
    FileIO fileIO = storageIoProvider.fileIoFor(access);
    // ensure the new fileIO is closed when the catalog is closed
    closeableGroup.addCloseable(fileIO);
    return fileIO;
  }

  private PolarisCallContext getCurrentPolarisContext() {
    return callContext.getPolarisCallContext();
  }

  private DurableManager getMetaStoreManager() {
    return metaStoreManager;
  }

  @VisibleForTesting
  long getCatalogId() {
    // TODO: Properly handle initialization
    if (catalogId <= 0) {
      throw new RuntimeException(
          "Failed to initialize catalogId before using catalog with name: " + catalogName);
    }
    return catalogId;
  }

  private void renameTableLike(
      PolarisEntitySubType subType, TableIdentifier from, TableIdentifier to) {
    LOGGER.debug("Renaming tableLike from {} to {}", from, to);
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofTableLike(from), subType);
    if (resolvedEntities == null) {
      if (subType == PolarisEntitySubType.ICEBERG_VIEW) {
        throw new NoSuchViewException("Cannot rename %s to %s. View does not exist", from, to);
      } else {
        throw new NoSuchTableException("Cannot rename %s to %s. Table does not exist", from, to);
      }
    }
    List<PolarisEntity> catalogPath = resolvedEntities.getRawParentPath();
    PolarisEntity leafEntity = resolvedEntities.getRawLeafEntity();
    final IcebergTableLikeEntity toEntity;
    List<PolarisEntity> newCatalogPath = null;
    if (!from.namespace().equals(to.namespace())) {
      PolarisResolvedPathWrapper resolvedNewParentEntities =
          resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(to.namespace()));
      if (resolvedNewParentEntities == null) {
        throw new NoSuchNamespaceException(
            "Cannot rename %s to %s. Namespace does not exist: %s", from, to, to.namespace());
      }
      newCatalogPath = resolvedNewParentEntities.getRawFullPath();

      // the "to" table has a new parent and a new name / namespace path
      toEntity =
          new IcebergTableLikeEntity.Builder(IcebergTableLikeEntity.of(leafEntity))
              .setTableIdentifier(to)
              .setParentId(resolvedNewParentEntities.getResolvedLeafEntity().getEntity().getId())
              .build();
    } else {
      // only the name of the entity is changed
      toEntity =
          new IcebergTableLikeEntity.Builder(IcebergTableLikeEntity.of(leafEntity))
              .setTableIdentifier(to)
              .build();
    }

    // rename the entity now
    EntityResult returnedEntityResult =
        getMetaStoreManager()
            .renameEntity(
                getCurrentPolarisContext(),
                PolarisEntity.toCoreList(catalogPath),
                leafEntity,
                PolarisEntity.toCoreList(newCatalogPath),
                toEntity);

    // handle error
    if (!returnedEntityResult.isSuccess()) {
      LOGGER.debug(
          "Rename error {} trying to rename {} to {}. Checking existing object.",
          returnedEntityResult.getReturnStatus(),
          from,
          to);
      switch (returnedEntityResult.getReturnStatus()) {
        case BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS:
          {
            PolarisEntitySubType existingEntitySubType =
                returnedEntityResult.getAlreadyExistsEntitySubType();
            throw new AlreadyExistsException(
                "Cannot rename %s to %s. %s already exists",
                from, to, entityNameForSubType(existingEntitySubType));
          }

        case BaseResult.ReturnStatus.ENTITY_NOT_FOUND:
          throw new NotFoundException("Cannot rename %s to %s. %s does not exist", from, to, from);

        // this is temporary. Should throw a special error that will be caught and retried
        case BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED:
        case BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RESOLVED:
          throw new RuntimeException("concurrent update detected, please retry");

        // some entities cannot be renamed
        case BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RENAMED:
          throw new BadRequestException("Cannot rename built-in object %s", leafEntity.getName());

        // some entities cannot be renamed
        default:
          throw new IllegalStateException(
              "Unknown error status " + returnedEntityResult.getReturnStatus());
      }
    } else {
      IcebergTableLikeEntity returnedEntity =
          IcebergTableLikeEntity.of(returnedEntityResult.getEntity());
      if (!toEntity.getTableIdentifier().equals(returnedEntity.getTableIdentifier())) {
        // As long as there are older deployments which don't support the atomic update of the
        // internalProperties during rename, we can log and then patch it up explicitly
        // in a best-effort way.
        LOGGER
            .atError()
            .addKeyValue("toEntity.getTableIdentifier()", toEntity.getTableIdentifier())
            .addKeyValue("returnedEntity.getTableIdentifier()", returnedEntity.getTableIdentifier())
            .log("Returned entity identifier doesn't match toEntity identifier");
        getMetaStoreManager()
            .updateEntityPropertiesIfNotChanged(
                getCurrentPolarisContext(),
                PolarisEntity.toCoreList(newCatalogPath),
                new IcebergTableLikeEntity.Builder(returnedEntity).setTableIdentifier(to).build());
      }
    }
  }

  /**
   * Caller must fill in all entity fields except parentId, since the caller may not want to
   * duplicate the logic to try to resolve parentIds before constructing the proposed entity. This
   * method will fill in the parentId if needed upon resolution.
   */
  private void createTableLike(TableIdentifier identifier, PolarisEntity entity) {
    PolarisResolvedPathWrapper resolvedParent =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(identifier.namespace()));
    if (resolvedParent == null) {
      // Illegal state because the namespace should've already been in the static resolution set.
      throw new IllegalStateException(
          String.format("Failed to fetch resolved parent for TableIdentifier '%s'", identifier));
    }

    createTableLike(identifier, entity, resolvedParent);
  }

  private void createTableLike(
      TableIdentifier identifier, PolarisEntity entity, PolarisResolvedPathWrapper resolvedParent) {
    IcebergTableLikeEntity icebergTableLikeEntity = IcebergTableLikeEntity.of(entity);
    // Set / suffix
    boolean requireTrailingSlash =
        realmConfig.getConfig(FeatureConfiguration.ADD_TRAILING_SLASH_TO_LOCATION);
    if (requireTrailingSlash
        && icebergTableLikeEntity.getBaseLocation() != null
        && !icebergTableLikeEntity.getBaseLocation().endsWith("/")) {
      icebergTableLikeEntity =
          new IcebergTableLikeEntity.Builder(icebergTableLikeEntity)
              .setBaseLocation(icebergTableLikeEntity.getBaseLocation() + "/")
              .build();
    }

    // Make sure the metadata file is valid for our allowed locations.
    String metadataLocation = icebergTableLikeEntity.getMetadataLocation();
    validateLocationForTableLike(identifier, metadataLocation, resolvedParent);

    List<PolarisEntity> catalogPath = resolvedParent.getRawFullPath();

    if (icebergTableLikeEntity.getParentId() <= 0) {
      // TODO: Validate catalogPath size is at least 1 for catalog entity?
      icebergTableLikeEntity =
          new IcebergTableLikeEntity.Builder(icebergTableLikeEntity)
              .setParentId(resolvedParent.getRawLeafEntity().getId())
              .build();
    }
    icebergTableLikeEntity =
        new IcebergTableLikeEntity.Builder(icebergTableLikeEntity)
            .setCreateTimestamp(System.currentTimeMillis())
            .build();

    EntityResult res =
        getMetaStoreManager()
            .createEntityIfNotExists(
                getCurrentPolarisContext(),
                PolarisEntity.toCoreList(catalogPath),
                icebergTableLikeEntity);
    if (!res.isSuccess()) {
      switch (res.getReturnStatus()) {
        case BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED:
          throw new NotFoundException("Parent path does not exist for %s", identifier);

        case BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS:
          throw alreadyExistsExceptionForTableLikeEntity(
              identifier, res.getAlreadyExistsEntitySubType());
        default:
          throw new IllegalStateException(
              String.format(
                  "Unknown error status for identifier %s: %s with extraInfo: %s",
                  identifier, res.getReturnStatus(), res.getExtraInformation()));
      }
    }
    PolarisEntity resultEntity = PolarisEntity.of(res);
    LOGGER.debug("Created TableLike entity {} with TableIdentifier {}", resultEntity, identifier);
  }

  private void updateTableLike(TableIdentifier identifier, PolarisEntity entity) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getResolvedPath(
            ResolvedPathKey.ofTableLike(identifier), entity.getSubType());
    if (resolvedEntities == null) {
      // Illegal state because the identifier should've already been in the static resolution set.
      throw new IllegalStateException(
          String.format("Failed to fetch resolved TableIdentifier '%s'", identifier));
    }
    IcebergTableLikeEntity icebergTableLikeEntity = new IcebergTableLikeEntity(entity);

    // Set / suffix
    boolean requireTrailingSlash =
        realmConfig.getConfig(FeatureConfiguration.ADD_TRAILING_SLASH_TO_LOCATION);
    if (requireTrailingSlash
        && icebergTableLikeEntity.getBaseLocation() != null
        && !icebergTableLikeEntity.getBaseLocation().endsWith("/")) {
      icebergTableLikeEntity =
          new IcebergTableLikeEntity.Builder(icebergTableLikeEntity)
              .setBaseLocation(icebergTableLikeEntity.getBaseLocation() + "/")
              .build();
    }

    // Make sure the metadata file is valid for our allowed locations.
    String metadataLocation = icebergTableLikeEntity.getMetadataLocation();
    validateLocationForTableLike(identifier, metadataLocation, resolvedEntities);

    List<PolarisEntity> catalogPath = resolvedEntities.getRawParentPath();
    EntityResult res =
        getMetaStoreManager()
            .updateEntityPropertiesIfNotChanged(
                getCurrentPolarisContext(),
                PolarisEntity.toCoreList(catalogPath),
                icebergTableLikeEntity);
    if (!res.isSuccess()) {
      switch (res.getReturnStatus()) {
        case BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED:
          throw new NotFoundException("Parent path does not exist for %s", identifier);

        case BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED:
          throw new CommitConflictException(
              "Failed to commit Table or View %s because it was concurrently modified", identifier);

        default:
          throw new IllegalStateException(
              String.format(
                  "Unknown error status for identifier %s: %s with extraInfo: %s",
                  identifier, res.getReturnStatus(), res.getExtraInformation()));
      }
    }
    PolarisEntity resultEntity = PolarisEntity.of(res);
    LOGGER.debug("Updated TableLike entity {} with TableIdentifier {}", resultEntity, identifier);
  }

  @SuppressWarnings("FormatStringAnnotation")
  private @NonNull DropEntityResult dropTableLike(
      PolarisEntitySubType subType,
      TableIdentifier identifier,
      Map<String, String> storageProperties,
      boolean purge) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofTableLike(identifier), subType);
    if (resolvedEntities == null) {
      // TODO: Error?
      return new DropEntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null);
    }

    List<PolarisEntity> catalogPath = resolvedEntities.getRawParentPath();
    PolarisEntity leafEntity = resolvedEntities.getRawLeafEntity();

    // Check that purge is enabled, if it is set:
    if (catalogPath != null && !catalogPath.isEmpty() && purge) {
      boolean dropWithPurgeEnabled =
          realmConfig.getConfig(FeatureConfiguration.DROP_WITH_PURGE_ENABLED, catalogEntity);
      if (!dropWithPurgeEnabled) {
        throw new ForbiddenException(
            String.format(
                "Unable to purge entity: %s. To enable this feature, set the Polaris configuration %s "
                    + "or the catalog configuration %s",
                identifier.name(),
                FeatureConfiguration.DROP_WITH_PURGE_ENABLED.key(),
                FeatureConfiguration.DROP_WITH_PURGE_ENABLED.catalogConfig()));
      }
    }

    return getMetaStoreManager()
        .dropEntityIfExists(
            getCurrentPolarisContext(),
            PolarisEntity.toCoreList(catalogPath),
            leafEntity,
            storageProperties,
            purge);
  }

  private boolean sendNotificationForTableLike(
      PolarisEntitySubType subType, TableIdentifier tableIdentifier, NotificationRequest request) {
    LOGGER.debug(
        "Handling notification request {} for tableIdentifier {}", request, tableIdentifier);
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getPassthroughResolvedPath(
            ResolvedPathKey.ofTableLike(tableIdentifier), subType);

    NotificationType notificationType = request.getNotificationType();

    Preconditions.checkNotNull(notificationType, "Expected a valid notification type.");

    if (notificationType == NotificationType.DROP) {
      return dropTableLike(
              PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier, Map.of(), false /* purge */)
          .isSuccess();
    } else if (notificationType == NotificationType.VALIDATE) {
      // In this mode we don't want to make any mutations, so we won't auto-create non-existing
      // parent namespaces. This means when we want to validate allowedLocations for the proposed
      // table metadata location, we must independently find the deepest non-null parent namespace
      // of the TableIdentifier, which may even be the base CatalogEntity if no parent namespaces
      // actually exist yet. We can then extract the right StorageInfo entity via a normal call
      // to findStorageInfoFromHierarchy.
      PolarisResolvedPathWrapper resolvedStorageEntity = null;
      Optional<PolarisEntity> storageInfoEntity = Optional.empty();
      for (int i = tableIdentifier.namespace().length(); i >= 0; i--) {
        Namespace nsLevel =
            Namespace.of(
                Arrays.stream(tableIdentifier.namespace().levels())
                    .limit(i)
                    .toArray(String[]::new));
        resolvedStorageEntity =
            resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(nsLevel));
        if (resolvedStorageEntity != null) {
          storageInfoEntity = FileIOUtil.findStorageInfoFromHierarchy(resolvedStorageEntity);
          break;
        }
      }

      if (resolvedStorageEntity == null || storageInfoEntity.isEmpty()) {
        throw new BadRequestException(
            "Failed to find StorageInfo entity for TableIdentifier %s", tableIdentifier);
      }

      // Validate location against the resolvedStorageEntity
      String metadataLocation =
          transformTableLikeLocation(tableIdentifier, request.getPayload().getMetadataLocation());
      validateLocationForTableLike(tableIdentifier, metadataLocation, resolvedStorageEntity);

      // Validate that we can construct a FileIO
      String locationDir = metadataLocation.substring(0, metadataLocation.lastIndexOf("/"));
      loadFileIOForTableLike(
          tableIdentifier,
          Set.of(locationDir),
          resolvedStorageEntity,
          new HashMap<>(tableDefaultProperties),
          Set.of(PolarisStorageActions.READ));

      LOGGER.debug(
          "Successful VALIDATE notification for tableIdentifier {}, metadataLocation {}",
          tableIdentifier,
          metadataLocation);
    } else if (notificationType == NotificationType.CREATE
        || notificationType == NotificationType.UPDATE) {

      Namespace ns = tableIdentifier.namespace();
      createNonExistingNamespaces(ns);

      PolarisResolvedPathWrapper resolvedParent =
          resolvedEntityView.getPassthroughResolvedPath(ResolvedPathKey.ofNamespace(ns));

      IcebergTableLikeEntity entity =
          IcebergTableLikeEntity.of(
              resolvedEntities == null ? null : resolvedEntities.getRawLeafEntity());

      String existingLocation;
      String newLocation =
          transformTableLikeLocation(tableIdentifier, request.getPayload().getMetadataLocation());
      if (null == entity) {
        existingLocation = null;
        entity =
            new IcebergTableLikeEntity.Builder(
                    PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier, newLocation)
                .setCatalogId(getCatalogId())
                .setId(
                    getMetaStoreManager().generateNewEntityId(getCurrentPolarisContext()).getId())
                .setLastNotificationTimestamp(request.getPayload().getTimestamp())
                .build();
      } else {
        // If the notification timestamp is out-of-order, we should not update the table
        if (entity.getLastAdmittedNotificationTimestamp().isPresent()
            && request.getPayload().getTimestamp()
                <= entity.getLastAdmittedNotificationTimestamp().get()) {
          throw new AlreadyExistsException(
              "A notification with a newer timestamp has been processed for table %s",
              tableIdentifier);
        }
        existingLocation = entity.getMetadataLocation();
        entity =
            new IcebergTableLikeEntity.Builder(entity)
                .setMetadataLocation(newLocation)
                .setLastNotificationTimestamp(request.getPayload().getTimestamp())
                .build();
      }
      // first validate we can read the metadata file
      validateLocationForTableLike(tableIdentifier, newLocation);

      String locationDir = newLocation.substring(0, newLocation.lastIndexOf("/"));

      FileIO fileIO =
          loadFileIOForTableLike(
              tableIdentifier,
              Set.of(locationDir),
              resolvedParent,
              new HashMap<>(tableDefaultProperties),
              Set.of(
                  PolarisStorageActions.READ,
                  PolarisStorageActions.WRITE,
                  PolarisStorageActions.LIST));
      TableMetadata tableMetadata = TableMetadataParser.read(fileIO, newLocation);

      // then validate that it points to a valid location for this table
      validateLocationForTableLike(tableIdentifier, tableMetadata.location());

      // finally, validate that the metadata file is within the table directory
      validateMetadataFileInTableDir(tableIdentifier, tableMetadata);

      // TODO: These might fail due to concurrent update; we need to do a retry in those cases.
      if (null == existingLocation) {
        LOGGER.debug(
            "Creating table {} for notification with metadataLocation {}",
            tableIdentifier,
            newLocation);
        createTableLike(tableIdentifier, entity, resolvedParent);
      } else {
        LOGGER.debug(
            "Updating table {} for notification with metadataLocation {}",
            tableIdentifier,
            newLocation);

        updateTableLike(tableIdentifier, entity);
      }
    }
    return true;
  }

  private void createNonExistingNamespaces(Namespace namespace) {
    // Pre-create namespaces if they don't exist
    for (int i = 1; i <= namespace.length(); i++) {
      Namespace nsLevel =
          Namespace.of(Arrays.stream(namespace.levels()).limit(i).toArray(String[]::new));
      if (resolvedEntityView.getPassthroughResolvedPath(ResolvedPathKey.ofNamespace(nsLevel))
          == null) {
        Namespace parentNamespace = PolarisCatalogHelpers.getParentNamespace(nsLevel);
        PolarisResolvedPathWrapper resolvedParent =
            resolvedEntityView.getPassthroughResolvedPath(
                ResolvedPathKey.ofNamespace(parentNamespace));
        try {
          createNamespaceInternal(nsLevel, Collections.emptyMap(), resolvedParent);
        } catch (AlreadyExistsException aee) {
          // Since we only attempted to create the namespace after checking that
          // getPassthroughResolvedPath for this level is null, this should be a relatively
          // infrequent case during high concurrency where another notification already
          // conveniently created the namespace between the time we checked and the time
          // we attempted to fill it in. It's working as intended in this case to simply
          // continue with the existing namespace, but the fact that this collision occurred
          // may be relevant to someone running the service in case of unexpected interactions,
          // so we'll still log the fact that this happened.
          LOGGER
              .atInfo()
              .setCause(aee)
              .addKeyValue("namespace", namespace)
              .log("Namespace already exists in createNonExistingNamespace");
        }
      }
    }
  }

  private Page<TableIdentifier> listTableLike(
      PolarisEntitySubType subType, Namespace namespace, PageToken pageToken) {
    PolarisResolvedPathWrapper resolvedEntities =
        resolvedEntityView.getResolvedPath(ResolvedPathKey.ofNamespace(namespace));
    if (resolvedEntities == null) {
      // Illegal state because the namespace should've already been in the static resolution set.
      throw new IllegalStateException(
          String.format("Failed to fetch resolved namespace '%s'", namespace));
    }

    List<PolarisEntity> catalogPath = resolvedEntities.getRawFullPath();
    ListEntitiesResult listResult =
        getMetaStoreManager()
            .listEntities(
                getCurrentPolarisContext(),
                PolarisEntity.toCoreList(catalogPath),
                PolarisEntityType.TABLE_LIKE,
                subType,
                pageToken);

    Namespace parentNamespace = PolarisCatalogHelpers.parentNamespace(catalogPath);
    return listResult
        .getPage()
        .map(record -> TableIdentifier.of(parentNamespace, record.getName()));
  }

  private int getMaxMetadataRefreshRetries() {
    return realmConfig.getConfig(FeatureConfiguration.MAX_METADATA_REFRESH_RETRIES);
  }

  // ===============================================================================================
  // Issue 29: merged Iceberg catalog feature-SPI implementation (E = NoExtension).
  //
  // The public REST operations below are transcribed from the retired IcebergCatalogHandler.
  // Authorization is composed via the CatalogAuthorizer helper (never a base class); the local data
  // mechanics are this instance's own Iceberg machinery (baseCatalog == this), and the federated
  // path forwards to a narrow remote delegate. These overrides are additive and unused until Inc6
  // rewires IcebergCatalogAdapter to call them directly; the legacy view-taking construction path
  // never reaches them.
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
   * #baseInitialized}). Moved verbatim from the retired {@code
   * IcebergCatalogHandler.initializeCatalog()}, folding in {@code
   * PolarisLocalCatalogFactory.createCatalog}'s local-initialize step (this instance IS the local
   * catalog, so it initializes itself instead of building a new one).
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
      initialize(catalogName, localCatalogProperties);
      this.baseCatalog = this;
      this.namespaceCatalog = this;
      this.viewCatalog = this;
    }
    this.baseInitialized = true;
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
  public PolarisResult<ListNamespacesResponse, NoExtension> listNamespaces(
      Namespace parent, String pageToken, Integer pageSize) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_NAMESPACES;
    authz.authorizeBasicNamespaceOperationOrThrow(op, parent);
    ensureBaseInitialized();

    ListNamespacesResponse response;
    if (isFederated) {
      response = catalogHandlerUtils.listNamespaces(namespaceCatalog, parent, pageToken, pageSize);
    } else {
      PageToken pageRequest = PageToken.build(pageToken, pageSize, this::shouldDecodeToken);
      var results = this.listNamespaces(parent, pageRequest);
      response =
          ListNamespacesResponse.builder()
              .addAll(results.items())
              .nextPageToken(results.encodedResponseToken())
              .build();
    }
    return PolarisResult.of(response);
  }

  @Override
  public PolarisResult<CreateNamespaceResponse, NoExtension> createNamespace(
      CreateNamespaceRequest request) {
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
    return PolarisResult.of(response);
  }

  @Override
  public PolarisResult<GetNamespaceResponse, NoExtension> getNamespaceMetadata(
      Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_NAMESPACE_METADATA;
    authz.authorizeBasicNamespaceOperationOrThrow(op, namespace);
    ensureBaseInitialized();

    return PolarisResult.of(catalogHandlerUtils.loadNamespace(namespaceCatalog, namespace));
  }

  @Override
  public PolarisResult<Void, NoExtension> checkNamespaceExists(Namespace namespace) {
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
    return PolarisResult.<Void>of(null);
  }

  @Override
  public PolarisResult<Void, NoExtension> deleteNamespace(Namespace namespace) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_NAMESPACE;
    authz.authorizeBasicNamespaceOperationOrThrow(op, namespace);
    ensureBaseInitialized();

    catalogHandlerUtils.dropNamespace(namespaceCatalog, namespace);
    return PolarisResult.<Void>of(null);
  }

  @Override
  public PolarisResult<UpdateNamespacePropertiesResponse, NoExtension> updateNamespaceProperties(
      Namespace namespace, UpdateNamespacePropertiesRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_NAMESPACE_PROPERTIES;
    authz.authorizeBasicNamespaceOperationOrThrow(op, namespace);
    ensureBaseInitialized();

    return PolarisResult.of(
        catalogHandlerUtils.updateNamespaceProperties(namespaceCatalog, namespace, request));
  }

  @Override
  public PolarisResult<ConfigResponse, NoExtension> getConfig() {
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
    return PolarisResult.of(response);
  }

  // ---- Issue 29 Inc 4c: table ops (transcribed from IcebergCatalogHandler). ----

  private static final String SNAPSHOTS_ALL = "all";
  private static final String SNAPSHOTS_REFS = "refs";

  @Override
  public PolarisResult<ListTablesResponse, NoExtension> listTables(
      Namespace namespace, String pageToken, Integer pageSize) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_TABLES;
    authz.authorizeBasicNamespaceOperationOrThrow(op, namespace);
    ensureBaseInitialized();

    ListTablesResponse response;
    if (isFederated) {
      response = catalogHandlerUtils.listTables(baseCatalog, namespace, pageToken, pageSize);
    } else {
      PageToken pageRequest = PageToken.build(pageToken, pageSize, this::shouldDecodeToken);
      var results = this.listTables(namespace, pageRequest);
      response =
          ListTablesResponse.builder()
              .addAll(results.items())
              .nextPageToken(results.encodedResponseToken())
              .build();
    }
    return PolarisResult.of(response);
  }

  @Override
  public PolarisResult<LoadTableResponse, NoExtension> createTableDirect(
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
      return PolarisResult.of(response, etagForCreatedTable(tableIdentifier, response));
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
  public PolarisResult<LoadTableResponse, NoExtension> createTableStaged(
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
    return PolarisResult.of(response);
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
  public PolarisResult<LoadTableResponse, NoExtension> registerTable(
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
      return PolarisResult.of(response, etagForCreatedTable(identifier, response));
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
  public ConditionalLoadOutcome<LoadTableResponse, NoExtension> loadTable(
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
          return new ConditionalLoadOutcome.NotModified<>(
              Optional.of(tableETag), NoExtension.INSTANCE);
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
      return new ConditionalLoadOutcome.Loaded<>(
          new PolarisResult<>(filteredResponse, loadedEtag, NoExtension.INSTANCE));
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
  public PolarisResult<LoadTableResponse, NoExtension> updateTable(
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
    return PolarisResult.of(
        catalogHandlerUtils.updateTable(baseCatalog, tableIdentifier, applyUpdateFilters(request)));
  }

  @Override
  public PolarisResult<LoadTableResponse, NoExtension> updateTableForStagedCreate(
      TableIdentifier tableIdentifier, UpdateTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.UPDATE_TABLE_FOR_STAGED_CREATE;
    authz.authorizeCreateTableLikeUnderNamespaceOperationOrThrow(op, tableIdentifier);
    ensureBaseInitialized();

    CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
    if (resolvedCatalog.isStaticFacade()) {
      throw new BadRequestException("Cannot update table on static-facade external catalogs.");
    }
    return PolarisResult.of(
        catalogHandlerUtils.updateTable(baseCatalog, tableIdentifier, applyUpdateFilters(request)));
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
  public PolarisResult<Void, NoExtension> dropTableWithoutPurge(TableIdentifier tableIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_TABLE_WITHOUT_PURGE;
    authz.authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);
    ensureBaseInitialized();

    catalogHandlerUtils.dropTable(baseCatalog, tableIdentifier);
    return PolarisResult.<Void>of(null);
  }

  @Override
  public PolarisResult<Void, NoExtension> dropTableWithPurge(TableIdentifier tableIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_TABLE_WITH_PURGE;
    authz.authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);
    ensureBaseInitialized();

    CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
    if (resolvedCatalog.isStaticFacade()) {
      throw new BadRequestException("Cannot drop table on static-facade external catalogs.");
    }
    catalogHandlerUtils.purgeTable(baseCatalog, tableIdentifier);
    return PolarisResult.<Void>of(null);
  }

  @Override
  public PolarisResult<Void, NoExtension> checkTableExists(TableIdentifier tableIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.TABLE_EXISTS;
    authz.authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_TABLE, tableIdentifier);
    ensureBaseInitialized();

    // TODO: Just skip CatalogHandlers for this one maybe
    catalogHandlerUtils.loadTable(baseCatalog, tableIdentifier);
    return PolarisResult.<Void>of(null);
  }

  @Override
  public PolarisResult<Void, NoExtension> renameTable(RenameTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.RENAME_TABLE;
    authz.authorizeRenameTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_TABLE, request.source(), request.destination());
    ensureBaseInitialized();

    CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
    if (resolvedCatalog.isStaticFacade()) {
      throw new BadRequestException("Cannot rename table on static-facade external catalogs.");
    }
    catalogHandlerUtils.renameTable(baseCatalog, request);
    return PolarisResult.<Void>of(null);
  }

  @Override
  public PolarisResult<Void, NoExtension> commitTransaction(
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
    // one. The merged class has a single metaStoreManager field, so capture the real manager here,
    // then swap in the TransactionWorkspaceMetaStoreManager so all mutations made by this catalog
    // only go into an in-memory collection that we can commit as a single atomic unit after all
    // validations, and finally commit the collected updates through the captured real manager.
    DurableManager realMetaStoreManager = metaStoreManager;
    TransactionWorkspaceMetaStoreManager transactionMetaStoreManager =
        new TransactionWorkspaceMetaStoreManager(diagnostics, realMetaStoreManager);
    setMetaStoreManager(transactionMetaStoreManager);

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

    eventAttributeMap.put(EventAttributes.TABLE_METADATAS, tableMetadataObjs);
    return PolarisResult.<Void>of(null);
  }

  @Override
  public PolarisResult<ImmutableLoadCredentialsResponse, NoExtension> loadCredentials(
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
      return PolarisResult.of(fallbackToFullLoadTable(tableIdentifier, refreshCredentialsEndpoint));
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
      return PolarisResult.of(fallbackToFullLoadTable(tableIdentifier, refreshCredentialsEndpoint));
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

    return PolarisResult.of(responseBuilder.build());
  }

  private ImmutableLoadCredentialsResponse fallbackToFullLoadTable(
      TableIdentifier tableIdentifier, Optional<String> refreshCredentialsEndpoint) {
    ConditionalLoadOutcome<LoadTableResponse, NoExtension> outcome =
        loadTable(
            tableIdentifier,
            SNAPSHOTS_ALL,
            null,
            EnumSet.of(AccessDelegationMode.VENDED_CREDENTIALS),
            refreshCredentialsEndpoint);
    if (outcome instanceof ConditionalLoadOutcome.Loaded<LoadTableResponse, NoExtension> loaded) {
      return ImmutableLoadCredentialsResponse.builder()
          .credentials(loaded.result().body().credentials())
          .build();
    }
    throw new IllegalStateException(
        "loadTable returned NotModified with ifNoneMatch=null; this is unreachable by construction");
  }

  @Override
  public PolarisResult<Void, NoExtension> reportMetrics(
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
    return PolarisResult.<Void>of(null);
  }

  @Override
  public PolarisResult<Boolean, NoExtension> submitNotification(
      TableIdentifier identifier, NotificationRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.SEND_NOTIFICATIONS;

    // For now, just require the full set of privileges on the base Catalog entity, which we can
    // also express just as the "root" Namespace for purposes of the PolarisIcebergCatalog being
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
    return PolarisResult.of(result);
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
    // supplies the ETag first-class in the result so the adapter can become pass-through in Inc6.
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
  public PolarisResult<ListTablesResponse, NoExtension> listViews(
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
      var results = this.listViews(namespace, pageRequest);
      response =
          ListTablesResponse.builder()
              .addAll(results.items())
              .nextPageToken(results.encodedResponseToken())
              .build();
    }
    return PolarisResult.of(response);
  }

  @Override
  public PolarisResult<LoadViewResponse, NoExtension> createView(
      Namespace namespace, CreateViewRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_VIEW;
    authz.authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        op, TableIdentifier.of(namespace, request.name()));
    ensureBaseInitialized();

    CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
    if (resolvedCatalog.isStaticFacade()) {
      throw new BadRequestException("Cannot create view on static-facade external catalogs.");
    }
    return PolarisResult.of(catalogHandlerUtils.createView(viewCatalog, namespace, request));
  }

  @Override
  public PolarisResult<LoadViewResponse, NoExtension> registerView(
      Namespace namespace, RegisterViewRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.REGISTER_VIEW;
    authz.authorizeCreateTableLikeUnderNamespaceOperationOrThrow(
        op, TableIdentifier.of(namespace, request.name()));
    ensureBaseInitialized();

    return PolarisResult.of(catalogHandlerUtils.registerView(viewCatalog, namespace, request));
  }

  @Override
  public PolarisResult<LoadViewResponse, NoExtension> getView(TableIdentifier viewIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LOAD_VIEW;
    authz.authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);
    ensureBaseInitialized();

    return PolarisResult.of(catalogHandlerUtils.loadView(viewCatalog, viewIdentifier));
  }

  @Override
  public PolarisResult<LoadViewResponse, NoExtension> replaceView(
      TableIdentifier viewIdentifier, UpdateTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.REPLACE_VIEW;
    authz.authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);
    ensureBaseInitialized();

    CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
    if (resolvedCatalog.isStaticFacade()) {
      throw new BadRequestException("Cannot replace view on static-facade external catalogs.");
    }
    return PolarisResult.of(
        catalogHandlerUtils.updateView(viewCatalog, viewIdentifier, applyUpdateFilters(request)));
  }

  @Override
  public PolarisResult<Void, NoExtension> deleteView(TableIdentifier viewIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.DROP_VIEW;
    authz.authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);
    ensureBaseInitialized();

    catalogHandlerUtils.dropView(viewCatalog, viewIdentifier);
    return PolarisResult.<Void>of(null);
  }

  @Override
  public PolarisResult<Void, NoExtension> checkViewExists(TableIdentifier viewIdentifier) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.VIEW_EXISTS;
    authz.authorizeBasicTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_VIEW, viewIdentifier);
    ensureBaseInitialized();

    // TODO: Just skip CatalogHandlers for this one maybe
    catalogHandlerUtils.loadView(viewCatalog, viewIdentifier);
    return PolarisResult.<Void>of(null);
  }

  @Override
  public PolarisResult<Void, NoExtension> renameView(RenameTableRequest request) {
    PolarisAuthorizableOperation op = PolarisAuthorizableOperation.RENAME_VIEW;
    authz.authorizeRenameTableLikeOperationOrThrow(
        op, PolarisEntitySubType.ICEBERG_VIEW, request.source(), request.destination());
    ensureBaseInitialized();

    CatalogEntity resolvedCatalog = getResolvedCatalogEntity();
    if (resolvedCatalog.isStaticFacade()) {
      throw new BadRequestException("Cannot rename view on static-facade external catalogs.");
    }
    catalogHandlerUtils.renameView(viewCatalog, request);
    return PolarisResult.<Void>of(null);
  }
}
