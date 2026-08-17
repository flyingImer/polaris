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
package org.apache.polaris.extension.durable.manager;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.ChangeTrackingResult;
import org.apache.polaris.core.persistence.dao.entity.CreateCatalogResult;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.persistence.dao.entity.GenerateEntityIdResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.LoadGrantsResult;
import org.apache.polaris.core.persistence.dao.entity.LoadPolicyMappingsResult;
import org.apache.polaris.core.persistence.dao.entity.PolicyAttachmentResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.persistence.dao.entity.PrivilegeResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolicyEntity;
import org.apache.polaris.core.policy.PolicyType;
import org.apache.polaris.spi.durable.DurableManager;
import org.apache.polaris.spi.durable.DurableOrchestrator;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.GrantManager;
import org.apache.polaris.spi.durable.PolarisEventManager;
import org.apache.polaris.spi.durable.PolarisPolicyMappingManager;
import org.apache.polaris.spi.durable.RecordKind;
import org.apache.polaris.spi.durable.SecretsManager;
import org.jspecify.annotations.NonNull;

/**
 * The single new-model durable manager. At ticket 96 this class is a wholesale replacement (整类替换,
 * EJ 2026-08-10) for both {@code AtomicOperationMetaStoreManager} and the transactional old-model
 * manager it sits alongside today; there is no per-method migration, the old implementations are
 * deleted in one step once this class covers their surface.
 *
 * <p>This manager owns every business rule and knows no storage topology. It implements {@link
 * DurableManager}, {@link GrantManager}, {@link SecretsManager}, {@link
 * PolarisPolicyMappingManager} and {@link PolarisEventManager} on one object because {@code
 * PolarisTestMetaStoreManager} (and, at ticket 96, every other caller of the old managers) narrows
 * the concrete instance it is handed to each of those sibling interfaces with a runtime cast; a
 * class missing one of them fails that cast, not a later call.
 *
 * <p>Writes go through the injected {@link DurableOrchestrator}, the write door: it alone knows how
 * to group a mutation list by atomicity domain, commit each group, and compensate across groups on
 * failure. Reads resolve through the injected kind-to-store function, the read door, which is also
 * where {@link DurableRecordStore#generateNewId} lives — reads do not pass through orchestration
 * because orchestration only organizes calls to primitives operations, and a read has no atomicity
 * or recovery story for that layer to promise (decided 2026-08-14).
 *
 * <p>This class never reads the old {@link org.apache.polaris.spi.durable.DurablePrimitives} handle
 * carried on {@link PolarisCallContext}. That handle is the old model's write/read door and
 * reaching for it here would silently reintroduce the coupling this class exists to remove.
 */
public class DefaultDurableManager
    implements DurableManager,
        GrantManager,
        SecretsManager,
        PolarisPolicyMappingManager,
        PolarisEventManager {

  private final Clock clock;
  private final PolarisDiagnostics diagnostics;
  private final DurableOrchestrator orchestrator;
  private final Function<RecordKind, DurableRecordStore> storeForKind;

  public DefaultDurableManager(
      @NonNull Clock clock,
      @NonNull PolarisDiagnostics diagnostics,
      @NonNull DurableOrchestrator orchestrator,
      @NonNull Function<RecordKind, DurableRecordStore> storeForKind) {
    this.clock = clock;
    this.diagnostics = diagnostics;
    this.orchestrator = orchestrator;
    this.storeForKind = storeForKind;
  }

  // ---------------------------------------------------------- DurableManager (ticket 91)

  @Override
  public @NonNull EntityResult readEntityByName(
      @NonNull PolarisCallContext callCtx,
      List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull String name) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull ListEntitiesResult listEntities(
      @NonNull PolarisCallContext callCtx,
      List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull PageToken pageToken) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull Page<PolarisBaseEntity> listFullEntities(
      @NonNull PolarisCallContext callCtx,
      List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull PageToken pageToken) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull GenerateEntityIdResult generateNewEntityId(@NonNull PolarisCallContext callCtx) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull CreatePrincipalResult createPrincipal(
      @NonNull PolarisCallContext callCtx, @NonNull PrincipalEntity principal) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull CreateCatalogResult createCatalog(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity catalog,
      @NonNull List<PolarisEntityCore> principalRoles) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull EntityResult createEntityIfNotExists(
      @NonNull PolarisCallContext callCtx,
      List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entity) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull EntitiesResult createEntitiesIfNotExist(
      @NonNull PolarisCallContext callCtx,
      List<PolarisEntityCore> catalogPath,
      @NonNull List<? extends PolarisBaseEntity> entities) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull EntityResult updateEntityPropertiesIfNotChanged(
      @NonNull PolarisCallContext callCtx,
      List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entity) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @NonNull PolarisCallContext callCtx, @NonNull List<EntityWithPath> entities) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull EntityResult renameEntity(
      @NonNull PolarisCallContext callCtx,
      List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entityToRename,
      List<PolarisEntityCore> newCatalogPath,
      @NonNull PolarisEntity renamedEntity) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull DropEntityResult dropEntityIfExists(
      @NonNull PolarisCallContext callCtx,
      List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entityToDrop,
      Map<String, String> cleanupProperties,
      boolean cleanup) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull EntityResult loadEntity(
      @NonNull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      @NonNull PolarisEntityType entityType) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull ChangeTrackingResult loadEntitiesChangeTracking(
      @NonNull PolarisCallContext callCtx, @NonNull List<PolarisEntityId> entityIds) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull ResolvedEntityResult loadResolvedEntityById(
      @NonNull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull ResolvedEntitiesResult loadResolvedEntities(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityType entityType,
      @NonNull List<PolarisEntityId> entityIds) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  // ------------------------------------------------------- DurableManager (ticket 92 surfaces)

  @Override
  public @NonNull BaseResult purge(@NonNull PolarisCallContext callCtx) {
    throw new UnsupportedOperationException("ticket 92: purge");
  }

  @Override
  public @NonNull EntitiesResult loadTasks(
      @NonNull PolarisCallContext callCtx, String executorId, PageToken pageToken) {
    throw new UnsupportedOperationException("ticket 92: task leasing (loadTasks)");
  }

  @Override
  public @NonNull ResolvedEntityResult loadResolvedEntityByName(
      @NonNull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @NonNull PolarisEntityType entityType,
      @NonNull String entityName) {
    throw new UnsupportedOperationException(
        "ticket 92: entity cache / resolver refresh (loadResolvedEntityByName)");
  }

  @Override
  public @NonNull ResolvedEntityResult refreshResolvedEntity(
      @NonNull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      @NonNull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId) {
    throw new UnsupportedOperationException(
        "ticket 92: entity cache / resolver refresh (refreshResolvedEntity)");
  }

  // ---------------------------------------------------------- GrantManager (ticket 91)

  @Override
  public @NonNull PrivilegeResult grantUsageOnRoleToGrantee(
      @NonNull PolarisCallContext callCtx,
      PolarisEntityCore catalog,
      @NonNull PolarisEntityCore role,
      @NonNull PolarisEntityCore grantee) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull PrivilegeResult revokeUsageOnRoleFromGrantee(
      @NonNull PolarisCallContext callCtx,
      PolarisEntityCore catalog,
      @NonNull PolarisEntityCore role,
      @NonNull PolarisEntityCore grantee) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull PrivilegeResult grantPrivilegeOnSecurableToRole(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore grantee,
      List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityCore securable,
      @NonNull PolarisPrivilege privilege) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull PrivilegeResult revokePrivilegeOnSecurableFromRole(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore grantee,
      List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityCore securable,
      @NonNull PolarisPrivilege privilege) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull LoadGrantsResult loadGrantsOnSecurable(
      @NonNull PolarisCallContext callCtx, PolarisEntityCore securable) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull LoadGrantsResult loadGrantsToGrantee(
      @NonNull PolarisCallContext callCtx, PolarisEntityCore grantee) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  // ---------------------------------------------------------- SecretsManager (ticket 91)

  @Override
  public @NonNull PrincipalSecretsResult loadPrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String clientId) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull PrincipalSecretsResult rotatePrincipalSecrets(
      @NonNull PolarisCallContext callCtx,
      @NonNull String clientId,
      long principalId,
      boolean reset,
      @NonNull String oldSecretHash) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public @NonNull PrincipalSecretsResult resetPrincipalSecrets(
      @NonNull PolarisCallContext callCtx,
      long principalId,
      @NonNull String resolvedClientId,
      String customClientSecret) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  @Override
  public void deletePrincipalSecrets(
      @NonNull PolarisCallContext callCtx, @NonNull String clientId, long principalId) {
    throw new UnsupportedOperationException("ticket 91: not yet implemented");
  }

  // ------------------------------------------------- PolarisPolicyMappingManager (ticket 92)

  @Override
  public @NonNull PolicyAttachmentResult attachPolicyToEntity(
      @NonNull PolarisCallContext callCtx,
      @NonNull List<PolarisEntityCore> targetCatalogPath,
      @NonNull PolarisEntityCore target,
      @NonNull List<PolarisEntityCore> policyCatalogPath,
      @NonNull PolicyEntity policy,
      Map<String, String> parameters) {
    throw new UnsupportedOperationException("ticket 92: policy mapping (attachPolicyToEntity)");
  }

  @Override
  public @NonNull PolicyAttachmentResult detachPolicyFromEntity(
      @NonNull PolarisCallContext callCtx,
      @NonNull List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityCore target,
      @NonNull List<PolarisEntityCore> policyCatalogPath,
      @NonNull PolicyEntity policy) {
    throw new UnsupportedOperationException("ticket 92: policy mapping (detachPolicyFromEntity)");
  }

  @Override
  public @NonNull LoadPolicyMappingsResult loadPoliciesOnEntity(
      @NonNull PolarisCallContext callCtx, @NonNull PolarisEntityCore target) {
    throw new UnsupportedOperationException("ticket 92: policy mapping (loadPoliciesOnEntity)");
  }

  @Override
  public @NonNull LoadPolicyMappingsResult loadPoliciesOnEntityByType(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityCore target,
      @NonNull PolicyType policyType) {
    throw new UnsupportedOperationException(
        "ticket 92: policy mapping (loadPoliciesOnEntityByType)");
  }

  // ------------------------------------------------------- PolarisEventManager (ticket 92)

  /**
   * Overridden rather than left as the interface default: the default reaches {@link
   * PolarisCallContext#getMetaStore()}, the old primitives handle this class must never touch.
   */
  @Override
  public void writeEvents(
      @NonNull PolarisCallContext callCtx, @NonNull List<EventEntity> polarisEvents) {
    throw new UnsupportedOperationException("ticket 92: events (writeEvents)");
  }
}
