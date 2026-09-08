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

package org.apache.polaris.spi.durable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.LocationBasedEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.dao.entity.CreateCatalogResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.persistence.dao.entity.GenerateEntityIdResult;
import org.apache.polaris.core.persistence.dao.entity.ListEntitiesResult;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Durable manager for the entity tree: catalogs, namespaces, tables, views, roles, policies, tasks
 * and every other entity kind share one record kind and are created, read, renamed and dropped
 * here. Owns the business rules of those operations (existence checks, name-collision semantics,
 * the id-before-write discipline, cleanup composition on drop) and knows no storage topology.
 * Authorization and request validation live above this layer.
 */
public interface CatalogDurableManager {

  /**
   * Resolve an entity by name. Can be a top-level entity like a catalog or an entity inside a
   * catalog like a namespace, a role, a table like entity, or a principal. If the entity is inside
   * a catalog, the parameter catalogPath must be specified
   *
   * @param callCtx call context
   * @param catalogPath path inside a catalog to that entity, rooted by the catalog. If null, the
   *     entity being resolved is a top-level account entity like a catalog.
   * @param entityType entity type
   * @param entitySubType entity subtype. Can be the special value ANY_SUBTYPE to match any
   *     subtypes. Else exact match on the subtype will be required.
   * @param name name of the entity, cannot be null
   * @return the result of the lookup operation. ENTITY_NOT_FOUND is returned if the specified
   *     entity is not found in the specified path. CONCURRENT_MODIFICATION_DETECTED_NEED_RETRY is
   *     returned if the specified catalog path cannot be resolved.
   */
  @NonNull EntityResult readEntityByName(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull String name);

  /**
   * List lightweight information about entities matching the given criteria. If all properties of
   * the entity are required,use {@link #listFullEntities} instead.
   *
   * @param callCtx call context
   * @param catalogPath path inside a catalog. If null or empty, the entities to list are top-level,
   *     like catalogs
   * @param entityType entity type
   * @param entitySubType entity subtype. Can be the special value ANY_SUBTYPE to match any subtype.
   *     Else exact match will be performed.
   * @return all entities name, ids and subtype under the specified namespace.
   */
  @NonNull ListEntitiesResult listEntities(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull PageToken pageToken);

  /**
   * Load full entities matching the given criteria with pagination. If only the entity name/id/type
   * is required, use {@link #listEntities} instead. If no pagination is required, use {@link
   * #listFullEntitiesAll} instead.
   *
   * @param callCtx call context
   * @param catalogPath path inside a catalog. If null or empty, the entities to list are top-level,
   *     like catalogs
   * @param entityType type of entities to list
   * @param entitySubType subType of entities to list (or ANY_SUBTYPE)
   * @return paged list of matching entities
   */
  @NonNull Page<PolarisBaseEntity> listFullEntities(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType,
      @NonNull PageToken pageToken);

  /**
   * Load full entities matching the given criteria into an unpaged list. If pagination is required
   * use {@link #listFullEntities} instead. If only the entity name/id/type is required, use {@link
   * #listEntities} instead.
   *
   * @param callCtx call context
   * @param catalogPath path inside a catalog. If null or empty, the entities to list are top-level,
   *     like catalogs
   * @param entityType type of entities to list
   * @param entitySubType subType of entities to list (or ANY_SUBTYPE)
   * @return list of all matching entities
   */
  default @NonNull List<PolarisBaseEntity> listFullEntitiesAll(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisEntityType entityType,
      @NonNull PolarisEntitySubType entitySubType) {
    return listFullEntities(
            callCtx, catalogPath, entityType, entitySubType, PageToken.readEverything())
        .items();
  }

  /**
   * Generate a new unique id that can be used by the Polaris client when it needs to create a new
   * entity
   *
   * @param callCtx call context
   * @return the newly created id, not expected to fail
   */
  @NonNull GenerateEntityIdResult generateNewEntityId(@NonNull PolarisCallContext callCtx);

  /**
   * Create a new catalog. This not only creates the new catalog entity but also the initial admin
   * role required to admin this catalog. If inline storage integration property is provided, create
   * a storage integration.
   *
   * @param callCtx call context
   * @param catalog the catalog entity to create
   * @param principalRoles once the catalog has been created, list of principal roles to grant its
   *     catalog_admin role to. If no principal role is specified, we will grant the catalog_admin
   *     role of the newly created catalog to the service admin role.
   * @return if success, the catalog which was created and its admin role.
   */
  @NonNull CreateCatalogResult createCatalog(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisBaseEntity catalog,
      @NonNull List<PolarisEntityCore> principalRoles);

  /**
   * Persist a newly created entity under the specified catalog path if specified, else this is a
   * top-level entity. We will re-resolve the specified path to ensure nothing has changed since the
   * Polaris app resolved the path. If the entity already exists with the same specified id, we will
   * simply return it. This can happen when the client retries. If a catalogPath is specified and
   * cannot be resolved, we will return null. And of course if another entity exists with the same
   * name, we will fail and also return null.
   *
   * @param callCtx call context
   * @param catalogPath path inside a catalog. If null, the entity to persist is assumed to be
   *     top-level.
   * @param entity entity to write
   * @return the newly created entity. If this entity was already created, we will simply return the
   *     already created entity. We will return null if a different entity with the same name exists
   *     or if the catalogPath couldn't be resolved. If null is returned, the client app should
   *     retry this operation.
   */
  @NonNull EntityResult createEntityIfNotExists(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entity);

  /**
   * Persist a batch of newly created entities under the specified catalog path if specified, else
   * these are top-level entities. We will re-resolve the specified path to ensure nothing has
   * changed since the Polaris app resolved the path. If any of the entities already exists with the
   * same specified id, we will simply return it. This can happen when the client retries. If a
   * catalogPath is specified and cannot be resolved, we will return null and none of the entities
   * will be persisted. And of course if any entity conflicts with an existing entity with the same
   * name, we will fail all entities and also return null.
   *
   * @param callCtx call context
   * @param catalogPath path inside a catalog. If null, the entity to persist is assumed to be
   *     top-level.
   * @param entities batch of entities to write
   * @return the newly created entities. If the entities were already created, we will simply return
   *     the already created entity. We will return null if a different entity with the same name
   *     exists or if the catalogPath couldn't be resolved. If null is returned, the client app
   *     should retry this operation.
   */
  @NonNull EntitiesResult createEntitiesIfNotExist(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull List<? extends PolarisBaseEntity> entities);

  /**
   * Update some properties of this entity assuming it can still be resolved the same way and itself
   * has not changed. If this is not the case we will return false. Else we will update both the
   * internal and visible properties and return true
   *
   * @param callCtx call context
   * @param catalogPath path to that entity. Could be null if this entity is top-level
   * @param entity entity to update, cannot be null
   * @return the entity we updated or null if the client should retry
   */
  @NonNull EntityResult updateEntityPropertiesIfNotChanged(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entity);

  /**
   * This works exactly like {@link #updateEntityPropertiesIfNotChanged(PolarisCallContext, List,
   * PolarisBaseEntity)} but allows to operate on multiple entities at once. Just loop through the
   * list, calling each entity update and return null if any of those fail.
   *
   * @param callCtx call context
   * @param entities the set of entities to update
   * @return list of all entities we updated or null if the client should retry because one update
   *     failed
   */
  @NonNull EntitiesResult updateEntitiesPropertiesIfNotChanged(
      @NonNull PolarisCallContext callCtx, @NonNull List<EntityWithPath> entities);

  /**
   * Rename an entity, potentially re-parenting it.
   *
   * @param callCtx call context
   * @param catalogPath path to that entity. Could be an empty list of the entity is a catalog.
   * @param entityToRename entity to rename. This entity should have been resolved by the client
   * @param newCatalogPath if not null, new catalog path
   * @param renamedEntity the new renamed entity we need to persist. We will use this argument to
   *     also update the internal and external properties as part of the rename operation. This is
   *     required to update the namespace path of the entity if it has changed
   * @return the entity after renaming it or null if the rename operation has failed
   */
  @NonNull EntityResult renameEntity(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entityToRename,
      @Nullable List<PolarisEntityCore> newCatalogPath,
      @NonNull PolarisEntity renamedEntity);

  /**
   * Drop the specified entity assuming it exists
   *
   * @param callCtx call context
   * @param catalogPath path to that entity. Could be an empty list of the entity is a catalog.
   * @param entityToDrop entity to drop, must have been resolved by the client
   * @param cleanupProperties if not null, properties that will be persisted with the cleanup task
   * @param cleanup true if resources owned by this entity should be deleted as well
   * @return the result of the drop entity call, either success or error. If the error, it could be
   *     that the namespace or catalog to drop still has children, this should not be retried and
   *     should cause a failure
   */
  @NonNull DropEntityResult dropEntityIfExists(
      @NonNull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @NonNull PolarisBaseEntity entityToDrop,
      @Nullable Map<String, String> cleanupProperties,
      boolean cleanup);

  /**
   * Load the entity from backend store. Will return NULL if the entity does not exist, i.e. has
   * been purged. The entity being loaded might have been dropped
   *
   * @param callCtx call context
   * @param entityCatalogId id of the catalog for that entity
   * @param entityId the id of the entity to load
   */
  @NonNull EntityResult loadEntity(
      @NonNull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      @NonNull PolarisEntityType entityType);

  /**
   * Check if the specified IcebergTableLikeEntity has any same-namespace siblings which share a
   * location
   *
   * @param callContext the polaris call context
   * @param entity the entity to check for overlapping siblings for
   * @return Optional.of(Optional.of ( location)) if the parent entity has children,
   *     Optional.of(Optional.empty()) if not, and Optional.empty() if the metastore doesn't support
   *     this operation
   */
  default <T extends PolarisEntity & LocationBasedEntity>
      Optional<Optional<String>> hasOverlappingSiblings(
          @NonNull PolarisCallContext callContext, T entity) {
    return Optional.empty();
  }
}
