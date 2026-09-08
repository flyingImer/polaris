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

package org.apache.polaris.core.persistence.resolver;

import java.util.List;
import org.apache.polaris.annotations.Internal;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.dao.entity.ChangeTrackingResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntitiesResult;
import org.apache.polaris.core.persistence.dao.entity.ResolvedEntityResult;
import org.jspecify.annotations.NonNull;

/**
 * Transitional read door of the entity-resolution machinery: the resolved-entity and
 * change-tracking reads the resolver and its cache consume. Not a durable manager and not an SPI;
 * it exists so the resolver's internals have one typed dependency while the persistence layer is
 * reshaped, and it disappears when those internals are re-pointed onto the per-domain durable
 * managers.
 */
@Internal
public interface ResolvedEntityReads {

  /**
   * Load change tracking information for a set of entities in one single shot and return for each
   * the version for the entity itself and the version associated to its grant records.
   *
   * @param callCtx call context
   * @param entityIds list of catalog/entity pair ids for which we need to efficiently load the
   *     version information, both entity version and grant records version.
   * @return a list of version tracking information. Order in that returned list is the same as the
   *     input list. Some elements might be NULL if the entity has been purged. Not expected to fail
   */
  @NonNull ChangeTrackingResult loadEntitiesChangeTracking(
      @NonNull PolarisCallContext callCtx, @NonNull List<PolarisEntityId> entityIds);

  /**
   * Load a resolved entity, i.e. an entity definition and associated grant records, from the
   * backend store. The entity is identified by its id (entity catalog id and id).
   *
   * <p>For entities that can be grantees, the associated grant records will include both the grant
   * records for this entity as a grantee and for this entity as a securable.
   *
   * @param callCtx call context
   * @param entityCatalogId id of the catalog for that entity
   * @param entityId id of the entity
   * @return result with entity and grants. Status will be ENTITY_NOT_FOUND if the entity was not
   *     found
   */
  @NonNull ResolvedEntityResult loadResolvedEntityById(
      @NonNull PolarisCallContext callCtx,
      long entityCatalogId,
      long entityId,
      PolarisEntityType entityType);

  /**
   * Load a resolved entity, i.e. an entity definition and associated grant records, from the
   * backend store. The entity is identified by its name. Will return NULL if the entity does not
   * exist, i.e. has been purged or dropped.
   *
   * <p>For entities that can be grantees, the associated grant records will include both the grant
   * records for this entity as a grantee and for this entity as a securable.
   *
   * @param callCtx call context
   * @param entityCatalogId id of the catalog for that entity
   * @param parentId the id of the parent of that entity
   * @param entityType the type of this entity
   * @param entityName the name of this entity
   * @return result with entity and grants. Status will be ENTITY_NOT_FOUND if the entity was not
   *     found
   */
  @NonNull ResolvedEntityResult loadResolvedEntityByName(
      @NonNull PolarisCallContext callCtx,
      long entityCatalogId,
      long parentId,
      @NonNull PolarisEntityType entityType,
      @NonNull String entityName);

  /**
   * Load a batch of resolved entities of a specified entity type given their {@link
   * PolarisEntityId}. Will return an empty list if the input list is empty. Order in that returned
   * list is the same as the input list. Some elements might be NULL if the entity has been dropped.
   *
   * @param callCtx call context
   * @param entityType the type of entities to load
   * @param entityIds the list of entity ids to load
   * @return a non-null list of entities corresponding to the lookup keys. Some elements might be
   *     NULL if the entity has been dropped.
   */
  @NonNull ResolvedEntitiesResult loadResolvedEntities(
      @NonNull PolarisCallContext callCtx,
      @NonNull PolarisEntityType entityType,
      @NonNull List<PolarisEntityId> entityIds);

  /**
   * Refresh a resolved entity from the backend store. Will return NULL if the entity does not
   * exist, i.e. has been purged or dropped. Else, will determine what has changed based on the
   * version information sent by the caller and will return only what has changed.
   *
   * <p>For entities that can be grantees, the associated grant records will include both the grant
   * records for this entity as a grantee and for this entity as a securable.
   *
   * @param callCtx call context
   * @param entityType type of the entity whose entity and grants we are refreshing
   * @param entityCatalogId id of the catalog for that entity
   * @param entityId the id of the entity to load
   * @return result with entity and grants. Status will be ENTITY_NOT_FOUND if the entity was not
   *     found
   */
  @NonNull ResolvedEntityResult refreshResolvedEntity(
      @NonNull PolarisCallContext callCtx,
      int entityVersion,
      int entityGrantRecordsVersion,
      @NonNull PolarisEntityType entityType,
      long entityCatalogId,
      long entityId);

  /**
   * Indicates whether this metastore manager implementation requires entities to be reloaded via
   * {@link #loadEntitiesChangeTracking} in order to ensure the most recent versions are obtained.
   *
   * <p>Generally this flag is {@code true} when entity caching is used.
   */
  default boolean requiresEntityReload() {
    return true;
  }
}
