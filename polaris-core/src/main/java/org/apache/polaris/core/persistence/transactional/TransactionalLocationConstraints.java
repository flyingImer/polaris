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
package org.apache.polaris.core.persistence.transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.config.BehaviorChangeConfiguration;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.core.storage.StorageUtil;

/**
 * Shared domain validation. Early Feature checks are useful feedback, but the sibling range and its
 * current values must be read again in the attempt that publishes the dependent metadata. Native
 * adapters never interpret locations. This PoC deliberately uses full protected listings; a
 * production location index can reduce that cost without changing the rule.
 */
final class TransactionalLocationConstraints {
  private TransactionalLocationConstraints() {}

  static void validate(
      PolarisCallContext context,
      TransactionalPersistence store,
      List<? extends PolarisBaseEntity> changes) {
    Map<PolarisEntityId, PolarisBaseEntity> finalEntities = new HashMap<>();
    changes.forEach(e -> finalEntities.put(id(e), e));
    for (PolarisBaseEntity entity : changes) {
      Set<String> locations = locations(entity);
      if (locations.isEmpty()) continue;
      PolarisBaseEntity original =
          store.lookupEntityInCurrentTxn(
              context, entity.getCatalogId(), entity.getId(), entity.getTypeCode());
      // Preserve existing locations on unrelated property changes. Moving under another parent
      // still needs the destination's range even when the location string is unchanged.
      if (original != null
          && original.getParentId() == entity.getParentId()
          && locations(original).equals(locations)) continue;
      if (!requiresCheck(context, store, entity)) continue;

      List<PolarisBaseEntity> siblings = new ArrayList<>();
      if (entity.getType() == PolarisEntityType.CATALOG) {
        siblings.addAll(list(context, store, entity, PolarisEntityType.CATALOG));
      } else {
        siblings.addAll(list(context, store, entity, PolarisEntityType.NAMESPACE));
        if (entity.getParentId() != entity.getCatalogId()) {
          siblings.addAll(list(context, store, entity, PolarisEntityType.TABLE_LIKE));
        }
      }
      Map<PolarisEntityId, PolarisBaseEntity> projected = new HashMap<>();
      siblings.forEach(e -> projected.put(id(e), e));
      // Validate against the final batch, including siblings created or moved by this operation.
      projected.putAll(finalEntities);
      for (PolarisBaseEntity sibling : projected.values()) {
        if (id(sibling).equals(id(entity))
            || sibling.getCatalogId() != entity.getCatalogId()
            || sibling.getParentId() != entity.getParentId()) continue;
        Set<String> otherLocations =
            entity.getType() == PolarisEntityType.CATALOG
                ? sibling.getType() == PolarisEntityType.CATALOG ? locations(sibling) : Set.of()
                : baseLocation(sibling);
        for (String requested : locations) {
          for (String other : otherLocations) {
            if (overlaps(requested, other)) {
              if (entity.getType() == PolarisEntityType.CATALOG) {
                throw new ValidationException(
                    "Catalog location overlaps existing catalog: %s", sibling.getName());
              }
              throw new ForbiddenException(
                  "Location '%s' overlaps sibling '%s' at '%s'",
                  requested, sibling.getName(), other);
            }
          }
        }
      }
    }
  }

  private static List<PolarisBaseEntity> list(
      PolarisCallContext context,
      TransactionalPersistence store,
      PolarisBaseEntity entity,
      PolarisEntityType type) {
    return store
        .loadEntitiesInCurrentTxn(
            context,
            entity.getCatalogId(),
            entity.getParentId(),
            type,
            PolarisEntitySubType.ANY_SUBTYPE,
            ignored -> true,
            Function.identity(),
            PageToken.readEverything())
        .items();
  }

  private static boolean requiresCheck(
      PolarisCallContext context, TransactionalPersistence store, PolarisBaseEntity entity) {
    var config = context.getRealmConfig();
    if (entity.getType() == PolarisEntityType.CATALOG) {
      return !config.getConfig(FeatureConfiguration.ALLOW_OVERLAPPING_CATALOG_URLS);
    }
    if (entity.getType() == PolarisEntityType.NAMESPACE) {
      return !config.getConfig(FeatureConfiguration.ALLOW_NAMESPACE_LOCATION_OVERLAP);
    }
    if (entity.getSubType() == PolarisEntitySubType.ICEBERG_VIEW
        && !config.getConfig(BehaviorChangeConfiguration.VALIDATE_VIEW_LOCATION_OVERLAP))
      return false;
    PolarisBaseEntity catalog =
        store.lookupEntityInCurrentTxn(
            context,
            PolarisEntityConstants.getNullId(),
            entity.getCatalogId(),
            PolarisEntityType.CATALOG.getCode());
    return !config.getConfig(
        FeatureConfiguration.ALLOW_TABLE_LOCATION_OVERLAP, CatalogEntity.of(catalog));
  }

  private static Set<String> locations(PolarisBaseEntity entity) {
    if (entity.getType() == PolarisEntityType.CATALOG) {
      CatalogEntity catalog = CatalogEntity.of(entity);
      Set<String> result = new HashSet<>();
      result.add(catalog.getBaseLocation());
      if (catalog.getStorageConfigurationInfo() != null) {
        result.addAll(catalog.getStorageConfigurationInfo().getAllowedLocations());
      }
      result.remove(null);
      return result.stream().map(StorageLocation::ensureTrailingSlash).collect(Collectors.toSet());
    }
    if (entity.getType() == PolarisEntityType.TABLE_LIKE
        && entity.getSubType() == PolarisEntitySubType.ICEBERG_TABLE) {
      return StorageUtil.getLocationsUsedByTable(
          entity.getPropertiesAsMap().get(PolarisEntityConstants.ENTITY_BASE_LOCATION),
          entity.getInternalPropertiesAsMap());
    }
    return baseLocation(entity);
  }

  private static Set<String> baseLocation(PolarisBaseEntity entity) {
    if (entity.getType() != PolarisEntityType.NAMESPACE
        && !(entity.getType() == PolarisEntityType.TABLE_LIKE
            && (entity.getSubType() == PolarisEntitySubType.ICEBERG_TABLE
                || entity.getSubType() == PolarisEntitySubType.ICEBERG_VIEW))) return Set.of();
    String location = entity.getPropertiesAsMap().get(PolarisEntityConstants.ENTITY_BASE_LOCATION);
    // View metadata carries a validation fact, not a new storage-root restriction. Persisting it
    // as public baseLocation would change existing allowed metadata-path behavior.
    if (entity.getSubType() == PolarisEntitySubType.ICEBERG_VIEW) {
      location =
          entity
              .getInternalPropertiesAsMap()
              .getOrDefault(IcebergTableLikeEntity.LOCATION, location);
    }
    return location == null ? Set.of() : Set.of(StorageLocation.ensureTrailingSlash(location));
  }

  private static boolean overlaps(String left, String right) {
    StorageLocation a = StorageLocation.of(left);
    StorageLocation b = StorageLocation.of(right);
    return a.isChildOf(b) || b.isChildOf(a);
  }

  private static PolarisEntityId id(PolarisBaseEntity entity) {
    return new PolarisEntityId(entity.getCatalogId(), entity.getId());
  }
}
