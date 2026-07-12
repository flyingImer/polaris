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
package org.apache.polaris.service.events;

import com.google.common.reflect.TypeToken;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTMessage;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.polaris.core.admin.model.AddGrantRequest;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogRole;
import org.apache.polaris.core.admin.model.CreatePrincipalRoleRequest;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.RevokeGrantRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRoleRequest;
import org.apache.polaris.core.entity.PolarisPrivilege;

/**
 * Whitelist of types allowed for event attributes. The built-in set covers primitives, Iceberg spec
 * types and Polaris admin-model types, all nameable from this class's own module. Served-REST
 * request/response types (policy, generic-table, notification, ...) live in modules that depend on
 * this one, so this class cannot name them without a dependency cycle; {@link #register} lets a
 * caller in a module that CAN see them extend the whitelist before constructing an {@link
 * AttributeKey} of that type.
 */
final class AllowedAttributeTypes {
  private AllowedAttributeTypes() {}

  static final Set<Class<?>> ALLOWED_TYPES =
      Set.of(
          // Primitives
          String.class,
          Boolean.class,
          Number.class,
          // Iceberg types
          RESTMessage.class,
          Namespace.class,
          TableIdentifier.class,
          TableMetadata.class,
          ViewMetadata.class,
          // Polaris admin model types
          Catalog.class,
          Principal.class,
          PrincipalRole.class,
          CatalogRole.class,
          GrantResource.class,
          UpdatePrincipalRequest.class,
          CreatePrincipalRoleRequest.class,
          UpdatePrincipalRoleRequest.class,
          UpdateCatalogRequest.class,
          UpdateCatalogRoleRequest.class,
          AddGrantRequest.class,
          RevokeGrantRequest.class,
          PolarisPrivilege.class);

  private static final Set<Class<?>> REGISTERED_TYPES = ConcurrentHashMap.newKeySet();

  /**
   * Extends the whitelist with a type this class cannot name directly. Must run before any {@link
   * AttributeKey} of that type is constructed (e.g. from a static initializer that precedes the key
   * declarations, in the caller's class).
   */
  static void register(Class<?>... types) {
    REGISTERED_TYPES.addAll(List.of(types));
  }

  private static final Set<Class<?>> COLLECTION_TYPES = Set.of(List.class, Set.class, Map.class);

  static boolean isAllowed(TypeToken<?> type) {
    Class<?> rawType = type.getRawType();
    if (COLLECTION_TYPES.contains(rawType)) {
      for (var typeParam : rawType.getTypeParameters()) {
        TypeToken<?> resolvedType = type.resolveType(typeParam);
        if (!isSubtypeOfAllowedType(resolvedType.getRawType())) {
          return false;
        }
      }
      return true;
    }
    return isSubtypeOfAllowedType(rawType);
  }

  // No ClassValue cache here (unlike the fixed built-in set, REGISTERED_TYPES can grow after a
  // class has already been checked, so a permanent per-Class cache could go stale).
  private static boolean isSubtypeOfAllowedType(Class<?> rawType) {
    return ALLOWED_TYPES.stream().anyMatch(t -> t.isAssignableFrom(rawType))
        || REGISTERED_TYPES.stream().anyMatch(t -> t.isAssignableFrom(rawType));
  }
}
