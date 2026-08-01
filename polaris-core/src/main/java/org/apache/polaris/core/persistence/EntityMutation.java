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
package org.apache.polaris.core.persistence;

import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * A single entity mutation (create, update, or delete) as part of an atomic {@link
 * BasePersistence#commitChangeSet} operation.
 *
 * <p>For {@link MutationType#UPDATE}, {@code originalEntity} is not just present, it must be the
 * exact entity object the caller most recently read (e.g. via {@link
 * BasePersistence#lookupEntity}) before deciding to mutate it. It is the compare-and-swap baseline,
 * not a value independently reconstructed or re-derived after the fact. A caller that wants to
 * update an entity it has not itself just read (for example, because it is composing a change from
 * a request payload) must read it first and use that read's result as {@code originalEntity} — this
 * type intentionally offers no way to build an update mutation without one.
 *
 * @param entity the entity to write or delete
 * @param originalEntity the baseline read for CAS comparison on updates; must be null for creates
 *     and deletes, and must be non-null for updates
 * @param type the type of mutation
 */
public record EntityMutation(
    @NonNull PolarisBaseEntity entity,
    @Nullable PolarisBaseEntity originalEntity,
    @NonNull MutationType type) {

  public enum MutationType {
    CREATE,
    UPDATE,
    DELETE
  }

  /** A new entity mutation (originalEntity is null; nothing has been read yet). */
  public static EntityMutation create(@NonNull PolarisBaseEntity entity) {
    return new EntityMutation(entity, null, MutationType.CREATE);
  }

  /**
   * An update mutation. {@code originalEntity} must be the entity object the caller read just
   * before deciding on {@code entity}'s new state, not a separately reconstructed value.
   */
  public static EntityMutation update(
      @NonNull PolarisBaseEntity entity, @NonNull PolarisBaseEntity originalEntity) {
    return new EntityMutation(entity, originalEntity, MutationType.UPDATE);
  }

  /** A delete mutation. */
  public static EntityMutation delete(@NonNull PolarisBaseEntity entity) {
    return new EntityMutation(entity, null, MutationType.DELETE);
  }
}
