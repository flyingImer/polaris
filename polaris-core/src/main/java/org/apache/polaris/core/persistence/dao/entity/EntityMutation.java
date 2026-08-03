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
package org.apache.polaris.core.persistence.dao.entity;

import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * One entity mutation within a {@code commitChangeSet} call (see {@code
 * DurablePrimitives#commitChangeSet}).
 *
 * <p>{@link #originalEntity} generalizes, to a list, the compare-and-swap baseline already carried
 * by {@code DurablePrimitives#writeEntity}'s {@code originalEntity} parameter: for an {@link
 * MutationType#UPDATE}, it must be the object the caller most recently read for this entity, not a
 * value reconstructed or re-derived afterwards. That provenance requirement is why {@link #update}
 * requires it non-null; a change-set implementation is expected to fail the whole commit if the
 * persisted state no longer matches this baseline, not to apply the update against whatever the
 * current state happens to be.
 *
 * @param entity the entity's target state after this mutation
 * @param originalEntity the read that justified this mutation, for {@link MutationType#UPDATE}
 *     only; {@code null} for {@link MutationType#CREATE} and {@link MutationType#DELETE}
 * @param type which kind of mutation this is
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

  /** A brand-new entity; nothing has been read for it yet. */
  public static EntityMutation create(@NonNull PolarisBaseEntity entity) {
    return new EntityMutation(entity, null, MutationType.CREATE);
  }

  /**
   * An update to a pre-existing entity.
   *
   * @param originalEntity the object the caller read just before building this mutation; the
   *     compare-and-swap baseline the commit validates against
   */
  public static EntityMutation update(
      @NonNull PolarisBaseEntity entity, @NonNull PolarisBaseEntity originalEntity) {
    return new EntityMutation(entity, originalEntity, MutationType.UPDATE);
  }

  /** A delete of a pre-existing entity. */
  public static EntityMutation delete(@NonNull PolarisBaseEntity entity) {
    return new EntityMutation(entity, null, MutationType.DELETE);
  }
}
