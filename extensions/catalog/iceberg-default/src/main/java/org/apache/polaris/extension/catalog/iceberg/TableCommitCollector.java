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

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.core.storage.StorageUtil;

/**
 * Collects the per-table entity updates of one multi-table transaction so they can be committed as
 * a single atomic unit.
 *
 * <p>A multi-table commit applies each table's metadata changes through that table's own operations
 * object, which would ordinarily persist one entity per table. This collector receives those entity
 * updates instead, and the caller commits the whole list once.
 *
 * <p>Not thread-safe, and scoped to a single request: one instance serves one transaction.
 */
final class TableCommitCollector {

  private final List<EntityWithPath> staged = new ArrayList<>();

  /**
   * Every location the staged tables requested, in staging order. A table contributes as many
   * entries as it declares locations, so this list is not index-aligned with {@link #staged}.
   */
  private final List<StorageLocation> stagedLocations = new ArrayList<>();

  /**
   * Records one table's entity update instead of persisting it now, refusing a table whose base
   * location overlaps one this transaction has already staged.
   *
   * <p>The per-table overlap validation cannot see this case. It resolves siblings from committed
   * rows, so two tables relocating into the same directory in one request each find the other still
   * at its old location, and both pass. Nothing afterwards re-examines the set, and the single
   * commit that follows carries conditions on each table's own version but none on either location
   * — so there is no writer to lose the race and no ordering that would explain the outcome. The
   * check belongs here because this is the only point that sees the whole request's locations
   * before anything is persisted.
   *
   * @throws ForbiddenException if the entity's base location is a prefix or a suffix of the base
   *     location of a table staged earlier in this transaction
   */
  void stage(List<PolarisEntityCore> catalogPath, IcebergTableLikeEntity entity) {
    // The same location set the per-table validation checks against committed siblings: the base
    // location plus any user-specified write paths, which the entity carries in its internal
    // properties. Checking only the base location would miss two tables pointing one write path at
    // the same directory.
    List<StorageLocation> requested =
        StorageUtil.getLocationsUsedByTable(
                entity.getBaseLocation(), entity.getInternalPropertiesAsMap())
            .stream()
            .map(StorageLocation::of)
            .toList();
    for (StorageLocation staging : requested) {
      for (StorageLocation earlier : stagedLocations) {
        if (staging.isChildOf(earlier) || earlier.isChildOf(staging)) {
          throw new ForbiddenException(
              "Unable to create entity at location '%s' because it conflicts with another table in "
                  + "the same transaction at location '%s'",
              staging, earlier);
        }
      }
    }
    stagedLocations.addAll(requested);
    staged.add(new EntityWithPath(catalogPath, entity));
  }

  /**
   * The staged updates, in the order they were collected. The order is the caller's program: the
   * durable layer groups adjacent updates that share an atomicity domain into one commit, so
   * reordering here would change what is atomic.
   */
  List<EntityWithPath> staged() {
    return List.copyOf(staged);
  }
}
