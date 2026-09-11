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
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.persistence.dao.entity.EntityWithPath;

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

  /** Records one table's entity update instead of persisting it now. */
  void stage(List<PolarisEntityCore> catalogPath, PolarisBaseEntity entity) {
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
