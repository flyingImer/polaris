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

import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.jspecify.annotations.NonNull;

/**
 * Durable manager for task leasing: loads the tasks an executor may run and records the lease on
 * each. Owns the lease-window rule and knows no storage topology.
 */
public interface TaskDurableManager {

  /**
   * Fetch a list of tasks to be completed. Tasks
   *
   * @param callCtx call context
   * @param executorId executor id
   * @param pageToken page token to start after
   * @return list of tasks to be completed
   */
  @NonNull EntitiesResult loadTasks(
      @NonNull PolarisCallContext callCtx, String executorId, PageToken pageToken);
}
