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

import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.jspecify.annotations.NonNull;

/**
 * One grant-record mutation within a {@code commitChangeSet} call (see {@code
 * DurablePrimitives#commitChangeSet}). Grant records have no independent CAS baseline: per {@code
 * DurablePrimitives#writeToGrantRecords}, all fields of a grant record are part of its primary key,
 * so a create either inserts a new record or is a no-op against an identical existing one.
 *
 * @param grantRecord the grant record to create or delete
 * @param type which kind of mutation this is
 */
public record GrantMutation(@NonNull PolarisGrantRecord grantRecord, @NonNull MutationType type) {

  public enum MutationType {
    CREATE,
    DELETE
  }

  public static GrantMutation create(@NonNull PolarisGrantRecord grantRecord) {
    return new GrantMutation(grantRecord, MutationType.CREATE);
  }

  public static GrantMutation delete(@NonNull PolarisGrantRecord grantRecord) {
    return new GrantMutation(grantRecord, MutationType.DELETE);
  }
}
