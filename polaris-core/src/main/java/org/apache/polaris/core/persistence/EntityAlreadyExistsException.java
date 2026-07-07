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
import org.apache.polaris.exceptions.PolarisConflictException;

/**
 * Exception raised when a conflict in the persistence layer prevents the attempted creation of a
 * new entity; provides a member holding the conflicting entity.
 *
 * <p>It is a {@link PolarisConflictException}, so anywhere it reaches the error mapper it renders
 * as HTTP 409 with the stable wire code {@code entity.already_exists} and needs no dedicated mapper
 * arm. Before this it extended {@code RuntimeException} directly and sat outside the Polaris error
 * family. On today's create paths it is caught inside {@code AtomicOperationMetaStoreManager} and
 * converted to a result status (the catalog then throws an Iceberg-native exception), so this
 * change is about correct family membership, not a client-visible status flip on those paths.
 */
public class EntityAlreadyExistsException extends PolarisConflictException {
  private static final String ERROR_CODE = "entity.already_exists";

  private final PolarisBaseEntity existingEntity;

  /**
   * @param existingEntity The conflicting entity that caused creation to fail.
   */
  public EntityAlreadyExistsException(PolarisBaseEntity existingEntity) {
    super(ERROR_CODE, message(existingEntity));
    this.existingEntity = existingEntity;
  }

  public EntityAlreadyExistsException(PolarisBaseEntity existingEntity, Throwable cause) {
    super(ERROR_CODE, message(existingEntity), cause);
    this.existingEntity = existingEntity;
  }

  private static String message(PolarisBaseEntity existingEntity) {
    return existingEntity.getName() + ":" + existingEntity.getId();
  }

  public PolarisBaseEntity getExistingEntity() {
    return this.existingEntity;
  }
}
