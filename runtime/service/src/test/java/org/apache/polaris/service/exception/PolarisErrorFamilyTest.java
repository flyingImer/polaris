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
package org.apache.polaris.service.exception;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.Response;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.exceptions.CommitConflictException;
import org.apache.polaris.core.persistence.EntityAlreadyExistsException;
import org.apache.polaris.core.persistence.IdempotencyPersistenceException;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.apache.polaris.exceptions.PolarisConflictException;
import org.apache.polaris.exceptions.PolarisNotFoundException;
import org.junit.jupiter.api.Test;

/**
 * Behavior of {@link PolarisExceptionMapper} over the semantic error family: HTTP status is chosen
 * by the semantic base (so a subclass needs no dedicated arm) and the wire {@code type} is the
 * stable declared {@code errorCode()}, never a class simple name. Also pins the bug fix: {@link
 * EntityAlreadyExistsException} and {@link RetryOnConcurrencyException} extended {@code
 * RuntimeException} directly and bypassed this mapper (falling to the RuntimeException mapper's 500
 * default on any path where they escape uncaught); folded under {@link PolarisConflictException}
 * they now reach this mapper and return 409.
 */
class PolarisErrorFamilyTest {

  private final PolarisExceptionMapper mapper = new PolarisExceptionMapper();

  private ErrorResponse wire(RuntimeException exception) {
    Response response =
        mapper.toResponse((org.apache.polaris.core.exceptions.PolarisException) exception);
    return (ErrorResponse) response.getEntity();
  }

  @Test
  void entityAlreadyExistsMapsTo409WithStableCodeNotFallthrough500() {
    PolarisBaseEntity existing =
        new PolarisBaseEntity(
            1L, 2L, PolarisEntityType.TABLE_LIKE, PolarisEntitySubType.NULL_SUBTYPE, 0L, "widget");

    ErrorResponse wire = wire(new EntityAlreadyExistsException(existing));

    assertThat(wire.code()).isEqualTo(409);
    assertThat(wire.code()).isNotEqualTo(500);
    assertThat(wire.type()).isEqualTo("entity.already_exists");
  }

  @Test
  void retryOnConcurrencyMapsTo409WithStableCodeNotFallthrough500() {
    ErrorResponse wire = wire(new RetryOnConcurrencyException("retry %s", "now"));

    assertThat(wire.code()).isEqualTo(409);
    assertThat(wire.code()).isNotEqualTo(500);
    assertThat(wire.type()).isEqualTo("entity.concurrency_conflict");
  }

  @Test
  void conflictBaseMapsTo409AndWireTypeIsTheDeclaredCode() {
    PolarisConflictException e = new PolarisConflictException("entity.conflict", "boom");

    ErrorResponse wire = wire(e);

    assertThat(wire.code()).isEqualTo(409);
    assertThat(wire.type()).isEqualTo("entity.conflict");
    // The wire type is the stable declared code, not the class simple name.
    assertThat(wire.type()).isNotEqualTo(e.getClass().getSimpleName());
  }

  @Test
  void domainSubclassOfNotFoundInheritsMappingWithNoDedicatedArm() {
    ErrorResponse wire = wire(new TestNotFound());

    assertThat(wire.code()).isEqualTo(404);
    assertThat(wire.type()).isEqualTo("test.not_found");
    assertThat(wire.type()).isNotEqualTo("TestNotFound");
  }

  @Test
  void foldedLeafEmitsItsStableCodeNotTheSimpleName() {
    // A former per-class leaf, now folded under a base, is classified by the base (409) and emits
    // its stable code as the wire type instead of the class simple name.
    CommitConflictException e = new CommitConflictException("still a conflict");

    ErrorResponse wire = wire(e);

    assertThat(wire.code()).isEqualTo(409);
    assertThat(wire.type()).isEqualTo("commit.conflict");
    assertThat(wire.type()).isNotEqualTo(e.getClass().getSimpleName());
  }

  @Test
  void genuinelyInternalErrorStaysOutsideTheClientFamilyAs500() {
    // The family is client-facing (404/409/400). An internal persistence failure is not a client
    // error, so it deliberately extends no base and falls to the mapper default (500).
    IdempotencyPersistenceException e = new IdempotencyPersistenceException("store unavailable");

    ErrorResponse wire = wire(e);

    assertThat(wire.code()).isEqualTo(500);
  }

  /**
   * A not-found domain error declared outside the mapper: no dedicated switch arm exists for it.
   */
  private static final class TestNotFound extends PolarisNotFoundException {
    TestNotFound() {
      super("test.not_found", "no such thing");
    }
  }
}
