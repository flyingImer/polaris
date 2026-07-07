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

import com.google.common.annotations.VisibleForTesting;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.polaris.core.exceptions.PolarisException;
import org.apache.polaris.exceptions.PolarisBadRequestException;
import org.apache.polaris.exceptions.PolarisConflictException;
import org.apache.polaris.exceptions.PolarisNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * An {@link ExceptionMapper} implementation for {@link PolarisException}s modeled after {@link
 * IcebergExceptionMapper}
 */
@Provider
public class PolarisExceptionMapper implements ExceptionMapper<PolarisException> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisExceptionMapper.class);

  private Response.Status getStatus(PolarisException exception) {
    // The mapper classifies purely by the three HTTP-semantic base exceptions: any error, OSS or
    // provider, that extends one of them inherits the right status with no new arm here. Errors
    // that
    // are genuinely internal (for example a persistence failure) extend none of the bases and fall
    // to the default 500, which is the intended behavior for a server-side fault.
    return switch (exception) {
      case PolarisNotFoundException notFound -> Response.Status.NOT_FOUND;
      case PolarisConflictException conflict -> Response.Status.CONFLICT;
      case PolarisBadRequestException badRequest -> Response.Status.BAD_REQUEST;
      default -> Response.Status.INTERNAL_SERVER_ERROR;
    };
  }

  /**
   * The wire {@code type}. For the client-facing semantic bases it is the stable declared {@code
   * errorCode()}, so a class rename cannot silently change the wire contract. The default applies
   * only to internal (500) errors, whose {@code type} is not a client contract; there it falls back
   * to the class simple name.
   */
  private static String wireType(PolarisException exception) {
    return switch (exception) {
      case PolarisNotFoundException notFound -> notFound.errorCode();
      case PolarisConflictException conflict -> conflict.errorCode();
      case PolarisBadRequestException badRequest -> badRequest.errorCode();
      default -> exception.getClass().getSimpleName();
    };
  }

  @Override
  public Response toResponse(PolarisException exception) {
    Response.Status status = getStatus(exception);
    getLogger()
        .atLevel(
            status.getFamily() == Response.Status.Family.SERVER_ERROR ? Level.INFO : Level.DEBUG)
        .setCause(exception)
        .log("Full PolarisException");

    ErrorResponse errorResponse =
        ErrorResponse.builder()
            .responseCode(status.getStatusCode())
            .withType(wireType(exception))
            .withMessage(exception.getMessage())
            .build();
    return Response.status(status)
        .entity(errorResponse)
        .type(MediaType.APPLICATION_JSON_TYPE)
        .build();
  }

  @VisibleForTesting
  Logger getLogger() {
    return LOGGER;
  }
}
