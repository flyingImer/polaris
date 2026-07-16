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

import com.azure.core.exception.HttpResponseException;
import com.google.cloud.BaseServiceException;
import com.google.cloud.storage.StorageException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.Locale;
import java.util.Set;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Cloud-storage-provider exception classification, shared by {@link BasePolarisIcebergCatalog} and
 * {@link BridgeBaseMetastoreViewCatalog} to decide whether a caught storage exception is retryable.
 * Split out of {@code org.apache.polaris.service.exception.IcebergExceptionMapper} (which remains
 * in runtime/service as the JAX-RS {@code @Provider}) because that class's broader
 * JAX-RS-response-code-mapping logic isn't needed here, and pulling the whole class (plus its
 * jakarta.ws.rs dependency) into this module for one predicate would be backwards; the
 * AWS/GCS/Azure SDK types this predicate switches on, however, are needed at compile time, so this
 * module declares them directly (mirroring runtime/service's exact coordinates).
 */
public final class StorageProviderExceptionClassifier {
  private StorageProviderExceptionClassifier() {}

  public static final int UNKNOWN_CLOUD_HTTP_CODE = -1;

  public static final Set<Integer> RETRYABLE_AZURE_HTTP_CODES =
      Set.of(408, 429, 500, 503, 504, UNKNOWN_CLOUD_HTTP_CODE);

  // Case-insensitive parts of exception messages that a request to a cloud provider was denied
  // due to lack of permissions.
  private static final Set<String> ACCESS_DENIED_HINTS =
      Set.of("access denied", "not authorized", "forbidden");

  public static boolean containsAnyAccessDeniedHint(String message) {
    String messageLower = message.toLowerCase(Locale.ENGLISH);
    return ACCESS_DENIED_HINTS.stream().anyMatch(messageLower::contains);
  }

  @VisibleForTesting
  public static Collection<String> getAccessDeniedHints() {
    return ImmutableSet.copyOf(ACCESS_DENIED_HINTS);
  }

  public static int extractHttpCodeFromCloudException(Throwable t) {
    return switch (t) {
      case S3Exception s3e -> s3e.statusCode();
      case HttpResponseException hre -> hre.getResponse().getStatusCode();
      case StorageException se -> se.getCode();
      default -> UNKNOWN_CLOUD_HTTP_CODE;
    };
  }

  /**
   * Check if the Throwable is retryable for the storage provider.
   *
   * @param t the Throwable
   * @return true if the Throwable is retryable
   */
  public static boolean isStorageProviderRetryableException(Throwable t) {
    if (t == null) {
      return false;
    }

    if (t.getMessage() != null && containsAnyAccessDeniedHint(t.getMessage())) {
      return false;
    }

    return switch (t) {
      // GCS
      case BaseServiceException bse -> bse.isRetryable();

      // S3
      case SdkException se -> se.retryable();

      // Azure exceptions don't have a retryable property so we just check the HTTP code
      case HttpResponseException hre ->
          RETRYABLE_AZURE_HTTP_CODES.contains(extractHttpCodeFromCloudException(hre));
      default -> true;
    };
  }
}
