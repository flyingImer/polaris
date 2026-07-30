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
package org.apache.polaris.core.storage;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

@PolarisImmutable
public interface StorageAccessConfig {
  Map<String, String> credentials();

  /**
   * Configuration properties returned to the client alongside any vended credentials (e.g. surfaced
   * in the Iceberg REST {@code LoadTableResponse.config()}) -- the client-visible counterpart to
   * {@link #internalProperties()}, which never leaves the server.
   *
   * <p>A {@link StorageCredentialVendor} implementation MAY extend this channel with standard
   * properties that its own backend needs the client to have -- e.g. encryption metadata a
   * provider's backend requires the client to see -- and doing so is an IN-CONTRACT use of {@code
   * extraProperties()}, not an out-of-band injection. Concretely: a vendor implementation may hold
   * backend-specific encryption metadata (say, an external volume's SSE mode and key id) that has
   * no representation in {@link #credentials()}; it translates that metadata into standard
   * Iceberg-recognized property keys (e.g. {@code s3.sse.type}, {@code s3.sse.key}) and adds them
   * here via {@link Builder#putExtraProperty}, so they reach the client through the same load
   * response as any vended credentials.
   */
  Map<String, String> extraProperties();

  /**
   * Configuration properties that are relevant only to the Polaris Server, but not to clients.
   * These properties override corresponding entries from {@link #extraProperties()}.
   */
  Map<String, String> internalProperties();

  Optional<Instant> expiresAt();

  /**
   * A two-state signal consulted only when {@link #credentials()} is empty; it carries no meaning
   * when credentials are present.
   *
   * <ul>
   *   <li>{@code true} -- empty credentials here is a bug. The caller expected this object to carry
   *       vended credentials and got none; something upstream failed to produce them.
   *   <li>{@code false} -- empty credentials here is a legitimate, non-error outcome: deployment
   *       policy disabled credential subscoping, no storage configuration resolved for the
   *       requested location, or the resolved backend has no credential concept to vend in the
   *       first place (e.g. a local filesystem).
   * </ul>
   *
   * <p>Defaults to {@code true} as a deliberate safety net: a code path that builds this object
   * without setting the flag and also fails to populate {@link #credentials()} should be treated as
   * buggy by default, not silently waved through as an intentional no-credentials outcome.
   */
  @Value.Default
  default boolean supportsCredentialVending() {
    return true;
  }

  default String get(StorageAccessProperty key) {
    if (key.isCredential()) {
      return credentials().get(key.getPropertyName());
    } else {
      String value = internalProperties().get(key.getPropertyName());
      return value != null ? value : extraProperties().get(key.getPropertyName());
    }
  }

  static StorageAccessConfig.Builder builder() {
    return ImmutableStorageAccessConfig.builder();
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder putCredential(String key, String value);

    @CanIgnoreReturnValue
    Builder putExtraProperty(String key, String value);

    @CanIgnoreReturnValue
    Builder putInternalProperty(String key, String value);

    @CanIgnoreReturnValue
    Builder expiresAt(Instant expiresAt);

    @CanIgnoreReturnValue
    Builder supportsCredentialVending(boolean supportsCredentialVending);

    default Builder put(StorageAccessProperty key, String value) {
      if (key.isExpirationTimestamp()) {
        expiresAt(Instant.ofEpochMilli(Long.parseLong(value)));
      }

      if (key.isCredential()) {
        return putCredential(key.getPropertyName(), value);
      } else {
        return putExtraProperty(key.getPropertyName(), value);
      }
    }

    StorageAccessConfig build();
  }
}
