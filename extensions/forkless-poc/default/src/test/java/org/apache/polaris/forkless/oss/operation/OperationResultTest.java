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
package org.apache.polaris.forkless.oss.operation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;
import org.apache.polaris.operation.model.OperationMetadata;
import org.apache.polaris.operation.model.OperationResult;
import org.junit.jupiter.api.Test;

class OperationResultTest {

  /** Stand-in for an Iceberg response type; the wrapper is generic over whatever the op returns. */
  private record LoadTableResponse(String metadataLocation) {}

  // Local-style op: derives an etag from the metadata location, no provider payload.
  private static OperationResult<LoadTableResponse> localLoad(String metadataLocation) {
    String etag = "\"" + Integer.toHexString(metadataLocation.hashCode()) + "\"";
    return new OperationResult<>(
        new LoadTableResponse(metadataLocation),
        new OperationMetadata(Optional.of(etag), Optional.empty()));
  }

  // Federated-style op: forwards a remote etag plus an opaque provider payload.
  private static OperationResult<LoadTableResponse> federatedLoad(
      String metadataLocation, String remoteEtag, Object providerPayload) {
    return new OperationResult<>(
        new LoadTableResponse(metadataLocation),
        new OperationMetadata(Optional.of(remoteEtag), Optional.of(providerPayload)));
  }

  @Test
  void localOpCarriesDerivedEtagAndNoProviderPayload() {
    OperationResult<LoadTableResponse> result = localLoad("s3://bucket/table/metadata/v1.json");

    assertThat(result.icebergResponse().metadataLocation())
        .isEqualTo("s3://bucket/table/metadata/v1.json");
    assertThat(result.metadata().etag()).isPresent();
    assertThat(result.metadata().providerPayload()).isEmpty();
  }

  @Test
  void federatedOpCarriesRemoteEtagAndOpaqueProviderPayload() {
    Object payload = Map.of("remoteTxnId", "abc-123");
    OperationResult<LoadTableResponse> result =
        federatedLoad("s3://remote/table/metadata/v9.json", "\"remote-etag\"", payload);

    assertThat(result.metadata().etag()).contains("\"remote-etag\"");
    assertThat(result.metadata().providerPayload()).containsSame(payload);
  }
}
