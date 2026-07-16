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

plugins {
  id("polaris-server")
  id("org.kordamp.gradle.jandex")
  id("polaris-server-test-runner")
}

dependencies {
  polarisServer(project(path = ":polaris-server", configuration = "quarkusRunner"))

  // The IcebergCatalogOps/IcebergViewCatalogOps feature-SPI interfaces, PolarisResult /
  // ConditionalLoadOutcome / ExtensionPayload / NoExtension, and every substrate SPI this default
  // implementation composes (EntityResolver, StorageIoProvider, PolarisEventDispatcher, etc.) all
  // live in polaris-core.
  implementation(project(":polaris-core"))

  // polaris-core depends on this via `implementation` (non-transitive); BasePolarisIcebergCatalog
  // references org.apache.polaris.core.admin.model.Catalog directly (getConfig's catalog-type
  // check), so this module needs its own explicit dependency, same as runtime/service.
  implementation(project(":polaris-api-management-model"))

  // CatalogAuthorizer's extraPassThroughPolicies parameter is typed on PolicyIdentifier, an
  // OpenAPI-generated model type owned by this module (which already depends on polaris-core, so
  // this is not a cycle).
  implementation(project(":polaris-api-catalog-service"))

  // Iceberg-SDK surface: BaseMetastoreViewCatalog/SupportsNamespaces/SupportsNotifications for the
  // encapsulated bridge, plus the REST request/response types the feature-SPI operations pass
  // through unchanged.
  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-core")

  implementation(libs.guava)

  // StorageProviderExceptionClassifier switches on cloud-SDK exception types to classify
  // storage-provider errors as retryable. Only the SDK types themselves are needed at compile
  // time (no client/credential wiring) -- mirrors the coordinates runtime/service already pulls
  // in for the same SDKs.
  implementation(platform(libs.google.cloud.storage.bom))
  implementation("com.google.cloud:google-cloud-storage")
  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:s3")
  implementation(platform(libs.azuresdk.bom))
  implementation("com.azure:azure-core")

  // CDI is provided by the serving runtime that aggregates this extension (see runtime/service).
  compileOnly(libs.jspecify)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.smallrye.common.annotation)
}
