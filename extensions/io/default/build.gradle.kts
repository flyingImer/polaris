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

  // The StorageIoProvider SPI, VendedServerStorageAccess model, FileIOUnknownHostException,
  // and the Azure/StorageLocation types the WASB translation relies on all live in polaris-core.
  implementation(project(":polaris-core"))

  // Compile-time Iceberg surface: FileIO decoration and CatalogUtil.loadFileIO.
  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-core")

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  implementation(platform(libs.opentelemetry.bom))
  implementation("io.opentelemetry:opentelemetry-api")

  // Storage SDK backends loaded reflectively by CatalogUtil.loadFileIO at runtime. Kept with the
  // default IO impl so this module is a self-contained peer of provider IO extensions behind the
  // StorageIoProvider SPI.
  runtimeOnly("org.apache.iceberg:iceberg-aws")
  runtimeOnly("software.amazon.awssdk:s3")
  runtimeOnly(platform(libs.azuresdk.bom))
  runtimeOnly("com.azure:azure-core")
  runtimeOnly("com.azure:azure-storage-blob")
  runtimeOnly("com.azure:azure-storage-file-datalake")

  // Promoted to compile-time (not just runtimeOnly, like the reflectively-loaded FileIO backends
  // above): the s3/gcs StorageCredentialVendorFactory beans call the AWS STS / GCP credentials SDK
  // types directly. Only the SDK types themselves are needed at compile time (no client/credential
  // wiring) -- mirrors the coordinates runtime/service already pulls in for the same SDKs
  // and the pattern extensions/catalog/iceberg-default/build.gradle.kts already uses for the same
  // reason. (software.amazon.awssdk:sts's own transitive graph brings in the auth/credentials
  // types the AWS factory's test constructor needs; s3 itself stays runtimeOnly above, it is not
  // touched by credential vending. The awssdk BOM moves here too -- implementation's version
  // constraints also cover runtimeOnly's s3 coordinate above, since runtimeClasspath extends
  // implementation.)
  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:sts")
  implementation(platform(libs.google.cloud.storage.bom))
  implementation("com.google.cloud:google-cloud-storage")

  // CDI is provided by the serving runtime that aggregates this extension (see runtime/service).
  compileOnly(libs.jspecify)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.smallrye.common.annotation)
  // StorageCredentialVendorConfig's @ConfigMapping (the AWS/GCP credential-vending half of
  // polaris.storage, moved here alongside this module's own vendor factory beans -- see that
  // class's javadoc). Precedent: extensions/auth/opa, extensions/auth/ranger.
  compileOnly(libs.smallrye.config.core)
  compileOnly(project(":polaris-config-docs-annotations"))

  // Test coverage for StorageCredentialVendorConfig, moved here from
  // runtime/service's StorageConfigurationTest alongside the interface itself.
  testImplementation(platform(libs.junit.bom))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation(libs.assertj.core)
  testImplementation(libs.mockito.core)
}
