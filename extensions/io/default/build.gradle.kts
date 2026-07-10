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

  // Storage SDK backends loaded reflectively by CatalogUtil.loadFileIO at runtime. Kept with the
  // default IO impl so this module is a self-contained peer of provider IO extensions behind the
  // StorageIoProvider SPI.
  runtimeOnly("org.apache.iceberg:iceberg-aws")
  runtimeOnly(platform(libs.awssdk.bom))
  runtimeOnly("software.amazon.awssdk:s3")
  runtimeOnly("software.amazon.awssdk:sts")
  runtimeOnly(platform(libs.google.cloud.storage.bom))
  runtimeOnly("com.google.cloud:google-cloud-storage")
  runtimeOnly(platform(libs.azuresdk.bom))
  runtimeOnly("com.azure:azure-core")
  runtimeOnly("com.azure:azure-storage-blob")
  runtimeOnly("com.azure:azure-storage-file-datalake")

  // CDI is provided by the serving runtime that aggregates this extension (see runtime/service).
  compileOnly(libs.jspecify)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.smallrye.common.annotation)
}
