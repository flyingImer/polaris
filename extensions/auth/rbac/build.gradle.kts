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

  // The authorizer SPI (PolarisAuthorizer), the resolution substrate (EntityResolver,
  // AuthorizationChain, the AuthorizationIntent/Request/Decision types) and the RBAC privilege
  // model all live in polaris-core. This module holds the built-in RBAC implementation of that SPI
  // as a swappable peer of the OPA and Ranger authorizers.
  implementation(project(":polaris-core"))

  // Iceberg ForbiddenException is the deny surface the authorizer throws through.
  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  // CDI is provided by the serving runtime that aggregates this extension (see runtime/service).
  // The @Identifier("internal") producer needs the CDI + smallrye annotations at compile time only.
  compileOnly(libs.jspecify)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.smallrye.common.annotation)

  testImplementation(testFixtures(project(":polaris-core")))
  testImplementation(platform(libs.junit.bom))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation(libs.assertj.core)
  testImplementation(libs.mockito.core)
}
