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
}

dependencies {
  implementation(project(":polaris-core"))
  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation(libs.postgresql)
  implementation("org.foundationdb:fdb-java:7.3.77")
  implementation("com.google.cloud:google-cloud-spanner:6.120.0")
  implementation(libs.smallrye.common.annotation)
  compileOnly(libs.jspecify)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.annotation.api)
  testImplementation(libs.h2)
  testImplementation(libs.mockito.junit.jupiter)
  testImplementation(testFixtures(project(":polaris-core")))
}

tasks.withType<Test>().configureEach {
  // Explicit local endpoints only; native tests skip when not configured.
  listOf(
      "poc.backend",
      "poc.jdbc.url",
      "poc.jdbc.user",
      "poc.jdbc.password",
      "poc.fdb.cluster",
      "poc.spanner.initialize",
      "poc.spanner.endpoint",
      "poc.spanner.database",
    )
    .forEach { key ->
      System.getProperty(key)?.let { systemProperty(key, it) }
    }
}

// Emulator-only diagnostic mode. The unfiltered suite remains the default and is also recorded.
// Its two concurrent worker tests are measured separately from serial workflow compatibility.
tasks.withType<Test>().configureEach {
  if (System.getProperty("poc.spanner.serial-fixtures") == "true") {
    filter.excludeTestsMatching("*.testCreateTasksInParallel")
    filter.excludeTestsMatching("*.testLoadTasksInParallel")
  }
}
