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

// polaris-server (Java 21): the gate tests wire the relational-jdbc store, a polaris-server
// module, which a Java-17 polaris-client test classpath cannot resolve; every server-side
// extensions impl uses polaris-server.
plugins { id("polaris-server") }

dependencies {
  implementation(project(":polaris-core"))
  compileOnly(libs.jspecify)

  testImplementation(project(":polaris-extensions-orchestration-default"))
  testImplementation(project(":polaris-extensions-primitives-routing-default"))
  testImplementation(project(":polaris-treemap"))
  testImplementation(project(":polaris-relational-jdbc"))
  testImplementation(libs.h2)
  testImplementation(testFixtures(project(":polaris-core")))
}
