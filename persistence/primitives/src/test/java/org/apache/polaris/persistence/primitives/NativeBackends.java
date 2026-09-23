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
package org.apache.polaris.persistence.primitives;

import java.util.Properties;
import org.apache.polaris.persistence.primitives.api.DurablePrimitives;
import org.apache.polaris.persistence.primitives.fdb.FdbPrimitives;
import org.apache.polaris.persistence.primitives.jdbc.JdbcPrimitives;
import org.apache.polaris.persistence.primitives.spanner.SpannerPrimitives;

final class NativeBackends {
  private static boolean spannerInitialized;

  static synchronized DurablePrimitives open(String name) {
    return switch (name) {
      case "jdbc", "h2" -> {
        String url =
            name.equals("h2")
                ? "jdbc:h2:mem:"
                    + java.util.UUID.randomUUID()
                    + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
                : required("poc.jdbc.url");
        var properties = new Properties();
        properties.setProperty(
            "user", System.getProperty("poc.jdbc.user", name.equals("h2") ? "sa" : "root"));
        properties.setProperty("password", System.getProperty("poc.jdbc.password", ""));
        var result = new JdbcPrimitives(url, properties);
        result.initialize();
        yield result;
      }
      case "fdb" -> new FdbPrimitives(required("poc.fdb.cluster"));
      case "spanner" -> {
        if (Boolean.getBoolean("poc.spanner.initialize") && !spannerInitialized) {
          SpannerPrimitives.initializeEmulator(
              required("poc.spanner.endpoint"), required("poc.spanner.database"));
          spannerInitialized = true;
        }
        yield new SpannerPrimitives(
            required("poc.spanner.endpoint"), required("poc.spanner.database"));
      }
      default -> throw new IllegalArgumentException("Unknown native backend " + name);
    };
  }

  private static String required(String key) {
    String value = System.getProperty(key);
    if (value == null) throw new IllegalArgumentException("Missing " + key);
    return value;
  }
}
