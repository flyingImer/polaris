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
package org.apache.polaris.authz.model;

import java.util.List;
import java.util.Objects;

/**
 * One unit of authorization intent, expressed by names only: a privilege plus, where relevant, a
 * securable path. Sealed to a closed set of shapes so an authorizer can decide every variant
 * without an open-ended visitor.
 */
public sealed interface AuthorizationIntent
    permits AuthorizationIntent.SingleTarget,
        AuthorizationIntent.Rename,
        AuthorizationIntent.Targetless {

  /** The privilege being requested. */
  String privilege();

  /** Intent against one securable, addressed by path. */
  record SingleTarget(String privilege, List<String> securablePath) implements AuthorizationIntent {
    public SingleTarget {
      Objects.requireNonNull(privilege, "privilege must be non-null");
      securablePath = List.copyOf(securablePath);
    }
  }

  /** Intent to move a securable from one path to another; both endpoints are authorized. */
  record Rename(String privilege, List<String> fromPath, List<String> toPath)
      implements AuthorizationIntent {
    public Rename {
      Objects.requireNonNull(privilege, "privilege must be non-null");
      fromPath = List.copyOf(fromPath);
      toPath = List.copyOf(toPath);
    }
  }

  /** Intent with no securable target (for example a server-wide privilege). */
  record Targetless(String privilege) implements AuthorizationIntent {
    public Targetless {
      Objects.requireNonNull(privilege, "privilege must be non-null");
    }
  }
}
