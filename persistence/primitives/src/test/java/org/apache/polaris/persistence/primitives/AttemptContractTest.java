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

import static org.apache.polaris.persistence.primitives.api.DurablePrimitives.Mutation.delete;
import static org.apache.polaris.persistence.primitives.api.DurablePrimitives.Mutation.put;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.persistence.primitives.api.DurablePrimitives;
import org.apache.polaris.persistence.primitives.api.StorageFailure;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AttemptContractTest {
  private DurablePrimitives backend;
  private String prefix;

  @BeforeEach
  void setup() {
    backend = NativeBackends.open(System.getProperty("poc.backend", "h2"));
    prefix = "contract/" + UUID.randomUUID() + "/";
  }

  @AfterEach
  void close() {
    if (backend != null) backend.close();
  }

  @Test
  void closeAbortsNativeLegacyWrites() {
    try (var attempt = backend.beginLegacy()) {
      attempt.applyForLegacyReadYourWrites(List.of(put(prefix + "key", new byte[] {1})));
      assertThat(attempt.get(prefix + "key")).containsExactly((byte) 1);
    }
    try (var read = backend.begin()) {
      assertThat(read.get(prefix + "key")).isNull();
      read.commit(List.of());
    }
  }

  @Test
  void terminalBatchPublishesMixedMutations() {
    try (var attempt = backend.begin()) {
      assertThat(attempt.get(prefix + "a")).isNull();
      attempt.commit(List.of(put(prefix + "a", new byte[] {1}), put(prefix + "b", new byte[] {2})));
    }
    try (var attempt = backend.begin()) {
      assertThat(attempt.get(prefix + "a")).containsExactly((byte) 1);
      attempt.commit(List.of(delete(prefix + "a"), put(prefix + "b", new byte[] {3})));
    }
    try (var read = backend.begin()) {
      assertThat(read.get(prefix + "a")).isNull();
      assertThat(read.get(prefix + "b")).containsExactly((byte) 3);
      read.commit(List.of());
    }
  }

  @Test
  void emptyRangeAndUnchangedParentPreventOrphan() throws Exception {
    String name = System.getProperty("poc.backend", "h2");
    Assumptions.assumeTrue(
        name.equals("fdb") || name.equals("jdbc"),
        "H2 is not an isolation proof; Spanner emulator database-wide locks cannot run this schedule");
    String parent = prefix + "parent";
    String children = prefix + "children/";
    try (var attempt = backend.begin()) {
      attempt.commit(List.of(put(parent, new byte[] {1})));
    }
    var observed = new CountDownLatch(2);
    var workers = Executors.newFixedThreadPool(2);
    try {
      var deletion =
          workers.submit(
              () -> {
                try (var attempt = backend.begin()) {
                  assertThat(attempt.scan(children, children + "~", 1)).isEmpty();
                  observed.countDown();
                  if (!observed.await(15, TimeUnit.SECONDS))
                    throw new AssertionError("Schedule did not rendezvous");
                  attempt.commit(List.of(delete(parent)));
                  return true;
                } catch (StorageFailure e) {
                  assertThat(e.outcome()).isEqualTo(StorageFailure.Outcome.CONFLICT);
                  return false;
                }
              });
      var creation =
          workers.submit(
              () -> {
                try (var attempt = backend.begin()) {
                  assertThat(attempt.get(parent)).isNotNull();
                  observed.countDown();
                  if (!observed.await(15, TimeUnit.SECONDS))
                    throw new AssertionError("Schedule did not rendezvous");
                  attempt.commit(List.of(put(children + "child", new byte[] {2})));
                  return true;
                } catch (StorageFailure e) {
                  assertThat(e.outcome()).isEqualTo(StorageFailure.Outcome.CONFLICT);
                  return false;
                }
              });
      assertThat(deletion.get(30, TimeUnit.SECONDS) && creation.get(30, TimeUnit.SECONDS))
          .isFalse();
      try (var read = backend.begin()) {
        if (read.get(children + "child") != null) assertThat(read.get(parent)).isNotNull();
        read.commit(List.of());
      }
    } finally {
      workers.shutdownNow();
    }
  }
}
