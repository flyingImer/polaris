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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.AbstractList;
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
  void readViewKeepsOneSnapshotAcrossConcurrentPublication() {
    Assumptions.assumeFalse(
        System.getProperty("poc.backend", "h2").equals("h2"),
        "H2 is only a wiring fixture, not a native snapshot conformance claim");
    try (var write = backend.begin()) {
      write.commit(List.of(put(prefix + "a", new byte[] {1}), put(prefix + "b", new byte[] {1})));
    }
    try (var read = backend.readView()) {
      assertThat(read.get(prefix + "a")).containsExactly((byte) 1);
      try (var write = backend.begin()) {
        write.commit(List.of(put(prefix + "a", new byte[] {2}), put(prefix + "b", new byte[] {2})));
      }
      assertThat(read.getMany(List.of(prefix + "b")).getFirst()).containsExactly((byte) 1);
    }
  }

  @Test
  void failureAfterFirstWriteChunkRollsBackWholeBatch() {
    try (var attempt = backend.begin()) {
      assertThatThrownBy(
              () ->
                  attempt.commit(
                      new AbstractList<DurablePrimitives.Mutation>() {
                        @Override
                        public DurablePrimitives.Mutation get(int index) {
                          if (index == 600)
                            throw new IllegalStateException("Injected before native commit");
                          return put(prefix + String.format("%04d", index), new byte[] {1});
                        }

                        @Override
                        public int size() {
                          return 700;
                        }
                      }))
          .isInstanceOf(RuntimeException.class);
    }
    try (var read = backend.begin()) {
      assertThat(read.scan(prefix, prefix + "~", 1000)).isEmpty();
      read.commit(List.of());
    }
  }

  @Test
  void batchReadPreservesMissingKeysOrderAndDuplicates() {
    try (var attempt = backend.begin()) {
      attempt.commit(List.of(put(prefix + "a", new byte[] {1}), put(prefix + "b", new byte[] {2})));
    }
    try (var read = backend.begin()) {
      var values =
          read.getMany(List.of(prefix + "b", prefix + "missing", prefix + "a", prefix + "b"));
      assertThat(values).hasSize(4);
      assertThat(values.get(0)).containsExactly((byte) 2);
      assertThat(values.get(1)).isNull();
      assertThat(values.get(2)).containsExactly((byte) 1);
      assertThat(values.get(3)).containsExactly((byte) 2);
      assertThat(read.getMany(List.of())).isEmpty();
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
        "H2 is not an isolation proof; Spanner emulator database-wide locks cannot run this"
            + " schedule");
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
      boolean deletionSucceeded = deletion.get(30, TimeUnit.SECONDS);
      boolean creationSucceeded = creation.get(30, TimeUnit.SECONDS);
      assertThat(deletionSucceeded && creationSucceeded).isFalse();
      try (var read = backend.begin()) {
        if (read.get(children + "child") != null) assertThat(read.get(parent)).isNotNull();
        read.commit(List.of());
      }
    } finally {
      workers.shutdownNow();
    }
  }
}
