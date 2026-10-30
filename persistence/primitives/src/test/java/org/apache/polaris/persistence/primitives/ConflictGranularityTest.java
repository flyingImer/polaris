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

import static org.apache.polaris.persistence.primitives.api.DurablePrimitives.Mutation.put;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.polaris.persistence.primitives.api.DurablePrimitives;
import org.apache.polaris.persistence.primitives.api.StorageFailure;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Isolates predicate contention from the allocator. Measurements are not performance guarantees.
 */
@EnabledIfSystemProperty(named = "poc.measure", matches = "true")
class ConflictGranularityTest {
  @ParameterizedTest(name = "{0}-siblings{1}-writers{2}")
  @CsvSource({
    "range,0,1",
    "range,32,1",
    "range,0,4",
    "range,32,4",
    "sequence,0,1",
    "sequence,32,1",
    "sequence,0,4",
    "sequence,32,4"
  })
  void measureIndependentCreates(String workload, int siblings, int writers) throws Exception {
    String backendName = System.getProperty("poc.backend", "h2");
    String prefix = "contention/" + UUID.randomUUID() + "/";
    int perWriter = 10;
    var attempts = new AtomicInteger();
    var conflicts = new AtomicInteger();
    var latencies = Collections.synchronizedList(new ArrayList<Long>());
    var firstReads = new CountDownLatch(writers);
    try (var backend = NativeBackends.open(backendName)) {
      try (var seed = backend.begin()) {
        var mutations = new ArrayList<DurablePrimitives.Mutation>();
        mutations.add(put(prefix + "sequence", ByteBuffer.allocate(4).putInt(0).array()));
        for (int i = 0; i < siblings; i++)
          mutations.add(put(prefix + "siblings/seed-" + i, new byte[] {1}));
        seed.commit(mutations);
      }
      try (var workers = Executors.newFixedThreadPool(writers)) {
        var futures = new ArrayList<Future<?>>();
        for (int writer = 0; writer < writers; writer++) {
          int owner = writer;
          futures.add(
              workers.submit(
                  () -> {
                    for (int operation = 0; operation < perWriter; operation++) {
                      long start = System.nanoTime();
                      boolean committed = false;
                      for (int retry = 0; retry < 100 && !committed; retry++) {
                        attempts.incrementAndGet();
                        try (var attempt = backend.begin()) {
                          String key = prefix + "siblings/" + owner + "-" + operation;
                          List<DurablePrimitives.Mutation> mutations;
                          if (workload.equals("range")) {
                            attempt.scan(
                                prefix + "siblings/", prefix + "siblings/~", Integer.MAX_VALUE);
                            mutations = List.of(put(key, new byte[] {2}));
                          } else {
                            int sequence =
                                ByteBuffer.wrap(attempt.get(prefix + "sequence")).getInt();
                            mutations =
                                List.of(
                                    put(
                                        prefix + "sequence",
                                        ByteBuffer.allocate(4).putInt(sequence + 1).array()));
                          }
                          if (operation == 0 && retry == 0) {
                            firstReads.countDown();
                            if (!firstReads.await(30, TimeUnit.SECONDS))
                              throw new AssertionError("Missing writer");
                          }
                          attempt.commit(mutations);
                          committed = true;
                        } catch (StorageFailure e) {
                          assertThat(e.outcome()).isEqualTo(StorageFailure.Outcome.CONFLICT);
                          conflicts.incrementAndGet();
                        }
                      }
                      assertThat(committed).as("bounded completion").isTrue();
                      latencies.add(System.nanoTime() - start);
                    }
                    return null;
                  }));
        }
        for (var future : futures) future.get(120, TimeUnit.SECONDS);
      }
      try (var read = backend.readView()) {
        if (workload.equals("range")) {
          assertThat(read.scan(prefix + "siblings/", prefix + "siblings/~", Integer.MAX_VALUE))
              .hasSize(siblings + writers * perWriter);
        } else {
          assertThat(ByteBuffer.wrap(read.get(prefix + "sequence")).getInt())
              .isEqualTo(writers * perWriter);
        }
      }
    }
    Collections.sort(latencies);
    long p95 = latencies.get((int) Math.ceil(latencies.size() * .95) - 1);
    Path reports = Path.of("build/reports/primitives-contention");
    Files.createDirectories(reports);
    String report =
        String.format(
            Locale.ROOT,
            "{\"backend\":\"%s\",\"workload\":\"%s\",\"siblings\":%d,\"writers\":%d,\"operations\":%d,\"attempts\":%d,\"conflicts\":%d,\"retries\":%d,\"abortFraction\":%.6f,\"p95Millis\":%.3f}%n",
            backendName,
            workload,
            siblings,
            writers,
            latencies.size(),
            attempts.get(),
            conflicts.get(),
            conflicts.get(),
            (double) conflicts.get() / attempts.get(),
            p95 / 1_000_000.0);
    Files.writeString(reports.resolve(workload + "-" + siblings + "-" + writers + ".json"), report);
  }
}
