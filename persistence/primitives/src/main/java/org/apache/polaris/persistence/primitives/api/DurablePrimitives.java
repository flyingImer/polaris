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
package org.apache.polaris.persistence.primitives.api;

import java.util.List;
import java.util.Objects;

/**
 * Experimental backend boundary. Keys and payloads have no Polaris domain meaning here. Each
 * attempt is serializable, including missing keys and ranges, together with its final writes. Close
 * aborts; only explicit commit can publish. No method retries caller code.
 */
public interface DurablePrimitives extends AutoCloseable {
  Attempt begin();

  /** One consistent snapshot for a composed read, with no mutations or later write attachment. */
  default ReadView readView() {
    return begin();
  }

  @Override
  default void close() {}

  record Entry(String key, byte[] value) {}

  /** A put, point delete (null value), or half-open range delete (non-null end). */
  record Mutation(String key, byte[] value, String end) {
    public static Mutation put(String key, byte[] value) {
      return new Mutation(key, Objects.requireNonNull(value), null);
    }

    public static Mutation delete(String key) {
      return new Mutation(key, null, null);
    }

    public static Mutation deleteRange(String begin, String end) {
      return new Mutation(begin, null, Objects.requireNonNull(end));
    }
  }

  interface ReadView extends AutoCloseable {
    byte[] get(String key);

    /**
     * Read all keys in the same view. Results preserve input order and duplicates, with null for
     * missing keys. When this view is an Attempt, every observation, including absence,
     * participates in its serializability. Adapters may chunk native requests without changing the
     * attempt or its read view.
     */
    List<byte[]> getMany(List<String> keys);

    /**
     * Ordered half-open range; limit must be positive. In an Attempt, empty results are protected.
     */
    List<Entry> scan(String begin, String end, int limit);

    @Override
    void close();
  }

  interface Attempt extends ReadView {
    /** Terminal: apply final mutations and commit the SAME attempt that performed the reads. */
    void commit(List<Mutation> mutations);
  }
}
