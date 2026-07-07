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
package org.apache.polaris.forkless.oss.durable;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.polaris.durable.model.EntityRecord;
import org.apache.polaris.spi.durable.DurablePrimitives;

/**
 * OSS-default {@link DurablePrimitives}: a plain in-memory record store. It assigns an id on create
 * when the incoming record has none, and its compare-and-swap is an atomic version check.
 */
public class InMemoryDurablePrimitives implements DurablePrimitives {

  private final ConcurrentHashMap<Long, EntityRecord> store = new ConcurrentHashMap<>();
  private final AtomicLong nextId = new AtomicLong();

  @Override
  public Optional<EntityRecord> read(long id) {
    return Optional.ofNullable(store.get(id));
  }

  @Override
  public EntityRecord create(EntityRecord record) {
    long id = record.id() > 0 ? record.id() : nextId.incrementAndGet();
    EntityRecord stored =
        new EntityRecord(id, record.version(), record.type(), record.name(), record.payload());
    store.put(id, stored);
    return stored;
  }

  @Override
  public boolean compareAndSwap(long id, int expectedVersion, EntityRecord next) {
    EntityRecord updated =
        store.computeIfPresent(
            id, (key, current) -> current.version() == expectedVersion ? next : current);
    return updated == next;
  }

  /**
   * Compensation hook used by {@link DefaultDurableManager} to undo a partial catalog creation. Not
   * part of the {@link DurablePrimitives} SPI: rollback is the high layer's concern, so it stays
   * off the raw contract and package-private to its default store.
   */
  void remove(long id) {
    store.remove(id);
  }

  int size() {
    return store.size();
  }
}
