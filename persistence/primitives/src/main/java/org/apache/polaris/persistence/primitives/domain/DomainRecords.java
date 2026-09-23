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
package org.apache.polaris.persistence.primitives.domain;

import static org.apache.polaris.persistence.primitives.api.DurablePrimitives.Mutation.delete;
import static org.apache.polaris.persistence.primitives.api.DurablePrimitives.Mutation.deleteRange;
import static org.apache.polaris.persistence.primitives.api.DurablePrimitives.Mutation.put;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.persistence.primitives.api.DurablePrimitives;

/** Logical record mappings shared by every adapter; part of the Manager implementation. */
public final class DomainRecords {
  private final DurablePrimitives backend;
  private final String realmPrefix;

  // The old callback SPI has no attempt parameter. Scope this bridge to the session, not globally.
  @SuppressWarnings("ThreadLocalUsage")
  private final ThreadLocal<DurablePrimitives.LegacyAttempt> current = new ThreadLocal<>();

  private final ObjectMapper mapper =
      new ObjectMapper()
          .setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
          .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
  private final Slice<PolarisBaseEntity> sliceEntities;
  private final Slice<PolarisBaseEntity> sliceEntitiesActive;
  private final Slice<PolarisBaseEntity> sliceEntitiesChangeTracking;
  private final Slice<PolarisGrantRecord> sliceGrantRecords;
  private final Slice<PolarisGrantRecord> sliceGrantRecordsByGrantee;
  private final Slice<PolarisPrincipalSecrets> slicePrincipalSecrets;
  private final Slice<PolarisPolicyMappingRecord> slicePolicyMappingRecords;
  private final Slice<PolarisPolicyMappingRecord> slicePolicyMappingRecordsByPolicy;

  public DomainRecords(DurablePrimitives backend, String realm) {
    this.backend = backend;
    this.realmPrefix = encode(realm) + "/";
    // the entities slice
    this.sliceEntities =
        new Slice<>(
            "entity",
            PolarisBaseEntity.class,
            entity -> buildKeyComposite(entity.getCatalogId(), entity.getId()));

    // the entities active slice; simply acts as a name-based index into the entities slice
    this.sliceEntitiesActive =
        new Slice<>("name", PolarisBaseEntity.class, this::buildEntitiesActiveKey);

    // change tracking
    this.sliceEntitiesChangeTracking =
        new Slice<>(
            "version",
            PolarisBaseEntity.class,
            entity -> buildKeyComposite(entity.getCatalogId(), entity.getId()));

    // grant records by securable
    this.sliceGrantRecords =
        new Slice<>(
            "grant",
            PolarisGrantRecord.class,
            grantRecord ->
                buildKeyComposite(
                    grantRecord.getSecurableCatalogId(),
                    grantRecord.getSecurableId(),
                    grantRecord.getGranteeCatalogId(),
                    grantRecord.getGranteeId(),
                    grantRecord.getPrivilegeCode()));

    // grant records by securable
    this.sliceGrantRecordsByGrantee =
        new Slice<>(
            "grantee",
            PolarisGrantRecord.class,
            grantRecord ->
                buildKeyComposite(
                    grantRecord.getGranteeCatalogId(),
                    grantRecord.getGranteeId(),
                    grantRecord.getSecurableCatalogId(),
                    grantRecord.getSecurableId(),
                    grantRecord.getPrivilegeCode()));

    // principal secrets
    slicePrincipalSecrets =
        new Slice<>(
            "secret",
            PolarisPrincipalSecrets.class,
            principalSecrets -> principalSecrets.getPrincipalClientId());

    this.slicePolicyMappingRecords =
        new Slice<>(
            "policy",
            PolarisPolicyMappingRecord.class,
            policyMappingRecord ->
                buildKeyComposite(
                    policyMappingRecord.getTargetCatalogId(),
                    policyMappingRecord.getTargetId(),
                    policyMappingRecord.getPolicyTypeCode(),
                    policyMappingRecord.getPolicyCatalogId(),
                    policyMappingRecord.getPolicyId()));

    this.slicePolicyMappingRecordsByPolicy =
        new Slice<>(
            "policy-target",
            PolarisPolicyMappingRecord.class,
            policyMappingRecord ->
                buildKeyComposite(
                    policyMappingRecord.getPolicyTypeCode(),
                    policyMappingRecord.getPolicyCatalogId(),
                    policyMappingRecord.getPolicyId(),
                    policyMappingRecord.getTargetCatalogId(),
                    policyMappingRecord.getTargetId()));
  }

  private static String encode(String value) {
    return HexFormat.of().formatHex(value.getBytes(StandardCharsets.UTF_8));
  }

  public <T> T runFinalBatch(Supplier<T> work) {
    if (current.get() != null) throw new IllegalStateException("Nested transaction");
    try (var nativeAttempt = backend.begin()) {
      var batch = new FinalBatch(nativeAttempt);
      current.set(batch);
      T result = work.get();
      if (current.get() != null) batch.commit(List.of());
      return result;
    } catch (org.apache.polaris.persistence.primitives.api.StorageFailure e) {
      if (e.outcome()
          == org.apache.polaris.persistence.primitives.api.StorageFailure.Outcome.CONFLICT) {
        throw new org.apache.polaris.core.persistence.RetryOnConcurrencyException(e);
      }
      throw e;
    } finally {
      current.remove();
    }
  }

  private static final class FinalBatch implements DurablePrimitives.LegacyAttempt {
    private final DurablePrimitives.Attempt nativeAttempt;
    private final java.util.ArrayList<DurablePrimitives.Mutation> mutations =
        new java.util.ArrayList<>();

    FinalBatch(DurablePrimitives.Attempt nativeAttempt) {
      this.nativeAttempt = nativeAttempt;
    }

    private void checkReading() {
      if (!mutations.isEmpty())
        throw new IllegalStateException("Final-batch workflow read after write");
    }

    @Override
    public byte[] get(String key) {
      checkReading();
      return nativeAttempt.get(key);
    }

    @Override
    public List<DurablePrimitives.Entry> scan(String begin, String end, int limit) {
      checkReading();
      return nativeAttempt.scan(begin, end, limit);
    }

    @Override
    public void applyForLegacyReadYourWrites(List<DurablePrimitives.Mutation> changes) {
      mutations.addAll(changes);
    }

    @Override
    public void commit(List<DurablePrimitives.Mutation> changes) {
      mutations.addAll(changes);
      nativeAttempt.commit(mutations);
    }

    @Override
    public void close() {
      nativeAttempt.close();
    }
  }

  public <T> T runInTransaction(PolarisDiagnostics diagnostics, Supplier<T> work) {
    if (current.get() != null) throw new IllegalStateException("Nested transaction");
    try (var attempt = backend.beginLegacy()) {
      current.set(attempt);
      T result = work.get();
      if (current.get() != null) attempt.commit(List.of());
      return result;
    } catch (org.apache.polaris.persistence.primitives.api.StorageFailure e) {
      if (e.outcome()
          == org.apache.polaris.persistence.primitives.api.StorageFailure.Outcome.CONFLICT) {
        throw new org.apache.polaris.core.persistence.RetryOnConcurrencyException(e);
      }
      throw e;
    } finally {
      current.remove();
    }
  }

  public <T> T runInReadTransaction(PolarisDiagnostics d, Supplier<T> work) {
    return runInTransaction(d, work);
  }

  public void runActionInTransaction(PolarisDiagnostics d, Runnable work) {
    runInTransaction(
        d,
        () -> {
          work.run();
          return null;
        });
  }

  public void runActionInReadTransaction(PolarisDiagnostics d, Runnable work) {
    runActionInTransaction(d, work);
  }

  private DurablePrimitives.LegacyAttempt attempt() {
    var result = current.get();
    if (result == null) throw new IllegalStateException("No transaction");
    return result;
  }

  long getNextSequence() {
    String key = realmPrefix + "sequence";
    byte[] value = attempt().get(key);
    long next =
        value == null ? 1 : Long.parseLong(new String(value, StandardCharsets.US_ASCII)) + 1;
    attempt()
        .applyForLegacyReadYourWrites(
            List.of(put(key, Long.toString(next).getBytes(StandardCharsets.US_ASCII))));
    return next;
  }

  void rollback() {
    attempt().close();
    current.remove();
  }

  void deleteAll() {
    attempt().applyForLegacyReadYourWrites(List.of(deleteRange(realmPrefix, realmPrefix + "~")));
  }

  final class Slice<T> {
    private final String prefix;
    private final Class<T> type;
    private final Function<T, String> key;

    Slice(String name, Class<T> type, Function<T, String> key) {
      this.prefix = realmPrefix + name + "/";
      this.type = type;
      this.key = key;
    }

    T read(String key) {
      byte[] value = attempt().get(prefix + encode(key));
      return value == null ? null : decode(value);
    }

    T decode(byte[] value) {
      try {
        return mapper.readValue(value, type);
      } catch (java.io.IOException e) {
        throw new IllegalStateException("Invalid durable record", e);
      }
    }

    List<T> readRange(String keyPrefix) {
      return attempt()
          .scan(prefix + encode(keyPrefix), prefix + encode(keyPrefix) + "~", Integer.MAX_VALUE)
          .stream()
          .map(e -> decode(e.value()))
          .toList();
    }

    boolean hasAny(String keyPrefix) {
      return !attempt()
          .scan(prefix + encode(keyPrefix), prefix + encode(keyPrefix) + "~", 1)
          .isEmpty();
    }

    void write(T value) {
      try {
        Object persisted = value;
        if (value instanceof PolarisPrincipalSecrets s) {
          // Persist verification hashes, never returned plaintext credentials.
          persisted =
              new PolarisPrincipalSecrets(
                  s.getPrincipalId(),
                  s.getPrincipalClientId(),
                  null,
                  null,
                  s.getSecretSalt(),
                  s.getMainSecretHash(),
                  s.getSecondarySecretHash());
        }
        attempt()
            .applyForLegacyReadYourWrites(
                List.of(
                    put(prefix + encode(key.apply(value)), mapper.writeValueAsBytes(persisted))));
      } catch (java.io.IOException e) {
        throw new IllegalStateException("Cannot encode durable record", e);
      }
    }

    void delete(String key) {
      attempt()
          .applyForLegacyReadYourWrites(
              List.of(DurablePrimitives.Mutation.delete(prefix + encode(key))));
    }

    void delete(T value) {
      delete(key.apply(value));
    }

    void deleteRange(String keyPrefix) {
      attempt()
          .applyForLegacyReadYourWrites(
              List.of(
                  DurablePrimitives.Mutation.deleteRange(
                      prefix + encode(keyPrefix), prefix + encode(keyPrefix) + "~")));
    }
  }

  Slice<PolarisBaseEntity> getSliceEntities() {
    return sliceEntities;
  }

  Slice<PolarisBaseEntity> getSliceEntitiesActive() {
    return sliceEntitiesActive;
  }

  Slice<PolarisBaseEntity> getSliceEntitiesChangeTracking() {
    return sliceEntitiesChangeTracking;
  }

  Slice<PolarisGrantRecord> getSliceGrantRecords() {
    return sliceGrantRecords;
  }

  Slice<PolarisGrantRecord> getSliceGrantRecordsByGrantee() {
    return sliceGrantRecordsByGrantee;
  }

  Slice<PolarisPrincipalSecrets> getSlicePrincipalSecrets() {
    return slicePrincipalSecrets;
  }

  Slice<PolarisPolicyMappingRecord> getSlicePolicyMappingRecords() {
    return slicePolicyMappingRecords;
  }

  Slice<PolarisPolicyMappingRecord> getSlicePolicyMappingRecordsByPolicy() {
    return slicePolicyMappingRecordsByPolicy;
  }

  String buildEntitiesActiveKey(PolarisEntityCore coreEntity) {
    return buildKeyComposite(
        coreEntity.getCatalogId(),
        coreEntity.getParentId(),
        coreEntity.getTypeCode(),
        coreEntity.getName());
  }

  /**
   * Key for the entities slice
   *
   * @param coreEntity core entity
   * @return the key
   */
  String buildEntitiesKey(PolarisEntityCore coreEntity) {
    return buildKeyComposite(coreEntity.getCatalogId(), coreEntity.getId());
  }

  /**
   * Build key from a set of value pairs
   *
   * @param keys string/long/integer values
   * @return unique string identifier
   */
  String buildKeyComposite(Object... keys) {
    StringBuilder result = new StringBuilder();
    for (Object key : keys) {
      if (result.length() != 0) {
        result.append("::");
      }
      result.append(encode(key.toString()));
    }
    return result.toString();
  }

  /**
   * Build prefix key from a set of value pairs; prefix key will end with the key separator
   *
   * @param keys string/long/integer values
   * @return unique string identifier
   */
  String buildPrefixKeyComposite(Object... keys) {
    StringBuilder result = new StringBuilder();
    for (Object key : keys) {
      result.append(encode(key.toString()));
      result.append("::");
    }
    return result.toString();
  }
}
