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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.CommitOutcomeUnknownException;
import org.apache.polaris.core.persistence.CommitRejectedException;
import org.apache.polaris.core.persistence.ConfirmedConflictRetry;
import org.apache.polaris.core.persistence.ConfirmedTransactionConflictException;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.persistence.primitives.api.DurablePrimitives;
import org.apache.polaris.persistence.primitives.api.StorageFailure;

/** Logical record mappings shared by every adapter; part of the Manager implementation. */
public final class DomainRecords {
  private final DurablePrimitives backend;
  private final String realmPrefix;

  // The old callback SPI has no attempt parameter. Keep an owner-checked thread scope only
  // for this migration bridge; the public Primitives API passes attempts explicitly.
  private static final ThreadLocal<Scope> CURRENT = new ThreadLocal<>();

  private record Scope(DomainRecords owner, FinalBatch attempt) {}

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
    return runScoped(work, false);
  }

  private <T> T runScoped(Supplier<T> work, boolean readOnly) {
    if (CURRENT.get() != null) throw new IllegalStateException("Nested transaction");
    try (var nativeAttempt = readOnly ? backend.readView() : backend.begin()) {
      var batch = new FinalBatch(nativeAttempt, readOnly);
      CURRENT.set(new Scope(this, batch));
      T result = work.get();
      if (CURRENT.get() != null && !readOnly) batch.commit(List.of());
      return result;
    } catch (StorageFailure e) {
      if (e.outcome() == StorageFailure.Outcome.CONFLICT) {
        throw new ConfirmedTransactionConflictException(e);
      }
      if (e.outcome() == StorageFailure.Outcome.UNKNOWN) throw new CommitOutcomeUnknownException(e);
      throw new CommitRejectedException(e);
    } finally {
      CURRENT.remove();
    }
  }

  private final class FinalBatch implements DurablePrimitives.Attempt {
    private final DurablePrimitives.ReadView nativeAttempt;
    private final boolean readOnly;
    private final ArrayList<DurablePrimitives.Mutation> mutations = new ArrayList<>();
    private Long nextSequence;

    FinalBatch(DurablePrimitives.ReadView nativeAttempt, boolean readOnly) {
      this.nativeAttempt = nativeAttempt;
      this.readOnly = readOnly;
    }

    private void checkWritable() {
      if (readOnly) throw new IllegalStateException("Read-only view cannot stage mutations");
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
    public List<byte[]> getMany(List<String> keys) {
      checkReading();
      return nativeAttempt.getMany(keys);
    }

    @Override
    public List<DurablePrimitives.Entry> scan(String begin, String end, int limit) {
      checkReading();
      return nativeAttempt.scan(begin, end, limit);
    }

    void stage(List<DurablePrimitives.Mutation> changes) {
      checkWritable();
      mutations.addAll(changes);
    }

    @Override
    public void commit(List<DurablePrimitives.Mutation> changes) {
      checkWritable();
      mutations.addAll(changes);
      if (nextSequence != null)
        mutations.add(
            put(
                realmPrefix + "sequence",
                Long.toString(nextSequence).getBytes(StandardCharsets.US_ASCII)));
      ((DurablePrimitives.Attempt) nativeAttempt).commit(mutations);
    }

    @Override
    public void close() {
      nativeAttempt.close();
    }
  }

  public <T> T runInTransaction(PolarisDiagnostics diagnostics, Supplier<T> work) {
    return runFinalBatch(work);
  }

  public <T> T runInReadTransaction(PolarisDiagnostics d, Supplier<T> work) {
    return ConfirmedConflictRetry.run(() -> runScoped(work, true));
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
    runInReadTransaction(
        d,
        () -> {
          work.run();
          return null;
        });
  }

  private FinalBatch attempt() {
    Scope scope = CURRENT.get();
    if (scope == null || scope.owner() != this)
      throw new IllegalStateException("No transaction for this session");
    return scope.attempt();
  }

  long getNextSequence() {
    FinalBatch plan = attempt();
    plan.checkWritable();
    plan.checkReading();
    if (plan.nextSequence == null) {
      byte[] value = plan.get(realmPrefix + "sequence");
      plan.nextSequence =
          value == null ? 0L : Long.parseLong(new String(value, StandardCharsets.US_ASCII));
    }
    // ID reservation is domain state computed during planning. Its final counter mutation is
    // appended at commit, so multiple allocations need no storage read-your-writes overlay.
    plan.nextSequence++;
    return plan.nextSequence;
  }

  void rollback() {
    attempt().close();
    CURRENT.remove();
  }

  void deleteAll() {
    attempt().stage(List.of(deleteRange(realmPrefix, realmPrefix + "~")));
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

    List<T> readMany(List<String> keys) {
      return attempt().getMany(keys.stream().map(key -> prefix + encode(key)).toList()).stream()
          .map(value -> value == null ? null : decode(value))
          .toList();
    }

    T decode(byte[] value) {
      try {
        return mapper.readValue(value, type);
      } catch (IOException e) {
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
            .stage(
                List.of(
                    put(prefix + encode(key.apply(value)), mapper.writeValueAsBytes(persisted))));
      } catch (IOException e) {
        throw new IllegalStateException("Cannot encode durable record", e);
      }
    }

    void delete(String key) {
      attempt().stage(List.of(DurablePrimitives.Mutation.delete(prefix + encode(key))));
    }

    void delete(T value) {
      delete(key.apply(value));
    }

    void deleteRange(String keyPrefix) {
      attempt()
          .stage(
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
