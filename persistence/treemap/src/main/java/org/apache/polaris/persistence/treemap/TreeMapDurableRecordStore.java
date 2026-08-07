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
package org.apache.polaris.persistence.treemap;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.ListScope;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.Precondition;
import org.apache.polaris.spi.durable.RecordKind;
import org.apache.polaris.spi.durable.RecordRef;
import org.apache.polaris.spi.durable.RecordVersions;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The in-memory implementation of {@link DurableRecordStore}, over {@link TreeMapSlices}.
 *
 * <p>It exists to answer two questions no second relational database could answer. First, whether
 * the SPI is implementable by a store that is not a relational database at all: there is no SQL
 * here, no schema, and no index, only maps and a key format this class chooses. Second, whether
 * cross-domain compensation works, which needs two stores reporting different {@link #domainOf}
 * values.
 *
 * <p>Atomicity is {@link TreeMapSlices#runInTransaction}, whose undo log restores every slice when
 * the body throws. That is the in-memory counterpart of the relational {@code
 * runWithinTransaction}, and it is why a failed precondition here throws internally rather than
 * returning a flag: throwing is what triggers the rollback.
 *
 * <p><b>Declares its own slices rather than reusing the eight this store already holds.</b> Those
 * eight belong to {@link TreeMapDurablePrimitivesImpl}, the old shape's implementation, and sharing
 * rows between the two would make a conformance run depend on which implementation wrote first. Two
 * stores, two sets of rows.
 *
 * <p><b>Scopes are served by scanning this store's own maps.</b> There is no index to seek, so
 * {@link #list} walks the kind's slice and keeps what matches. That is not caller-side filtering,
 * which the contract forbids because it would put rows on a wire: the store evaluates the scope
 * itself, and no wire exists. The shipped {@code hasOverlappingSiblings} on the old shape already
 * works exactly this way, with its own {@code // TODO we could optimize this full scan}. This class
 * is a conformance target and a second atomicity domain, not a performance target.
 */
public class TreeMapDurableRecordStore implements DurableRecordStore {

  /**
   * A policy ceiling, matching the relational store's default so a conformance test that exercises
   * the limit does not have to special-case which store it is talking to.
   */
  public static final int DEFAULT_MAX_ITEMS_PER_COMMIT = 1000;

  private final TreeMapSlices slices;
  private final PolarisDiagnostics diagnostics;
  private final int maxItemsPerCommit;
  private final Map<RecordKind, KindBinding<?>> bindings;

  /** Opaque by contract: callers may compare it and must not interpret it. */
  private final Object domain = new Object();

  public TreeMapDurableRecordStore(
      @NonNull TreeMapSlices slices, @NonNull PolarisDiagnostics diagnostics) {
    this(slices, diagnostics, DEFAULT_MAX_ITEMS_PER_COMMIT);
  }

  public TreeMapDurableRecordStore(
      @NonNull TreeMapSlices slices,
      @NonNull PolarisDiagnostics diagnostics,
      int maxItemsPerCommit) {
    this.slices = slices;
    this.diagnostics = diagnostics;
    this.maxItemsPerCommit = maxItemsPerCommit;
    this.bindings = buildBindings();
  }

  /**
   * Signals a failed precondition. Thrown so {@code runInTransaction} rolls the whole commit back.
   */
  private static final class PreconditionFailed extends RuntimeException {
    private final Precondition precondition;

    PreconditionFailed(Precondition precondition) {
      super("precondition failed", null, false, false);
      this.precondition = precondition;
    }
  }

  // ------------------------------------------------------------------ registry

  /**
   * What a kind needs: where its rows live, how to key one, and how to answer each question a scope
   * or a precondition can ask about it.
   *
   * @param identityKey the slice key for a record, and the same format {@link RecordRef} identity
   *     keys are rendered into
   * @param uniquenessKey null when the kind's identity and uniqueness are the same tuple
   * @param versions null when the kind carries no version
   */
  private record KindBinding<T>(
      TreeMapSlices.Slice<T> slice,
      Function<T, String> identityKey,
      @Nullable Function<T, String> uniquenessKey,
      @Nullable Function<T, Long> parentId,
      @Nullable Function<T, String> location,
      Function<T, List<String>> referencedKeys,
      @Nullable Function<T, RecordVersions> versions) {}

  private Map<RecordKind, KindBinding<?>> buildBindings() {
    Map<RecordKind, KindBinding<?>> map = new LinkedHashMap<>();

    map.put(
        PolarisRecordKinds.ENTITY,
        new KindBinding<PolarisBaseEntity>(
            slices.newSlice(e -> key(e.getId()), e -> new PolarisBaseEntity.Builder(e).build()),
            e -> key(e.getId()),
            e -> key(e.getParentId(), e.getTypeCode(), e.getName()),
            PolarisBaseEntity::getParentId,
            TreeMapDurableRecordStore::locationWithoutScheme,
            e -> List.of(key(e.getParentId())),
            e -> new RecordVersions(e.getEntityVersion(), e.getGrantRecordsVersion())));

    map.put(
        PolarisRecordKinds.GRANT_RECORD,
        new KindBinding<PolarisGrantRecord>(
            slices.newSlice(TreeMapDurableRecordStore::grantKey, PolarisGrantRecord::new),
            TreeMapDurableRecordStore::grantKey,
            null,
            null,
            null,
            // a grant is reachable from either end, which is why the shipped interface's two
            // list methods collapse into one REFERENCING scope with a different anchor
            g ->
                List.of(
                    key(g.getSecurableCatalogId(), g.getSecurableId()),
                    key(g.getGranteeCatalogId(), g.getGranteeId())),
            null));

    map.put(
        PolarisRecordKinds.POLICY_MAPPING,
        new KindBinding<PolarisPolicyMappingRecord>(
            slices.newSlice(TreeMapDurableRecordStore::policyKey, PolarisPolicyMappingRecord::new),
            TreeMapDurableRecordStore::policyKey,
            null,
            null,
            null,
            p ->
                List.of(
                    key(p.getTargetCatalogId(), p.getTargetId()),
                    key(p.getPolicyCatalogId(), p.getPolicyId())),
            null));

    map.put(
        PolarisRecordKinds.PRINCIPAL_SECRETS,
        new KindBinding<PolarisPrincipalSecrets>(
            slices.newSlice(s -> key(s.getPrincipalClientId()), PolarisPrincipalSecrets::new),
            s -> key(s.getPrincipalClientId()),
            null,
            null,
            null,
            s -> List.of(),
            null));

    map.put(
        PolarisRecordKinds.EVENT,
        new KindBinding<EventEntity>(
            // events had no slice at all before TreeMapSlices grew a newSlice factory
            slices.newSlice(e -> key(e.getId()), Function.identity()),
            e -> key(e.getId()),
            null,
            null,
            null,
            e -> List.of(),
            null));

    return Map.copyOf(map);
  }

  private static String key(Object... parts) {
    return java.util.Arrays.stream(parts).map(String::valueOf).collect(Collectors.joining("::"));
  }

  private static String grantKey(PolarisGrantRecord g) {
    return key(
        g.getSecurableCatalogId(),
        g.getSecurableId(),
        g.getGranteeCatalogId(),
        g.getGranteeId(),
        g.getPrivilegeCode());
  }

  private static String policyKey(PolarisPolicyMappingRecord p) {
    return key(
        p.getTargetCatalogId(),
        p.getTargetId(),
        p.getPolicyTypeCode(),
        p.getPolicyCatalogId(),
        p.getPolicyId());
  }

  /**
   * The entity's storage location with the scheme stripped, or null when it has none. Read from the
   * base-location property, the same source the relational store's row mapper reads.
   */
  private static @Nullable String locationWithoutScheme(PolarisBaseEntity entity) {
    String base = entity.getPropertiesAsMap().get(PolarisEntityConstants.ENTITY_BASE_LOCATION);
    return base == null ? null : StorageLocation.of(base).withoutScheme();
  }

  @SuppressWarnings("unchecked")
  private <T> KindBinding<T> binding(RecordKind kind) {
    KindBinding<?> b = bindings.get(kind);
    if (b == null) {
      throw new IllegalArgumentException(
          "No mapper registered for record kind '" + kind.id() + "' in this store");
    }
    return (KindBinding<T>) b;
  }

  // ------------------------------------------------------------------ writes

  @Override
  public @NonNull CommitResult commit(@NonNull List<Mutation> mutations) {
    if (mutations.size() > maxItemsPerCommit) {
      return CommitResult.tooManyItems();
    }
    for (Mutation m : mutations) {
      if (!domain.equals(domainOf(m.target()))) {
        return CommitResult.domainMismatch();
      }
      for (Precondition p : m.preconditions()) {
        if (p.ref().isPresent() && !domain.equals(domainOf(p.ref().get()))) {
          return CommitResult.domainMismatch();
        }
      }
    }
    try {
      slices.runActionInTransaction(
          diagnostics,
          () -> {
            for (Mutation m : mutations) {
              applyMutation(m);
            }
          });
    } catch (PreconditionFailed e) {
      // The undo log has already restored every slice, so nothing from this commit survives.
      return CommitResult.preconditionFailed(List.of(e.precondition));
    }
    return CommitResult.applied();
  }

  private <T> void applyMutation(Mutation m) {
    KindBinding<T> b = binding(m.kind());
    for (Precondition p : m.preconditions()) {
      if (!holds(p)) {
        throw new PreconditionFailed(p);
      }
    }
    @SuppressWarnings("unchecked")
    T record = (T) m.record();
    switch (m.op()) {
      case CREATE -> {
        if (b.slice().read(b.identityKey().apply(record)) != null) {
          throw new PreconditionFailed(Precondition.notExists(m.target()));
        }
        b.slice().write(record);
      }
      case UPDATE -> b.slice().write(record);
      case DELETE -> b.slice().delete(b.identityKey().apply(record));
    }
  }

  private <T> boolean holds(Precondition p) {
    if (p.op() == Precondition.Op.NONE || p.ref().isEmpty()) {
      return true;
    }
    RecordRef ref = p.ref().get();
    Optional<T> found = lookupInTransaction(ref);
    return switch (p.op()) {
      case NOT_EXISTS -> found.isEmpty();
      case EXISTS -> found.isPresent();
      case VERSION_EQUALS -> {
        KindBinding<T> b = binding(ref.kind());
        Function<T, RecordVersions> versions = b.versions();
        if (versions == null) {
          throw new IllegalArgumentException(
              "Record kind '"
                  + ref.kind().id()
                  + "' carries no version, so a VERSION_EQUALS precondition cannot be satisfied"
                  + " against it");
        }
        yield found
            .map(versions)
            .map(
                v ->
                    switch (p.attribute().orElseThrow()) {
                      case RECORD_VERSION -> v.recordVersion() == p.expectedVersion();
                      case GRANT_RECORDS_VERSION -> v.grantRecordsVersion() == p.expectedVersion();
                    })
            .orElse(false);
      }
      case NONE -> true;
    };
  }

  @Override
  public long generateNewId() {
    // getNextSequence asserts a write transaction is open, so the counter bump rides in one.
    return slices.runInTransaction(diagnostics, slices::getNextSequence);
  }

  // ------------------------------------------------------------------ reads

  /**
   * Resolves a reference in whichever of the two addressing modes it uses.
   *
   * <p><b>Assumes a transaction is already open.</b> {@link TreeMapSlices.Slice#read} asserts one,
   * and the store may not nest, so exactly one caller opens it: a public read wraps this in a read
   * transaction, while a precondition check during {@link #commit} is already inside the write
   * transaction whose rollback it may trigger. The SPI has no transaction concept of its own, per
   * Issue 47's S8, so opening one is this store's private business.
   */
  private <T> Optional<T> lookupInTransaction(RecordRef ref) {
    KindBinding<T> b = binding(ref.kind());
    String wanted = key(ref.key().toArray());
    if (ref.mode() == RecordRef.Mode.IDENTITY) {
      return Optional.ofNullable(b.slice().read(wanted));
    }
    Function<T, String> uniqueness =
        b.uniquenessKey() != null ? b.uniquenessKey() : b.identityKey();
    return b.slice().readRange("").stream()
        .filter(r -> wanted.equals(uniqueness.apply(r)))
        .findFirst();
  }

  @Override
  public @NonNull <T> Optional<T> get(@NonNull RecordRef ref, @NonNull Class<T> type) {
    return slices.runInReadTransaction(
        diagnostics, () -> this.<T>lookupInTransaction(ref).map(type::cast));
  }

  @Override
  public @NonNull <T> List<Optional<T>> getMany(
      @NonNull List<RecordRef> refs, @NonNull Class<T> type) {
    // One transaction for the whole batch, so the positional results are one snapshot rather than
    // N independent reads.
    return slices.runInReadTransaction(
        diagnostics,
        () -> {
          List<Optional<T>> out = new ArrayList<>(refs.size());
          for (RecordRef ref : refs) {
            out.add(this.<T>lookupInTransaction(ref).map(type::cast));
          }
          return out;
        });
  }

  @Override
  public @NonNull <T> Page<T> list(
      @NonNull ListScope scope, @NonNull PageToken pageToken, @NonNull Class<T> type) {
    RecordKind kind =
        scope
            .kind()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "A kind-less scope spans every registered kind and is not implemented by"
                            + " this store yet"));
    KindBinding<T> b = binding(kind);
    String anchor = key(scope.anchor().key().toArray());
    List<T> matched =
        slices.runInReadTransaction(
            diagnostics,
            () ->
                b.slice().readRange("").stream()
                    .filter(r -> matches(b, scope, anchor, r))
                    .toList());
    // One page: keyset pagination needs an ordered key column, and this store's slice keys are
    // strings chosen per kind rather than a single ordering column.
    return Page.page(pageToken, matched.stream().map(type::cast).toList(), null);
  }

  private <T> boolean matches(KindBinding<T> b, ListScope scope, String anchor, T record) {
    return switch (scope.shape()) {
      case CHILDREN_OF_PARENT -> {
        Function<T, Long> parent = b.parentId();
        if (parent == null) {
          yield false;
        }
        if (!anchor.equals(key(parent.apply(record)))) {
          yield false;
        }
        yield scope.subtype().isEmpty() || subtypeOf(record) == scope.subtype().get();
      }
      case REFERENCING -> b.referencedKeys().apply(record).contains(anchor);
      case UNDER_LOCATION_PREFIX -> {
        Function<T, String> location = b.location();
        if (location == null) {
          yield false;
        }
        String value = location.apply(record);
        yield value != null && value.startsWith(scope.locationPrefix().orElseThrow());
      }
    };
  }

  private static int subtypeOf(Object record) {
    if (record instanceof PolarisBaseEntity entity) {
      return entity.getSubTypeCode();
    }
    throw new IllegalArgumentException("Only entities carry a subtype");
  }

  @Override
  public @NonNull List<Optional<RecordVersions>> versionsOf(@NonNull List<RecordRef> refs) {
    for (RecordRef ref : refs) {
      if (binding(ref.kind()).versions() == null) {
        throw new IllegalArgumentException(
            "Record kind '" + ref.kind().id() + "' carries no version, so versionsOf is undefined");
      }
    }
    return slices.runInReadTransaction(
        diagnostics,
        () -> {
          List<Optional<RecordVersions>> out = new ArrayList<>(refs.size());
          for (RecordRef ref : refs) {
            KindBinding<Object> b = binding(ref.kind());
            out.add(this.lookupInTransaction(ref).map(b.versions()));
          }
          return out;
        });
  }

  // ------------------------------------------------------------------ declarations

  @Override
  public @NonNull Object domainOf(@NonNull RecordRef target) {
    // One TreeMapSlices instance is one atomicity domain: its undo log covers every slice it holds
    // and nothing beyond. Two instances are two domains, which is what the cross-domain test needs.
    binding(target.kind());
    return domain;
  }

  @Override
  public int maxItemsPerCommit() {
    return maxItemsPerCommit;
  }
}
