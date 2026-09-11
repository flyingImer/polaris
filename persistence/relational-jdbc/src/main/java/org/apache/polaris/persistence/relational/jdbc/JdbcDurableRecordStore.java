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
package org.apache.polaris.persistence.relational.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.pagination.EntityIdToken;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.persistence.relational.jdbc.models.Converter;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEntity;
import org.apache.polaris.persistence.relational.jdbc.models.ModelEvent;
import org.apache.polaris.persistence.relational.jdbc.models.ModelGrantRecord;
import org.apache.polaris.persistence.relational.jdbc.models.ModelPolicyMappingRecord;
import org.apache.polaris.persistence.relational.jdbc.models.ModelPrincipalAuthenticationData;
import org.apache.polaris.spi.durable.CommitDisruptedException;
import org.apache.polaris.spi.durable.CommitDisruptedException.DurableEffect;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.LookupPath;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.Precondition;
import org.apache.polaris.spi.durable.Read;
import org.apache.polaris.spi.durable.ReadToken;
import org.apache.polaris.spi.durable.RecordKind;
import org.apache.polaris.spi.durable.RecordRef;
import org.apache.polaris.spi.durable.RecordVersions;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The relational-JDBC implementation of {@link DurableRecordStore}.
 *
 * <p>One instance per database, per realm. Per Issue 47's R1 an implementation instance is scoped
 * to a backend deployment rather than to an atomicity domain; for a single relational database
 * those coincide, which is why {@link #domainOf} returns a constant and why two instances over two
 * databases give two domains.
 *
 * <p><b>Nothing here is a new mechanism.</b> Atomicity is {@link
 * DatasourceOperations#runWithinTransaction}, which already rolls back both when its callback
 * returns false and when it throws. SQL comes from {@link QueryGenerator}, which is parameterised
 * by table name and column list. Row conversion comes from the existing {@code Model*} {@link
 * Converter} classes. This class contributes exactly one thing the shipped implementation lacks: a
 * registry saying which table, columns, converter and key columns each {@link RecordKind} maps to.
 *
 * <p>It does not modify {@link JdbcDurablePrimitivesImpl}. Both exist side by side until callers
 * migrate; see {@link DurableRecordStore} for the migration's end state.
 *
 * <h2>Two things found while writing this</h2>
 *
 * <p><b>1. Whether a lookup path's anchor may carry a denormalised scoping column is the data
 * model's question, and the declaration answers it.</b> The shipped {@code listEntities} filters on
 * {@code catalog_id}, {@code parent_id} and {@code type_code}, and this class filters on exactly
 * the same columns, so the query it issues is the shipped query. The by-parent declaration states
 * the anchors as the parent address, {@code (parent-catalog, parent)} — declared once in the
 * durable logical data model, realized here.
 *
 * <p>The history is worth keeping. The {@code entities} primary key is {@code (realm_id, id)}, so
 * an id is unique within a realm, from which it *appears* that {@code parent_id} alone determines
 * the parent and {@code catalog_id} is redundant for correctness. That is an <b>inference</b> from
 * the key, not an observation of behaviour, and a first version of this class acted on it and
 * dropped the column. Two things were wrong with doing that. It changes a query in a refactor whose
 * whole job is to be behaviour-preserving, without first proving the two queries return the same
 * rows. And it settled inside one implementation a question that belongs to the data model — which
 * is exactly where the declared-path form now places it.
 *
 * <p>Leaving the shipped columns in place also removes a cost that version invented. The only index
 * serving a children lookup is the uniqueness constraint {@code (realm_id, catalog_id, parent_id,
 * type_code, name)}, whose leading columns are {@code realm_id, catalog_id}. Filtering on {@code
 * catalog_id} keeps that index applicable, so <b>no new index and no schema change is needed</b>.
 *
 * <p><b>2. {@link Precondition.Op#VERSION_EQUALS} is only satisfiable for entities.</b> Only the
 * {@code ENTITIES} table carries {@code entity_version} and {@code grant_records_version}; the
 * other four tables have no version column, and for grant and policy-mapping records every field is
 * part of the key so there is nothing to version. A version precondition against those kinds is
 * rejected rather than silently ignored.
 */
public class JdbcDurableRecordStore implements DurableRecordStore {

  /**
   * A policy ceiling, not a physical one. A relational transaction imposes no statement limit, so
   * this number is a deployment choice about how large a single atomic unit may get. Stated as a
   * declaration so a caller can size a request instead of retrying into a limit.
   */
  public static final int DEFAULT_MAX_ITEMS_PER_COMMIT = 1000;

  private final DatasourceOperations datasourceOperations;
  private final String realmId;
  private final int schemaVersion;
  private final int maxItemsPerCommit;
  private final Map<RecordKind, KindBinding<?>> bindings;

  /**
   * The single atomicity domain this instance serves. Opaque to callers by contract; they may
   * compare it and must not interpret it.
   */
  private final Object domain = new Object();

  public JdbcDurableRecordStore(
      @NonNull DatasourceOperations datasourceOperations,
      @NonNull String realmId,
      int schemaVersion) {
    this(datasourceOperations, realmId, schemaVersion, DEFAULT_MAX_ITEMS_PER_COMMIT);
  }

  public JdbcDurableRecordStore(
      @NonNull DatasourceOperations datasourceOperations,
      @NonNull String realmId,
      int schemaVersion,
      int maxItemsPerCommit) {
    this.datasourceOperations = datasourceOperations;
    this.realmId = realmId;
    this.schemaVersion = schemaVersion;
    this.maxItemsPerCommit = maxItemsPerCommit;
    this.bindings = buildBindings();
  }

  // ------------------------------------------------------------------ registry

  /**
   * What a kind needs in order to be stored: where its rows live, how to read one, which columns
   * each of the two addressing modes uses, and how each of the kind's declared lookup paths binds
   * to this schema.
   *
   * @param versionColumns the columns a {@link Precondition.Op#VERSION_EQUALS} may name, empty when
   *     the kind carries no version
   * @param paths the kind's declared lookup paths, realized as column bindings — the registration
   *     half of the declared-once-normative discipline; empty when the kind declares none
   */
  private record KindBinding<T>(
      String table,
      List<String> columns,
      Converter<T> reader,
      List<String> identityColumns,
      List<String> uniquenessColumns,
      Map<Precondition.VersionAttribute, String> versionColumns,
      Map<LookupPath, PathBinding> paths,
      @Nullable String orderColumn,
      Function<Object, Map<String, Object>> rowOf) {}

  /**
   * One declared lookup path, realized against this schema.
   *
   * @param anchorTypes the declared required anchors' types, in order
   * @param trailingType the declared optional trailing anchor's type, or null when the path
   *     declares none
   * @param anchorColumns the columns the required anchors bind to, in declared order; empty when a
   *     dedicated query serves the path
   * @param trailingColumn the column a declared optional trailing anchor binds to, or null
   * @param dedicatedQuery the query builder for a path whose realization is not an
   *     equality-anchored select over the kind's table, or null
   */
  private record PathBinding(
      List<Class<?>> anchorTypes,
      @Nullable Class<?> trailingType,
      List<String> anchorColumns,
      @Nullable String trailingColumn,
      @Nullable Function<List<Object>, QueryGenerator.PreparedQuery> dedicatedQuery) {}

  private static long asLong(Object anchor) {
    return ((Number) anchor).longValue();
  }

  private Map<RecordKind, KindBinding<?>> buildBindings() {
    DatabaseType databaseType = datasourceOperations.getDatabaseType();
    Map<RecordKind, KindBinding<?>> map = new LinkedHashMap<>();

    map.put(
        PolarisRecordKinds.ENTITY,
        new KindBinding<>(
            ModelEntity.TABLE_NAME,
            ModelEntity.getAllColumnNames(schemaVersion),
            new ModelEntity(schemaVersion),
            // identity is (realm, id): the table's primary key is (realm_id, id), so id alone
            // addresses one row within a realm. catalog_id is deliberately absent.
            List.of("id"),
            // the declared logical uniqueness tuple, realm structural: (parent, type, name).
            // catalog_id is NOT a reference component: the data model rules it "present for query
            // locality, not as part of the key", and a store may not demand a locality component
            // from a caller's reference. The physical UNIQUE index keeps catalog_id; a uniqueness
            // lookup here filters three columns plus realm and so does not use that index's full
            // prefix — a locality cost, not a correctness one.
            List.of("parent_id", "type_code", "name"),
            Map.of(
                Precondition.VersionAttribute.RECORD_VERSION, "entity_version",
                Precondition.VersionAttribute.GRANT_RECORDS_VERSION, "grant_records_version"),
            Map.of(
                // anchors (parent-catalog, parent [, subtype]): the parent ADDRESS bound to the
                // shipped children query's exact filter columns — see the class javadoc for why
                // catalog_id stays
                PolarisRecordKinds.ENTITY_BY_PARENT,
                new PathBinding(
                    List.of(Long.class, Long.class),
                    Integer.class,
                    List.of("catalog_id", "parent_id"),
                    "sub_type_code",
                    null),
                // anchors (catalog, prefix): the shipped overlap query, unchanged — its catalog
                // anchor and its exact-segment-OR-prefix condition are the declared realization
                PolarisRecordKinds.ENTITY_BY_LOCATION_PREFIX,
                new PathBinding(
                    List.of(Long.class, String.class),
                    null,
                    List.of(),
                    null,
                    anchors ->
                        QueryGenerator.generateOverlapQuery(
                            realmId,
                            schemaVersion,
                            asLong(anchors.get(0)),
                            (String) anchors.get(1)))),
            ModelEntity.ID_COLUMN,
            r -> ModelEntity.fromEntity((PolarisBaseEntity) r, schemaVersion).toMap(databaseType)));

    map.put(
        PolarisRecordKinds.GRANT_RECORD,
        new KindBinding<>(
            ModelGrantRecord.TABLE_NAME,
            ModelGrantRecord.ALL_COLUMNS,
            new ModelGrantRecord(),
            // every field is part of the key, so identity and uniqueness are the same tuple
            ModelGrantRecord.ALL_COLUMNS,
            ModelGrantRecord.ALL_COLUMNS,
            Map.of(),
            // the two directions the data model declares, each with its own index
            // (idx_grants_realm_securable, idx_grants_realm_grantee)
            Map.of(
                PolarisRecordKinds.GRANT_RECORD_BY_SECURABLE,
                new PathBinding(
                    List.of(Long.class, Long.class),
                    null,
                    List.of("securable_catalog_id", "securable_id"),
                    null,
                    null),
                PolarisRecordKinds.GRANT_RECORD_BY_GRANTEE,
                new PathBinding(
                    List.of(Long.class, Long.class),
                    null,
                    List.of("grantee_catalog_id", "grantee_id"),
                    null,
                    null)),
            null,
            r -> ModelGrantRecord.fromGrantRecord((PolarisGrantRecord) r).toMap(databaseType)));

    map.put(
        PolarisRecordKinds.POLICY_MAPPING,
        new KindBinding<>(
            ModelPolicyMappingRecord.TABLE_NAME,
            ModelPolicyMappingRecord.ALL_COLUMNS,
            new ModelPolicyMappingRecord(),
            List.of(
                "target_catalog_id",
                "target_id",
                "policy_type_code",
                "policy_catalog_id",
                "policy_id"),
            List.of(
                "target_catalog_id",
                "target_id",
                "policy_type_code",
                "policy_catalog_id",
                "policy_id"),
            Map.of(),
            Map.of(
                PolarisRecordKinds.POLICY_MAPPING_BY_TARGET,
                new PathBinding(
                    List.of(Long.class, Long.class),
                    null,
                    List.of("target_catalog_id", "target_id"),
                    null,
                    null),
                PolarisRecordKinds.POLICY_MAPPING_BY_POLICY,
                new PathBinding(
                    List.of(Long.class, Long.class),
                    null,
                    List.of("policy_catalog_id", "policy_id"),
                    null,
                    null)),
            null,
            r ->
                ModelPolicyMappingRecord.fromPolicyMappingRecord((PolarisPolicyMappingRecord) r)
                    .toMap(databaseType)));

    map.put(
        PolarisRecordKinds.PRINCIPAL_SECRETS,
        new KindBinding<>(
            ModelPrincipalAuthenticationData.TABLE_NAME,
            ModelPrincipalAuthenticationData.ALL_COLUMNS,
            new ModelPrincipalAuthenticationData(),
            // authentication resolves the secret before it resolves the principal, so the
            // client id is the identity
            List.of("principal_client_id"),
            List.of("principal_client_id"),
            Map.of(),
            // no declared list paths: the model's by-principal and enumeration paths are
            // documented gaps no shipped backend serves, not declarations to realize here
            Map.of(),
            null,
            r ->
                ModelPrincipalAuthenticationData.fromPrincipalAuthenticationData(
                        (PolarisPrincipalSecrets) r)
                    .toMap(databaseType)));

    map.put(
        PolarisRecordKinds.EVENT,
        new KindBinding<>(
            ModelEvent.TABLE_NAME,
            ModelEvent.ALL_COLUMNS,
            ModelEvent.CONVERTER,
            List.of("event_id"),
            List.of("event_id"),
            Map.of(),
            Map.of(),
            null,
            r -> ModelEvent.fromEvent((EventEntity) r).toMap(databaseType)));

    // Unmodifiable but ordered: the union form iterates this map, and handing iteration order to
    // Map.copyOf's per-run salting would make the union's concatenation order a per-JVM accident.
    return java.util.Collections.unmodifiableMap(map);
  }

  private KindBinding<?> binding(RecordKind kind) {
    KindBinding<?> b = bindings.get(kind);
    if (b == null) {
      // The store does not guess and does not silently drop the record: an unregistered kind is a
      // configuration error, which is what makes the kind -> store -> impl mapping checkable.
      throw new IllegalArgumentException(
          "No mapper registered for record kind '" + kind.id() + "' in this store");
    }
    return b;
  }

  // ------------------------------------------------------------------ writes

  @Override
  public @NonNull CommitResult commit(@NonNull List<Mutation> mutations) {
    if (mutations.size() > maxItemsPerCommit) {
      return CommitResult.tooManyItems();
    }
    for (Mutation m : mutations) {
      if (m.op() == Mutation.Op.DELETE && m.record() != null) {
        throw new IllegalArgumentException(
            "A DELETE carries no payload: the record is addressed by its target ref alone");
      }
      if (!domain.equals(domainOf(m.target()))) {
        return CommitResult.domainMismatch();
      }
      for (Precondition p : m.preconditions()) {
        if (p.ref().isPresent() && !domain.equals(domainOf(p.ref().get()))) {
          return CommitResult.domainMismatch();
        }
      }
    }

    List<Precondition> failed = new ArrayList<>();
    try {
      datasourceOperations.runWithinTransaction(
          connection -> {
            for (Mutation m : mutations) {
              if (!applyMutation(connection, m, failed)) {
                // Returning false rolls the whole transaction back. A failed precondition is never
                // a partial apply and never a silent no-op.
                return false;
              }
            }
            return true;
          });
    } catch (DisruptedTransactionException e) {
      CommitDisruptedException disrupted =
          new CommitDisruptedException(
              e.durableEffect(), "Failed to commit " + mutations.size() + " mutations", e);
      // Whatever also failed on the way out rides the exception a caller catches. That is where
      // this
      // type's own rule about a verdict and a later failure says to look for it, and a caller
      // should
      // not have to know how many times the failure was wrapped on its way here.
      for (Throwable alsoFailed : e.getSuppressed()) {
        disrupted.addSuppressed(alsoFailed);
      }
      throw disrupted;
    } catch (SQLException e) {
      // Everything from the transaction helper arrives classified. Anything else reaching here is
      // unclassified, and the only safe reading of an unclassified failure is the pessimistic one.
      throw new CommitDisruptedException(
          DurableEffect.UNKNOWN, "Failed to commit " + mutations.size() + " mutations", e);
    }
    return failed.isEmpty() ? CommitResult.applied() : CommitResult.preconditionFailed(failed);
  }

  private boolean applyMutation(Connection connection, Mutation m, List<Precondition> failed)
      throws SQLException {
    KindBinding<?> b = binding(m.kind());

    // Preconditions split in two: a condition on the mutation's own target folds into the
    // statement's WHERE clause, so it costs no extra round trip and the row count decides it.
    // Anything else needs its own read.
    Map<String, Object> foldedEquals = new LinkedHashMap<>();
    Set<String> foldedIsNull = new LinkedHashSet<>();
    Precondition foldedToken = null;
    for (Precondition p : m.preconditions()) {
      if (p.op() == Precondition.Op.NONE) {
        continue;
      }
      boolean onOwnTarget = p.ref().map(m.target()::equals).orElse(false);
      if (p.op() == Precondition.Op.VERSION_EQUALS && onOwnTarget) {
        foldedEquals.put(versionColumn(b, p), p.expectedVersion());
      } else if (p.op() == Precondition.Op.UNCHANGED_SINCE && onOwnTarget) {
        // The token carries this row's own column values, in the binding's column order, so the
        // check IS the write's WHERE clause. A competitor that commits first leaves no row matching
        // those values and the statement touches nothing. Checking it with a separate SELECT would
        // prove nothing at this store's isolation level: a competitor committing between the check
        // and the write would go unnoticed and the write would land on its row.
        List<@Nullable Object> parts = p.token().orElseThrow().parts();
        if (parts.size() != b.columns().size()) {
          throw new IllegalArgumentException(
              "An unchangedSince token for "
                  + m.kind().id()
                  + " carries "
                  + parts.size()
                  + " parts but the kind has "
                  + b.columns().size()
                  + " columns, so it was not issued by this store for this kind");
        }
        for (int i = 0; i < parts.size(); i++) {
          Object part = parts.get(i);
          if (part == null) {
            // A column that WAS null has to compare equal only to null, and `col = ?` bound to null
            // matches nothing in SQL.
            foldedIsNull.add(b.columns().get(i));
          } else {
            foldedEquals.put(b.columns().get(i), part);
          }
        }
        foldedToken = p;
      } else if (!checkPrecondition(connection, p)) {
        failed.add(p);
        return false;
      }
    }

    switch (m.op()) {
      case CREATE -> {
        // A row map's values legitimately contain null for nullable columns (e.g. a
        // non-namespace/table entity's location_without_scheme). List.copyOf rejects null
        // elements, so this copies into a null-tolerant list instead; the columns, the query
        // shape, and every non-null value are unchanged.
        List<Object> values = new ArrayList<>(b.rowOf().apply(m.record()).values());
        try {
          datasourceOperations.execute(
              connection,
              QueryGenerator.generateInsertQuery(b.columns(), b.table(), values, realmId));
        } catch (SQLException e) {
          if (datasourceOperations.isUniquenessConstraintViolation(e)) {
            // The primary key and the uniqueness constraint are what enforce a must-not-exist
            // condition, so a constraint violation IS the failed precondition.
            failed.add(notExistsPreconditionOf(m));
            return false;
          }
          throw e;
        }
      }
      case UPDATE -> {
        Map<String, Object> where = whereFor(b, m.target());
        where.putAll(foldedEquals);
        int rows =
            datasourceOperations.execute(
                connection,
                QueryGenerator.generateUpdateQuery(
                    b.columns(),
                    b.table(),
                    b.rowOf().apply(m.record()),
                    where,
                    Map.of(),
                    Map.of(),
                    foldedIsNull,
                    Set.of()));
        if (rows == 0) {
          failed.add(foldedToken != null ? foldedToken : firstVersionPrecondition(m));
          return false;
        }
      }
      case DELETE -> {
        Map<String, Object> where = whereFor(b, m.target());
        where.putAll(foldedEquals);
        int rows =
            datasourceOperations.execute(
                connection,
                QueryGenerator.generateDeleteQuery(
                    b.columns(), b.table(), where, Map.of(), Map.of(), foldedIsNull, Set.of()));
        // An unconditioned delete of an absent row stays a no-op, as it has always been here. Once
        // a condition was folded, though, zero rows means that condition did not hold: the row is
        // gone, or it is no longer the row the caller read.
        if (rows == 0 && (!foldedEquals.isEmpty() || !foldedIsNull.isEmpty())) {
          failed.add(foldedToken != null ? foldedToken : firstVersionPrecondition(m));
          return false;
        }
      }
    }
    return true;
  }

  private String versionColumn(KindBinding<?> b, Precondition p) {
    Precondition.VersionAttribute attribute =
        p.attribute()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "A VERSION_EQUALS precondition must name a version attribute"));
    String column = b.versionColumns().get(attribute);
    if (column == null) {
      throw new IllegalArgumentException(
          "Record kind has no "
              + attribute
              + " column, so a VERSION_EQUALS precondition cannot be satisfied against it");
    }
    return column;
  }

  private Precondition notExistsPreconditionOf(Mutation m) {
    return m.preconditions().stream()
        .filter(p -> p.op() == Precondition.Op.NOT_EXISTS)
        .findFirst()
        .orElseGet(() -> Precondition.notExists(m.target()));
  }

  private Precondition firstVersionPrecondition(Mutation m) {
    return m.preconditions().stream()
        .filter(p -> p.op() != Precondition.Op.NONE)
        .findFirst()
        .orElseGet(() -> Precondition.exists(m.target()));
  }

  /** Evaluates one precondition that could not be folded into a statement. */
  private boolean checkPrecondition(Connection connection, Precondition p) throws SQLException {
    Optional<RecordRef> maybeRef = p.ref();
    if (maybeRef.isEmpty()) {
      return true;
    }
    RecordRef ref = maybeRef.get();
    KindBinding<?> b = binding(ref.kind());
    Map<String, Object> where = whereFor(b, ref);
    if (p.op() == Precondition.Op.UNCHANGED_SINCE) {
      // A condition on a record this mutation does not write, decided the way EXISTS and
      // VERSION_EQUALS are decided for another record: one read inside this same transaction. The
      // staleness a competitor's commit could introduce between this read and this transaction's
      // own
      // commit is the window the contract already accepts for a condition on another record,
      // because
      // a stale read of a record nobody here writes cannot corrupt that record. A condition on the
      // mutation's OWN target never reaches this method: it folds into that statement's WHERE
      // clause,
      // where the row count decides it and no window exists.
      List<?> rows =
          datasourceOperations.executeSelect(
              connection,
              QueryGenerator.generateSelectQuery(b.columns(), b.table(), where),
              b.reader());
      if (rows.isEmpty()) {
        // unchangedSince asserts the record is still the one that was read, so an absent record
        // fails it rather than vacuously satisfying it.
        return false;
      }
      // Built by the same function that issued the token in read(), so what was handed out and what
      // is compared here line up by construction rather than by convention.
      ReadToken current = ReadToken.of(new ArrayList<>(b.rowOf().apply(rows.getFirst()).values()));
      return p.token().orElseThrow().equals(current);
    }
    if (p.op() == Precondition.Op.VERSION_EQUALS) {
      where.put(versionColumn(b, p), p.expectedVersion());
    }
    // Project exactly the columns filtered on: QueryGenerator validates that every where-column is
    // among the projections.
    List<String> projections = new ArrayList<>(where.keySet());
    projections.remove("realm_id");
    boolean present =
        !datasourceOperations
            .executeSelect(
                connection,
                QueryGenerator.generateSelectQuery(projections, b.table(), where),
                rowReader(rs -> Boolean.TRUE))
            .isEmpty();
    return switch (p.op()) {
      case NOT_EXISTS -> !present;
      case EXISTS, VERSION_EQUALS -> present;
      // Decided above, before the presence query, because it needs the row's value columns rather
      // than only its existence. Reaching here would mean that early return was removed.
      case UNCHANGED_SINCE ->
          throw new IllegalStateException(
              "An unchangedSince condition is decided before the presence query");
      case NONE -> true;
    };
  }

  /** Reads one row into a value. Narrower than a {@link Converter}, which also writes. */
  private interface RowReader<T> {
    T read(java.sql.ResultSet rs) throws SQLException;
  }

  /**
   * Adapts a read-only row function to {@link Converter}, which {@code executeSelect} requires.
   * {@code Converter} is not a functional interface because it also carries the write direction,
   * and a projection narrower than a whole table has no write direction to offer.
   */
  private static <T> Converter<T> rowReader(RowReader<T> reader) {
    return new Converter<T>() {
      @Override
      public T fromResultSet(java.sql.ResultSet rs) throws SQLException {
        return reader.read(rs);
      }

      @Override
      public Map<String, Object> toMap(DatabaseType databaseType) {
        throw new UnsupportedOperationException(
            "This converter reads a projection and cannot write a row");
      }
    };
  }

  @Override
  public long generateNewId() {
    return IdGenerator.getIdGenerator().nextId();
  }

  // ------------------------------------------------------------------ reads

  @Override
  public @NonNull <T> Optional<T> get(@NonNull RecordRef ref, @NonNull Class<T> type) {
    KindBinding<?> b = binding(ref.kind());
    try {
      List<?> rows =
          datasourceOperations.executeSelect(
              QueryGenerator.generateSelectQuery(b.columns(), b.table(), whereFor(b, ref)),
              b.reader());
      return rows.isEmpty() ? Optional.empty() : Optional.of(type.cast(rows.getFirst()));
    } catch (SQLException e) {
      throw new RuntimeException("Failed to read " + ref.kind().id(), e);
    }
  }

  @Override
  public @NonNull <T> Optional<Read<T>> read(@NonNull RecordRef ref, @NonNull Class<T> type) {
    KindBinding<?> b = binding(ref.kind());
    // Costs no extra statement: get already selects every column. The token is those columns' own
    // values in the binding's column order, which is the order applyMutation folds them back into a
    // WHERE clause, so what is read and what is verified line up by construction.
    return get(ref, type)
        .map(
            record ->
                new Read<>(
                    record, ReadToken.of(new ArrayList<>(b.rowOf().apply(record).values()))));
  }

  @Override
  public @NonNull <T> List<Optional<T>> getMany(
      @NonNull List<RecordRef> refs, @NonNull Class<T> type) {
    // Positional, so a caller can correlate results with requests. One statement per ref keeps the
    // two addressing modes uniform; a batched form is an optimisation, not a contract change.
    List<Optional<T>> out = new ArrayList<>(refs.size());
    for (RecordRef ref : refs) {
      out.add(get(ref, type));
    }
    return out;
  }

  @Override
  public @NonNull <T> Page<T> list(
      @NonNull RecordKind kind,
      @NonNull LookupPath path,
      @NonNull List<Object> anchors,
      @NonNull PageToken pageToken,
      @NonNull Class<T> type) {
    KindBinding<?> b = binding(kind);
    PathBinding p = pathBinding(kind, b, path, anchors);
    QueryGenerator.PreparedQuery query = listQuery(b, p, anchors, pageToken);
    try {
      AtomicReference<Page<T>> result = new AtomicReference<>();
      datasourceOperations.executeSelectOverStream(
          query, b.reader(), stream -> result.set(pageOf(b, p, kind, pageToken, stream, type)));
      return result.get();
    } catch (SQLException e) {
      throw new RuntimeException("Failed to list " + kind.id(), e);
    }
  }

  @Override
  public @NonNull <T> Page<T> list(
      @NonNull LookupPath path,
      @NonNull List<Object> anchors,
      @NonNull PageToken pageToken,
      @NonNull Class<T> type) {
    // The union over every registered kind declaring the path, evaluated store-side. Every Polaris
    // path today is declared by exactly one kind, so the union delegates with full paging. The
    // multi-kind branch below is unreachable until a data-model change introduces the first shared
    // path name, and that change is also where its semantics (paging across kinds, one-snapshot
    // isolation, per-kind anchor signatures) get declared — a shared path enters the vocabulary
    // only through the data model, never through this code growing cleverness first.
    List<RecordKind> declaring =
        bindings.entrySet().stream()
            .filter(e -> e.getValue().paths().containsKey(path))
            .map(Map.Entry::getKey)
            .toList();
    if (declaring.isEmpty()) {
      throw new IllegalArgumentException(
          "No registered kind declares lookup path '" + path.name() + "' in this store");
    }
    if (declaring.size() == 1) {
      return list(declaring.getFirst(), path, anchors, pageToken, type);
    }
    List<T> out = new ArrayList<>();
    for (RecordKind kind : declaring) {
      out.addAll(list(kind, path, anchors, pageToken, type).items());
    }
    return Page.page(pageToken, out, null);
  }

  /** Resolves a declared path and validates the anchors against its declared signature. */
  private PathBinding pathBinding(
      RecordKind kind, KindBinding<?> b, LookupPath path, List<Object> anchors) {
    PathBinding p = b.paths().get(path);
    if (p == null) {
      throw new IllegalArgumentException(
          "Record kind '"
              + kind.id()
              + "' declares no lookup path '"
              + path.name()
              + "' in this store");
    }
    int min = p.anchorTypes().size();
    int max = min + (p.trailingType() == null ? 0 : 1);
    if (anchors.size() < min || anchors.size() > max) {
      throw new IllegalArgumentException(
          "Lookup path '"
              + path.name()
              + "' of kind '"
              + kind.id()
              + "' declares "
              + (min == max ? min + " anchors" : min + " to " + max + " anchors")
              + ", got "
              + anchors.size());
    }
    for (int i = 0; i < anchors.size(); i++) {
      Class<?> declared = i < min ? p.anchorTypes().get(i) : p.trailingType();
      if (!declared.isInstance(anchors.get(i))) {
        throw new IllegalArgumentException(
            "Lookup path '"
                + path.name()
                + "' of kind '"
                + kind.id()
                + "' declares anchor "
                + i
                + " as "
                + declared.getSimpleName()
                + ", got "
                + (anchors.get(i) == null ? "null" : anchors.get(i).getClass().getSimpleName()));
      }
    }
    return p;
  }

  private <T> Page<T> pageOf(
      KindBinding<?> b,
      PathBinding p,
      RecordKind kind,
      PageToken pageToken,
      Stream<?> stream,
      Class<T> type) {
    // A keyset token may only be issued where listQuery actually applied the keyset. A path with
    // a dedicated query never does, so issuing a token there would hand the caller a continuation
    // the next request cannot honor — the same first page forever.
    if (b.orderColumn() != null
        && p.dedicatedQuery() == null
        && PolarisRecordKinds.ENTITY.equals(kind)) {
      @SuppressWarnings("unchecked")
      Stream<PolarisBaseEntity> entities = (Stream<PolarisBaseEntity>) stream;
      return Page.mapped(pageToken, entities, type::cast, EntityIdToken::fromEntity);
    }
    // Kinds with no single ordering column are returned as one page. Keyset pagination needs an
    // ordered key, and a grant or policy-mapping row has no column that plays that role.
    return Page.page(pageToken, stream.map(type::cast).toList(), null);
  }

  private QueryGenerator.PreparedQuery listQuery(
      KindBinding<?> b, PathBinding p, List<Object> anchors, PageToken pageToken) {
    if (p.dedicatedQuery() != null) {
      return p.dedicatedQuery().apply(anchors);
    }
    Map<String, Object> whereEquals = new LinkedHashMap<>();
    Map<String, Object> whereGreater = new LinkedHashMap<>();
    String orderBy = null;

    List<String> columns = p.anchorColumns();
    for (int i = 0; i < columns.size(); i++) {
      whereEquals.put(columns.get(i), anchors.get(i));
    }
    if (anchors.size() > columns.size() && p.trailingColumn() != null) {
      whereEquals.put(p.trailingColumn(), anchors.get(anchors.size() - 1));
    }

    whereEquals.put("realm_id", realmId);
    if (pageToken.paginationRequested() && b.orderColumn() != null) {
      orderBy = b.orderColumn();
      pageToken
          .valueAs(EntityIdToken.class)
          .ifPresent(t -> whereGreater.put(b.orderColumn(), t.entityId()));
    }
    return QueryGenerator.generateSelectQuery(
        b.columns(), b.table(), whereEquals, whereGreater, orderBy);
  }

  @Override
  public @NonNull List<Optional<RecordVersions>> versionsOf(@NonNull List<RecordRef> refs) {
    List<Optional<RecordVersions>> out = new ArrayList<>(refs.size());
    for (RecordRef ref : refs) {
      KindBinding<?> b = binding(ref.kind());
      if (b.versionColumns().isEmpty()) {
        throw new IllegalArgumentException(
            "Record kind '" + ref.kind().id() + "' carries no version, so versionsOf is undefined");
      }
      Map<String, Object> where = whereFor(b, ref);
      // The narrow read this operation exists for: the version columns plus the key columns being
      // filtered on, never the full row.
      List<String> projections = new ArrayList<>(where.keySet());
      projections.remove("realm_id");
      String recordVersion = b.versionColumns().get(Precondition.VersionAttribute.RECORD_VERSION);
      String grantVersion =
          b.versionColumns().get(Precondition.VersionAttribute.GRANT_RECORDS_VERSION);
      projections.add(recordVersion);
      projections.add(grantVersion);
      try {
        List<RecordVersions> rows =
            datasourceOperations.executeSelect(
                QueryGenerator.generateSelectQuery(projections, b.table(), where),
                rowReader(
                    rs -> new RecordVersions(rs.getLong(recordVersion), rs.getLong(grantVersion))));
        out.add(rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst()));
      } catch (SQLException e) {
        throw new RuntimeException("Failed to read versions of " + ref.kind().id(), e);
      }
    }
    return out;
  }

  // ------------------------------------------------------------------ declarations

  @Override
  public @NonNull Object domainOf(@NonNull RecordRef target) {
    // One database is one atomicity domain, so every target in this instance shares it. A backend
    // that partitions would return the partition the target lives in.
    binding(target.kind());
    return domain;
  }

  @Override
  public int maxItemsPerCommit() {
    return maxItemsPerCommit;
  }

  // ------------------------------------------------------------------ helpers

  /**
   * The WHERE clause addressing exactly the row a reference names, in whichever of the two modes
   * the reference uses. The realm is always added: an instance serves one realm and is never asked
   * which.
   */
  private Map<String, Object> whereFor(KindBinding<?> b, RecordRef ref) {
    List<String> columns =
        ref.mode() == RecordRef.Mode.IDENTITY ? b.identityColumns() : b.uniquenessColumns();
    if (columns.size() != ref.key().size()) {
      throw new IllegalArgumentException(
          "Reference to "
              + ref.kind().id()
              + " by "
              + ref.mode()
              + " needs "
              + columns.size()
              + " key parts, got "
              + ref.key().size());
    }
    Map<String, Object> where = new LinkedHashMap<>();
    for (int i = 0; i < columns.size(); i++) {
      where.put(columns.get(i), ref.key().get(i));
    }
    where.put("realm_id", realmId);
    return where;
  }
}
