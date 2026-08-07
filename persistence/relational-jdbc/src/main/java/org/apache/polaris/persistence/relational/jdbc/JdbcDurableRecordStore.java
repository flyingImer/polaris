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
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
 * <h2>Two things the contract does not yet express cleanly</h2>
 *
 * <p><b>1. A scope anchor is a parent address, not a record reference.</b> {@link
 * ListScope.Shape#CHILDREN_OF_PARENT} takes a {@link RecordRef} for the parent, but an entity row
 * stores its parent as the pair {@code (catalog_id, parent_id)} while an entity's own identity is
 * the single {@code id}. {@code catalog_id} is a denormalised scoping column, not part of the
 * parent's identity, and it cannot be derived by resolving the parent: a catalog's own {@code
 * catalog_id} is 0, not its id. So the anchor's key is read here as the pair {@code [catalogId,
 * parentId]}, which is not what {@code byIdentity} means elsewhere. Recorded as a contract question
 * rather than papered over.
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
   * What a kind needs in order to be stored: where its rows live, how to read one, and which
   * columns each of the two addressing modes uses.
   *
   * @param versionColumns the columns a {@link Precondition.Op#VERSION_EQUALS} may name, empty when
   *     the kind carries no version
   */
  private record KindBinding<T>(
      String table,
      List<String> columns,
      Converter<T> reader,
      List<String> identityColumns,
      List<String> uniquenessColumns,
      Map<Precondition.VersionAttribute, String> versionColumns,
      @Nullable String orderColumn,
      Function<Object, Map<String, Object>> rowOf) {}

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
            List.of("catalog_id", "parent_id", "type_code", "name"),
            Map.of(
                Precondition.VersionAttribute.RECORD_VERSION, "entity_version",
                Precondition.VersionAttribute.GRANT_RECORDS_VERSION, "grant_records_version"),
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
            null,
            r -> ModelEvent.fromEvent((EventEntity) r).toMap(databaseType)));

    return Map.copyOf(map);
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
    } catch (SQLException e) {
      throw new RuntimeException("Failed to commit " + mutations.size() + " mutations", e);
    }
    return failed.isEmpty() ? CommitResult.applied() : CommitResult.preconditionFailed(failed);
  }

  private boolean applyMutation(Connection connection, Mutation m, List<Precondition> failed)
      throws SQLException {
    KindBinding<?> b = binding(m.kind());

    // Preconditions split in two: a version check on the mutation's own target folds into the
    // statement's WHERE clause, so it costs no extra round trip. Anything else needs its own read.
    Map<String, Object> foldedVersions = new LinkedHashMap<>();
    for (Precondition p : m.preconditions()) {
      if (p.op() == Precondition.Op.NONE) {
        continue;
      }
      if (p.op() == Precondition.Op.VERSION_EQUALS
          && p.ref().map(m.target()::equals).orElse(false)) {
        String column = versionColumn(b, p);
        foldedVersions.put(column, p.expectedVersion());
      } else if (!checkPrecondition(connection, p)) {
        failed.add(p);
        return false;
      }
    }

    switch (m.op()) {
      case CREATE -> {
        List<Object> values = List.copyOf(b.rowOf().apply(m.record()).values());
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
        where.putAll(foldedVersions);
        List<Object> values = List.copyOf(b.rowOf().apply(m.record()).values());
        int rows =
            datasourceOperations.execute(
                connection,
                QueryGenerator.generateUpdateQuery(b.columns(), b.table(), values, where));
        if (rows == 0) {
          failed.add(firstVersionPrecondition(m));
          return false;
        }
      }
      case DELETE -> {
        Map<String, Object> where = whereFor(b, m.target());
        where.putAll(foldedVersions);
        int rows =
            datasourceOperations.execute(
                connection, QueryGenerator.generateDeleteQuery(b.columns(), b.table(), where));
        if (rows == 0 && !foldedVersions.isEmpty()) {
          failed.add(firstVersionPrecondition(m));
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
      @NonNull ListScope scope, @NonNull PageToken pageToken, @NonNull Class<T> type) {
    RecordKind kind =
        scope
            .kind()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "A kind-less scope spans every registered kind and is not implemented by"
                            + " this store yet; see hasChildren in the plan"));
    KindBinding<?> b = binding(kind);
    QueryGenerator.PreparedQuery query = listQuery(b, scope, pageToken);
    try {
      AtomicReference<Page<T>> result = new AtomicReference<>();
      datasourceOperations.executeSelectOverStream(
          query, b.reader(), stream -> result.set(pageOf(b, scope, pageToken, stream, type)));
      return result.get();
    } catch (SQLException e) {
      throw new RuntimeException("Failed to list " + kind.id(), e);
    }
  }

  private <T> Page<T> pageOf(
      KindBinding<?> b, ListScope scope, PageToken pageToken, Stream<?> stream, Class<T> type) {
    if (b.orderColumn() != null && PolarisRecordKinds.ENTITY.id().equals(scope.kind().get().id())) {
      @SuppressWarnings("unchecked")
      Stream<PolarisBaseEntity> entities = (Stream<PolarisBaseEntity>) stream;
      return Page.mapped(pageToken, entities, type::cast, EntityIdToken::fromEntity);
    }
    // Kinds with no single ordering column are returned as one page. Keyset pagination needs an
    // ordered key, and a grant or policy-mapping row has no column that plays that role.
    return Page.page(pageToken, stream.map(type::cast).toList(), null);
  }

  private QueryGenerator.PreparedQuery listQuery(
      KindBinding<?> b, ListScope scope, PageToken pageToken) {
    Map<String, Object> whereEquals = new LinkedHashMap<>();
    Map<String, Object> whereGreater = new LinkedHashMap<>();
    String orderBy = null;

    switch (scope.shape()) {
      case CHILDREN_OF_PARENT -> {
        // See the class javadoc: the anchor's key is the parent ADDRESS, which for entities is the
        // pair (catalog_id, parent_id) rather than the parent's own identity.
        List<Object> anchor = scope.anchor().key();
        if (anchor.size() != 2) {
          throw new IllegalArgumentException(
              "CHILDREN_OF_PARENT expects the anchor key to be [catalogId, parentId]");
        }
        whereEquals.put("catalog_id", anchor.get(0));
        whereEquals.put("parent_id", anchor.get(1));
        scope.subtype().ifPresent(st -> whereEquals.put("sub_type_code", st));
      }
      case REFERENCING -> {
        List<Object> anchor = scope.anchor().key();
        List<String> columns = referencingColumns(b, anchor.size());
        for (int i = 0; i < columns.size(); i++) {
          whereEquals.put(columns.get(i), anchor.get(i));
        }
      }
      case UNDER_LOCATION_PREFIX -> {
        List<Object> anchor = scope.anchor().key();
        if (anchor.size() != 1) {
          throw new IllegalArgumentException(
              "UNDER_LOCATION_PREFIX expects the anchor key to be [catalogId]");
        }
        return QueryGenerator.generateOverlapQuery(
            realmId,
            schemaVersion,
            ((Number) anchor.getFirst()).longValue(),
            scope.locationPrefix().orElseThrow());
      }
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

  /**
   * Which columns a {@link ListScope.Shape#REFERENCING} anchor binds to. Grant records are the
   * shape this exists for: grants on a securable and grants to a grantee are the same scope with a
   * different anchor, which is why the shipped interface's two methods collapse into one.
   */
  private List<String> referencingColumns(KindBinding<?> b, int anchorSize) {
    if (ModelGrantRecord.TABLE_NAME.equals(b.table()) && anchorSize == 2) {
      return List.of("securable_catalog_id", "securable_id");
    }
    if (ModelPolicyMappingRecord.TABLE_NAME.equals(b.table()) && anchorSize == 2) {
      return List.of("target_catalog_id", "target_id");
    }
    throw new IllegalArgumentException(
        "REFERENCING is not defined for table "
            + b.table()
            + " with a "
            + anchorSize
            + "-part anchor");
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
