#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""Four native adapters; business operations are not imported here.

Identifiers come only from the supplied, closed layout declarations. This is a
prototype adapter surface, not a general SQL/query engine or production driver.
"""
import functools
import json

from bindings import FIELD_TYPES, LOOKUPS, TABLES, secondary_paths
from primitives import Attempt, Conflict, Unknown


def protected_read(method):
    @functools.wraps(method)
    def invoke(self, *args, **kwargs):
        self.require_open()
        try:
            return method(self, *args, **kwargs)
        except Exception as error:
            outcome = self._classify(error)
            self.state = "UNKNOWN" if isinstance(outcome, Unknown) else "ABORTED"
            raise outcome from (error if outcome is not error else None)
    return invoke


def q(name):
    return '"' + name + '"'


def sql_schema():
    statements = []
    for kind, (columns, key_size) in TABLES.items():
        fields = [q("Realm") + " TEXT NOT NULL"]
        fields += [q(c) + (" BIGINT" if FIELD_TYPES[kind][c] == "int" else " TEXT") + " NOT NULL" for c in columns]
        keys = ("Realm", *columns[:key_size])
        statements.append(f'CREATE TABLE {q(kind)} ({",".join(fields)}, PRIMARY KEY ({",".join(map(q, keys))}))')
        for name, fields in secondary_paths(kind).items():
            statements.append(f'CREATE INDEX {q(name)} ON {q(kind)} ({",".join(map(q, ("Realm", *fields)))})')
    return statements


class SqlStore:
    def __init__(self, dsn):
        import psycopg
        self.psycopg, self.dsn = psycopg, dsn
        with psycopg.connect(dsn, autocommit=True) as connection:
            for sql in sql_schema():
                connection.execute(sql)
            self.version = connection.execute("SELECT version()").fetchone()[0]

    def begin(self, realm, **faults):
        return SqlAttempt(self, realm, **faults)


class SqlAttempt(Attempt):
    def __init__(self, store, realm, **faults):
        super().__init__(**faults)
        self.connection = store.psycopg.connect(store.dsn, autocommit=True)
        self.connection.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
        self.connection.execute("SET LOCAL statement_timeout = '10s'")
        self.realm = realm

    @protected_read
    def get(self, kind, key):
        self.record("point_read", kind=kind)
        return next(iter(self._select(kind, TABLES[kind][0][:len(key)], key)), None)

    @protected_read
    def lookup(self, path, args, limit=None):
        self.record("range_read", path=path, limit=limit)
        kind, fields = LOOKUPS[path]
        return self._select(kind, fields, args, limit)

    @protected_read
    def scan(self, kind):
        self.record("audit_scan", kind=kind)
        return self._select(kind, (), ())

    def _select(self, kind, fields, values, limit=None):
        fields = ("Realm", *fields)
        where = " AND ".join(q(c) + "=%s" for c in fields)
        sql = f'SELECT {",".join(map(q, TABLES[kind][0]))} FROM {q(kind)} WHERE {where}'
        if limit is not None:
            sql += " LIMIT " + str(int(limit))
        return list(self.connection.execute(sql, (self.realm, *values)))

    def _apply(self, change):
        columns, key_size = TABLES[change.kind]
        self.record("mutation", kind=change.kind, delete=change.row is None)
        if change.row is None:
            fields = ("Realm", *columns[:key_size])
            where = " AND ".join(q(c) + "=%s" for c in fields)
            self.connection.execute(f'DELETE FROM {q(change.kind)} WHERE {where}', (self.realm, *change.key))
        else:
            fields = ("Realm", *columns)
            keys = ("Realm", *columns[:key_size])
            updates = ",".join(q(c) + "=EXCLUDED." + q(c) for c in columns[key_size:])
            action = "DO UPDATE SET " + updates if updates else "DO NOTHING"
            self.connection.execute(
                f'INSERT INTO {q(change.kind)} ({",".join(map(q, fields))}) '
                f'VALUES ({",".join(["%s"] * len(fields))}) '
                f'ON CONFLICT ({",".join(map(q, keys))}) {action}',
                (self.realm, *change.row))

    def _commit(self):
        self.connection.execute("COMMIT")

    def _rollback(self):
        self.connection.execute("ROLLBACK")

    def _close(self):
        self.connection.close()

    def _classify(self, error):
        code = getattr(error, "sqlstate", None)
        if code in ("40001", "40P01"):
            return Conflict(str(error))
        if code == "40003" or isinstance(error, Unknown):
            return Unknown(str(error))
        if self.state == "COMMITTING":
            return Unknown(str(error))  # Conservative; production adapters refine error taxonomy.
        return error


class FdbStore:
    def __init__(self, cluster_file):
        import fdb
        import fdb.tuple
        fdb.api_version(730)
        self.fdb, self.tuple = fdb, fdb.tuple
        self.db = fdb.open(str(cluster_file))
        self.version = "FoundationDB 7.3.77 (pinned server and client)"
        # This experiment supports secondary keys formed from primary key fields.
        for kind, (columns, size) in TABLES.items():
            for fields in secondary_paths(kind).values():
                if not set(fields) <= set(columns[:size]):
                    raise ValueError("this bounded FDB binding needs key-derived secondary indexes")

    def begin(self, realm, **faults):
        return FdbAttempt(self, realm, **faults)


class FdbAttempt(Attempt):
    def __init__(self, store, realm, **faults):
        super().__init__(**faults)
        self.store, self.realm = store, realm
        self.tr = store.db.create_transaction()
        self.tr.options.set_timeout(10000)

    def key(self, kind, key):
        return self.store.tuple.pack((self.realm, kind, *key))

    @protected_read
    def get(self, kind, key):
        self.record("point_read", kind=kind)
        value = self.tr.get(self.key(kind, key)).value
        return tuple(json.loads(value)) if value is not None else None

    @protected_read
    def lookup(self, path, args, limit=None):
        kind, fields = LOOKUPS[path]
        self.record("range_read", path=path, limit=limit, unsafe_snapshot=self.unsafe_empty)
        prefix = self.key("index:" + path if path in secondary_paths(kind) else kind, args)
        reader = self.tr.snapshot if self.unsafe_empty else self.tr
        return [tuple(json.loads(pair.value)) for pair in reader.get_range_startswith(prefix, limit=limit or 0)]

    @protected_read
    def scan(self, kind):
        self.record("audit_scan", kind=kind)
        return [tuple(json.loads(pair.value)) for pair in self.tr.get_range_startswith(self.key(kind, ()))]

    def _apply(self, change):
        self.record("mutation", kind=change.kind, delete=change.row is None)
        keys = [self.key(change.kind, change.key)]
        columns, size = TABLES[change.kind]
        key_fields = dict(zip(columns[:size], change.key))
        for name, fields in secondary_paths(change.kind).items():
            keys.append(self.key("index:" + name, (*[key_fields[c] for c in fields], *change.key)))
        for key in keys:
            if change.row is None:
                self.tr.clear(key)
                self.record("native_clear")
            else:
                self.tr.set(key, json.dumps(change.row).encode())
                self.record("native_set")

    def _commit(self):
        self.tr.commit().wait()

    def _rollback(self):
        self.tr.cancel()

    def _close(self):
        pass

    def _classify(self, error):
        if isinstance(error, self.store.fdb.FDBError):
            if error.code == 1020:
                return Conflict(str(error))
            if error.code == 1021 or self.state == "COMMITTING":
                return Unknown(str(error))
        return error


def spanner_schema():
    statements = []
    for kind, (columns, key_size) in TABLES.items():
        fields = ["Realm STRING(128) NOT NULL"]
        fields += [f'`{c}` ' + ("INT64" if FIELD_TYPES[kind][c] == "int" else "STRING(256)") + " NOT NULL" for c in columns]
        keys = ("Realm", *columns[:key_size])
        statements.append(f'CREATE TABLE {kind} ({",".join(fields)}) PRIMARY KEY ({",".join(keys)})')
        for name, fields in secondary_paths(kind).items():
            statements.append(f'CREATE INDEX {name} ON {kind} ({",".join(("Realm", *fields))})')
    return statements


class SpannerStore:
    def __init__(self, database):
        self.database = database
        self.version = "Cloud Spanner emulator 1.5.57 (not production Spanner)"

    def begin(self, realm, **faults):
        return SpannerAttempt(self, realm, **faults)


class SpannerAttempt(Attempt):
    def __init__(self, store, realm, **faults):
        from google.cloud.spanner_v1 import KeySet, param_types
        super().__init__(**faults)
        self.KeySet, self.param_types, self.realm = KeySet, param_types, realm
        self.session = store.database.session()
        self.session.create()
        self.tr = self.session.transaction()
        self.tr.begin()

    @protected_read
    def get(self, kind, key):
        self.record("point_read", kind=kind)
        rows = list(self.tr.read(kind, TABLES[kind][0], self.KeySet(keys=[(self.realm, *key)]), timeout=10))
        return tuple(rows[0]) if rows else None

    @protected_read
    def lookup(self, path, args, limit=None):
        self.record("range_read", path=path, limit=limit)
        kind, fields = LOOKUPS[path]
        return self._select(kind, fields, args, limit, path if path in secondary_paths(kind) else None)

    @protected_read
    def scan(self, kind):
        self.record("audit_scan", kind=kind)
        return self._select(kind, (), ())

    def _select(self, kind, fields, values, limit=None, index=None):
        fields = ("Realm", *fields)
        params = {f"p{i}": v for i, v in enumerate((self.realm, *values))}
        types = {name: self.param_types.STRING for name in params}
        where = " AND ".join(f'`{c}`=@p{i}' for i, c in enumerate(fields))
        table = kind + ("@{FORCE_INDEX=" + index + "}" if index else "")
        sql = f'SELECT {",".join("`" + c + "`" for c in TABLES[kind][0])} FROM {table} WHERE {where}'
        if limit is not None:
            sql += " LIMIT " + str(int(limit))
        return [tuple(row) for row in self.tr.execute_sql(sql, params=params, param_types=types, timeout=10)]

    def _apply(self, change):
        self.record("mutation", kind=change.kind, delete=change.row is None)
        if change.row is None:
            self.tr.delete(change.kind, self.KeySet(keys=[(self.realm, *change.key)]))
        else:
            self.tr.insert_or_update(change.kind, ("Realm", *TABLES[change.kind][0]), [(self.realm, *change.row)])
        self.record("buffer_mutation")

    def _commit(self):
        self.tr.commit()

    def _rollback(self):
        from google.api_core.exceptions import Aborted, NotFound
        try:
            self.tr.rollback()
        except (Aborted, NotFound):
            pass

    def _close(self):
        self.session.delete()

    def _classify(self, error):
        from google.api_core.exceptions import Aborted
        if isinstance(error, Aborted):
            return Conflict(str(error))
        if self.state == "COMMITTING":
            return Unknown(str(error))
        return error
