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

"""Logical layout supplied by the domain; adapters interpret only these declarations."""
from dataclasses import dataclass


TABLES = {
    "Entities": (("Scope", "Id", "Parent", "Kind", "Name", "Ev", "Gv"), 2),
    "Names": (("Scope", "Parent", "Kind", "Name", "Id"), 4),
    "Versions": (("Scope", "Id", "Ev", "Gv"), 2),
    "Grants": (("SecScope", "SecId", "GranteeScope", "GranteeId", "Privilege"), 5),
}
FIELD_TYPES = {
    kind: {field: "int" if field in ("Ev", "Gv") else "string" for field in columns}
    for kind, (columns, _) in TABLES.items()
}
# Each path is a declared equality-prefix lookup, not arbitrary domain code.
LOOKUPS = {
    "children": ("Names", ("Scope", "Parent")),
    "securable_grants": ("Grants", ("SecScope", "SecId")),
    "grantee_grants": ("Grants", ("GranteeScope", "GranteeId")),
}


def secondary_paths(table):
    columns, _ = TABLES[table]
    return {
        name: fields for name, (kind, fields) in LOOKUPS.items()
        if kind == table and tuple(columns[:len(fields)]) != fields
    }


@dataclass(frozen=True)
class Mutation:
    kind: str
    key: tuple
    row: tuple | None  # None means delete.


class Batch:
    """Final changes accumulated in memory. No reads or database query overlay."""
    def __init__(self):
        self.changes = {}

    def put(self, kind, row):
        row = tuple(row)
        key = row[:TABLES[kind][1]]
        self.changes[kind, key] = Mutation(kind, key, row)

    def delete(self, kind, key):
        key = tuple(key)
        self.changes[kind, key] = Mutation(kind, key, None)

    def final(self):
        return tuple(self.changes.values())
