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

"""Shared, deliberately bounded Polaris metadata workflows. No database imports.

These are extracted shapes, not a replacement for the upstream Java Manager.
IDs and external preparation are supplied before the metadata attempt.
"""
from dataclasses import dataclass, replace

from bindings import Batch
from primitives import Conflict, Unknown


class Rejected(Exception):
    pass


@dataclass(frozen=True)
class Entity:
    scope: str
    id: str
    parent: str
    kind: str
    name: str
    ev: int = 1
    gv: int = 1

    @property
    def key(self):
        return self.scope, self.id

    @property
    def name_key(self):
        return self.scope, self.parent, self.kind, self.name

    def row(self):
        return self.scope, self.id, self.parent, self.kind, self.name, self.ev, self.gv


@dataclass(frozen=True)
class Prepared:
    changes: tuple
    candidate: object


def load(view, key):
    row = view.get("Entities", key)
    return Entity(*row) if row else None


def require(view, key):
    entity = load(view, key)
    if entity is None:
        raise Rejected("entity missing")
    return entity


def save(batch, entity, *, new=False):
    batch.put("Entities", entity.row())
    batch.put("Versions", (*entity.key, entity.ev, entity.gv))
    if new:
        batch.put("Names", (*entity.name_key, entity.id))


def erase(batch, entity):
    batch.delete("Entities", entity.key)
    batch.delete("Versions", entity.key)
    batch.delete("Names", entity.name_key)


def create_catalog(view, identity="catalog-1", name="warehouse"):
    existing = load(view, ("0", identity))
    if existing:
        binding = view.get("Names", (identity, identity, "catalog_role", "catalog_admin"))
        if not binding:
            raise Rejected("existing catalog has no admin role")
        admin = require(view, (identity, binding[-1]))
        return Prepared((), (existing.row(), admin.row()))
    if view.get("Names", ("0", "root", "catalog", name)):
        raise Rejected("catalog name already exists")
    binding = view.get("Names", ("0", "root", "principal_role", "service_admin"))
    if not binding:
        raise Rejected("service_admin missing")
    service = require(view, ("0", binding[-1]))
    catalog = Entity("0", identity, "root", "catalog", name)
    admin = Entity(identity, "admin-" + identity, identity, "catalog_role", "catalog_admin")
    batch = Batch()
    save(batch, replace(catalog, gv=3), new=True)
    save(batch, replace(admin, gv=4), new=True)
    save(batch, replace(service, gv=service.gv + 1))
    for grant in [(*catalog.key, *admin.key, "manage_access"),
                  (*catalog.key, *admin.key, "manage_metadata"),
                  (*admin.key, *service.key, "usage")]:
        batch.put("Grants", grant)
    # Upstream's new-object return values precede helper version increments.
    return Prepared(batch.final(), (catalog.row(), admin.row()))


def change_grant(view, securable, grantee, privilege, *, revoke=False):
    # Deduplicate point loads, but increment once per participating endpoint.
    endpoints = {key: require(view, key) for key in dict.fromkeys((securable, grantee))}
    key = (*securable, *grantee, privilege)
    existing = view.get("Grants", key)
    batch = Batch()
    if bool(existing) == (not revoke):
        return Prepared((), "already in requested state")
    if revoke:
        batch.delete("Grants", key)
    else:
        batch.put("Grants", key)
    for endpoint in (securable, grantee):
        endpoints[endpoint] = replace(endpoints[endpoint], gv=endpoints[endpoint].gv + 1)
    for entity in endpoints.values():
        save(batch, entity)
    return Prepared(batch.final(), "revoked" if revoke else "granted")


def drop_leaf(view, key):
    """Bounded leaf entity and grants only: no cascade, policy, secret, or task lifecycle."""
    entity = require(view, key)
    if view.lookup("children", key, limit=1):
        raise Rejected("entity has children")
    grants = set(view.lookup("securable_grants", key))
    grants.update(view.lookup("grantee_grants", key))
    others = {tuple(row[:2]) for row in grants} | {tuple(row[2:4]) for row in grants}
    others.discard(key)
    batch = Batch()
    for other in sorted(others):
        partner = load(view, other)
        if partner:
            save(batch, replace(partner, gv=partner.gv + 1))
    for row in sorted(grants):
        batch.delete("Grants", row)
    erase(batch, entity)
    return Prepared(batch.final(), entity.row())


def create_namespace(view, scope, identity, parent, name):
    require(view, (scope, parent))
    entity = Entity(scope, identity, parent, "namespace", name)
    if load(view, entity.key) or view.get("Names", entity.name_key):
        raise Rejected("namespace id or name already exists")
    batch = Batch()
    save(batch, entity, new=True)
    return Prepared(batch.final(), entity.row())


def delete_namespace(view, key):
    entity = require(view, key)
    if view.lookup("children", key, limit=1):
        raise Rejected("namespace not empty")
    batch = Batch()
    erase(batch, entity)
    return Prepared(batch.final(), entity.row())


def resolved_read(view, key):
    entity = require(view, key)
    versions = view.get("Versions", key)
    grants = tuple(sorted(view.lookup("grantee_grants", key)))
    return Prepared((), (entity.row(), versions, grants))


class Manager:
    def __init__(self, store, realm):
        self.store, self.realm = store, realm
        self.attempts = []

    def execute(self, operation, *args, max_attempts=3, faults=None, **kwargs):
        # Only this owner replays domain logic. An adapter never invokes it.
        for number in range(max_attempts):
            attempt = self.store.begin(self.realm, **(faults or {}))
            self.attempts.append(attempt)
            try:
                prepared = operation(attempt, *args, **kwargs)
                attempt.commit(prepared.changes)
                return prepared.candidate  # Candidate is released only after confirmed commit.
            except Conflict:
                if number + 1 == max_attempts:
                    raise
            except Unknown:
                raise  # No replay, cleanup, or invented resolution.
            finally:
                attempt.close()
