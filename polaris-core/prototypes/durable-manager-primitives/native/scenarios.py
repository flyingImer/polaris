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

"""Executable acceptance experiments, with snapshots and schedules as evidence."""
from bindings import Batch, TABLES
from manager import (
    Entity, Manager, Rejected, change_grant, create_catalog, create_namespace,
    delete_namespace, drop_leaf, resolved_read, save,
)
from primitives import Capacity, Conflict, InjectedAbort, Unknown


SERVICE = Entity("0", "service-admin", "root", "principal_role", "service_admin")
SECURABLE = Entity("0", "securable", "root", "catalog", "warehouse")
ROLE = Entity("0", "role", "root", "principal_role", "analyst")
NAMESPACE = Entity("catalog", "parent", "root", "namespace", "parent")


def seed(store, realm, entities, grants=()):
    batch = Batch()
    for entity in entities:
        save(batch, entity, new=True)
    for row in grants:
        batch.put("Grants", row)
    attempt = store.begin(realm)
    try:
        attempt.commit(batch.final())
    finally:
        attempt.close()


def snapshot(store, realm):
    attempt = store.begin(realm)
    try:
        result = {kind: sorted(attempt.scan(kind)) for kind in TABLES}
        attempt.commit(())
        return result
    finally:
        attempt.close()


def expect_error(error_class, action):
    try:
        action()
    except error_class as error:
        return str(error)
    raise AssertionError("expected " + error_class.__name__)


def version_agreement(state):
    expected = sorted((r[0], r[1], r[-2], r[-1]) for r in state["Entities"])
    assert expected == state["Versions"], "entity and version tracker disagree"


def no_orphan(state):
    entities = {(r[0], r[1]) for r in state["Entities"]}
    assert ("catalog", "child") not in entities or ("catalog", "parent") in entities, "orphan child"


def traces(manager):
    return [a.summary() for a in manager.attempts]


def run_scenarios(store, backend):
    results = []

    def record(name, **evidence):
        results.append({"scenario": name, "status": "passed", **evidence})

    realm = "catalog-success"
    seed(store, realm, [SERVICE])
    manager = Manager(store, realm)
    candidate = manager.execute(create_catalog)
    state = snapshot(store, realm)
    version_agreement(state)
    assert len(state["Entities"]) == 3 and len(state["Grants"]) == 3 and len(state["Names"]) == 3
    assert {r[1]: r[-1] for r in state["Entities"]} == {"service-admin": 2, "catalog-1": 3, "admin-catalog-1": 4}
    record("catalog initialization", candidate=candidate, state=state, attempts=traces(manager))
    repeat = Manager(store, realm)
    repeated = repeat.execute(create_catalog)
    assert repeated[0][-1] == 3 and repeated[1][-1] == 4
    assert snapshot(store, realm) == state
    record("existing catalog identity", candidate=repeated, state=state, attempts=traces(repeat),
           qualification="same-ID source behavior; not a generic Unknown resolver")
    denied = Manager(store, realm)
    expect_error(Rejected, lambda: denied.execute(create_catalog, "another-id", "warehouse"))
    assert snapshot(store, realm) == state
    record("catalog name absence", attempts=traces(denied), state=state)

    for label, fault, exception in [
        ("abort after native staging", {"fail_after": 1}, InjectedAbort),
        ("capacity rejection", {"capacity": 2}, Capacity),
    ]:
        realm = label.replace(" ", "-")
        seed(store, realm, [SERVICE])
        before = snapshot(store, realm)
        manager = Manager(store, realm)
        error = expect_error(exception, lambda: manager.execute(create_catalog, faults=fault))
        after = snapshot(store, realm)
        assert before == after
        record(label, error=error, state=after, attempts=traces(manager),
               fault="deterministic adapter injection; capacity is not a measured native limit")

    realm = "grant-revoke"
    seed(store, realm, [SECURABLE, ROLE])
    manager = Manager(store, realm)
    manager.execute(change_grant, SECURABLE.key, ROLE.key, "usage")
    granted = snapshot(store, realm)
    version_agreement(granted)
    assert len(granted["Grants"]) == 1 and all(r[-1] == 2 for r in granted["Entities"])
    manager.execute(change_grant, SECURABLE.key, ROLE.key, "usage", revoke=True)
    revoked = snapshot(store, realm)
    version_agreement(revoked)
    assert not revoked["Grants"] and all(r[-1] == 3 for r in revoked["Entities"])
    record("grant and revoke with versions", granted=granted, state=revoked, attempts=traces(manager))

    realm = "grant-abort"
    seed(store, realm, [SECURABLE, ROLE])
    before = snapshot(store, realm)
    manager = Manager(store, realm)
    expect_error(InjectedAbort, lambda: manager.execute(change_grant, SECURABLE.key, ROLE.key, "usage", faults={"fail_after": 1}))
    assert snapshot(store, realm) == before
    record("grant mutation rollback", state=before, attempts=traces(manager))

    realm = "endpoint-alias"
    seed(store, realm, [ROLE])
    manager = Manager(store, realm)
    manager.execute(change_grant, ROLE.key, ROLE.key, "usage")
    state = snapshot(store, realm)
    assert state["Entities"][0][-1] == 3  # Both endpoint roles count, one final row.
    version_agreement(state)
    record("aliased grant endpoints", state=state, attempts=traces(manager),
           qualification="helper accounting edge case; does not assert a feature permits self-grants")

    realm = "drop-leaf"
    third = Entity("0", "other-role", "root", "principal_role", "other")
    grants = [(*SECURABLE.key, *ROLE.key, "usage"),
              (*SECURABLE.key, *ROLE.key, "manage_access"),
              (*ROLE.key, *third.key, "usage")]
    seed(store, realm, [SECURABLE, ROLE, third], grants)
    before = snapshot(store, realm)
    aborted = Manager(store, realm)
    expect_error(InjectedAbort, lambda: aborted.execute(drop_leaf, ROLE.key, faults={"fail_after": 3}))
    assert snapshot(store, realm) == before
    manager = Manager(store, realm)
    manager.execute(drop_leaf, ROLE.key)
    state = snapshot(store, realm)
    version_agreement(state)
    assert len(state["Entities"]) == 2 and not state["Grants"] and len(state["Names"]) == 2
    assert all(r[-1] == 2 for r in state["Entities"])  # Counterpart once, not once per grant.
    record("bounded leaf drop and rollback", state=state, attempts=traces(manager), abort_attempts=traces(aborted))

    realm = "namespace-race"
    seed(store, realm, [NAMESPACE])
    deletion = store.begin(realm)
    child = Manager(store, realm)
    schedule = ["delete reads parent and empty children range"]
    try:
        prepared = delete_namespace(deletion, NAMESPACE.key)
        try:
            child.execute(create_namespace, "catalog", "child", "parent", "child", max_attempts=1)
            schedule.append("child commits")
        except Conflict:
            # The emulator serializes transactions by locking the entire database.
            schedule.append("child attempt aborted while delete is active")
            deletion.commit(prepared.changes)
            schedule.append("delete commits")
            expect_error(Rejected, lambda: child.execute(create_namespace, "catalog", "child", "parent", "child"))
            schedule.append("fresh child attempt rejects missing parent")
        else:
            expect_error(Conflict, lambda: deletion.commit(prepared.changes))
            schedule.append("delete aborts on dependency conflict")
            fresh = Manager(store, realm)
            expect_error(Rejected, lambda: fresh.execute(delete_namespace, NAMESPACE.key))
            schedule.append("fresh delete attempt rejects nonempty namespace")
    finally:
        deletion.close()
    state = snapshot(store, realm)
    no_orphan(state)
    record("namespace child-create versus delete", schedule=schedule, state=state,
           delete_attempt=deletion.summary(), child_attempts=traces(child),
           qualification="emulator whole-database concurrency differs from production Spanner" if backend == "spanner" else "native serializable transaction")

    realm = "retry-unchanged-parent"
    seed(store, realm, [NAMESPACE])
    manager = Manager(store, realm)
    concurrent = {"ran": False, "deleted": False}

    def interrupted_create(view):
        prepared = create_namespace(view, "catalog", "child", "parent", "child")
        if not concurrent["ran"]:
            concurrent["ran"] = True
            try:
                Manager(store, realm).execute(delete_namespace, NAMESPACE.key, max_attempts=1)
                concurrent["deleted"] = True
            except Conflict:
                pass  # A locking backend may select the other transaction as victim.
        return prepared

    try:
        manager.execute(interrupted_create)
        assert not concurrent["deleted"]
    except Rejected:
        assert concurrent["deleted"] and len(manager.attempts) == 2
    state = snapshot(store, realm)
    no_orphan(state)
    record("Manager retry reloads unchanged parent", state=state, attempts=traces(manager),
           competing_delete_committed=concurrent["deleted"],
           automatic_retry_observed=len(manager.attempts) > 1)

    realm = "unknown"
    seed(store, realm, [SERVICE])
    manager = Manager(store, realm)
    error = expect_error(Unknown, lambda: manager.execute(create_catalog, faults={"lose_ack": True}))
    assert len(manager.attempts) == 1 and manager.attempts[0].state == "UNKNOWN"
    state = snapshot(store, realm)
    assert len(state["Entities"]) == 3 and len(state["Grants"]) == 3
    record("lost acknowledgement does not replay", state=state, attempts=traces(manager), error=error,
           fault="synthetic suppression after native commit success; not an actual transport failure")

    realm = "resolved-read"
    seed(store, realm, [SECURABLE, ROLE])
    manager = Manager(store, realm)
    manager.execute(change_grant, SECURABLE.key, ROLE.key, "usage")
    result = manager.execute(resolved_read, ROLE.key)
    assert result[0][-2:] == result[1][-2:] and len(result[2]) == 1
    record("one composed read view", result=result, attempts=traces(manager),
           qualification="one scoped read; this case does not exhaustively test concurrent snapshots")

    realm = "overlapping-read"
    seed(store, realm, [SECURABLE, ROLE])
    reader = store.begin(realm)
    writer = Manager(store, realm)
    try:
        original = reader.get("Entities", ROLE.key)
        try:
            writer.execute(change_grant, SECURABLE.key, ROLE.key, "usage", max_attempts=1)
        except Conflict:
            writer_outcome = "aborted"
        else:
            writer_outcome = "committed"
        try:
            version = reader.get("Versions", ROLE.key)
            grants = reader.lookup("grantee_grants", ROLE.key)
            assert original[-2:] == version[-2:]
            assert len(grants) == 0  # Same before-grant read view, never mixed observations.
            reader.commit(())
        except Conflict:
            pass  # Fresh composed read below must establish its own coherent view.
    finally:
        reader.close()
    fresh = Manager(store, realm).execute(resolved_read, ROLE.key)
    assert fresh[0][-2:] == fresh[1][-2:]
    assert len(fresh[2]) == (1 if writer_outcome == "committed" else 0)
    record("composed read overlapping grant publication", first_read_attempt=reader.summary(),
           writer_outcome=writer_outcome, fresh_result=fresh)

    if backend == "fdb":
        realm = "negative-control"
        seed(store, realm, [NAMESPACE])
        deletion = store.begin(realm, unsafe_empty=True)
        try:
            prepared = delete_namespace(deletion, NAMESPACE.key)
            Manager(store, realm).execute(create_namespace, "catalog", "child", "parent", "child")
            deletion.commit(prepared.changes)
        finally:
            deletion.close()
        state = snapshot(store, realm)
        expect_error(AssertionError, lambda: no_orphan(state))
        record("negative control: unprotected FDB range", state=state,
               observation="orphan reproduced as expected; deliberately invalid adapter mode")
    return results
