<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied. See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Java Manager / Primitives integration prototype

Base: Apache Polaris `main` at `a919f11e51ff76b5ef56633d8cb0429ff2b4c100`.
Branch: `prototype/durable-java-primitives` on `flyingImer/polaris`.

This experiment runs the actual Polaris Java `TransactionalMetaStoreManagerImpl`
and its inherited upstream test fixtures against native storage adapters. It is
an opt-in prototype, not a replacement for the production persistence backends.
There is no translated Python business workflow in this branch.

## Questions this branch answers

1. Can a new database reuse Polaris operation semantics without implementing
   catalog, role, grant, policy and credential workflows itself?
2. Can protected reads and terminal writes keep the same native transaction,
   including empty ranges and unchanged objects?
3. What changes are needed in the real upstream call chain, and which remaining
   caller requirements are not solved by a storage abstraction?

The motivating community problems remain:

- [T1: partial multi-object commits](https://lists.apache.org/thread/l7b4py5vl0x9rbn6soy4k9qxchmpmgrd):
  grant/version mismatch, incomplete catalog initialization and interrupted drop.
- [T2: consistent multi-object changes](https://lists.apache.org/thread/vx0k8ow4k87m4y7cxpmojb0zy17t5ldy):
  validation dependencies, independent RBAC, composed reads and retry behavior.
- [T3: historical persistence contract](https://lists.apache.org/thread/rf5orxs815zs4h64p4rwp03q3pbgxb5r):
  backend-neutral guarantees and the difference between staging and publication.

This branch validates concrete subsets of these problems. It does **not** claim
that adding a transaction under the existing Manager fixes every caller-side
validation, authorization, pagination or external-effect problem in those threads.

## Actual Java wiring

| Responsibility | Code | Reused / changed |
|---|---|---|
| Operation semantics | `TransactionalMetaStoreManagerImpl` in `polaris-core` | Reused. Shared workflows now separate protected reads from final mutations |
| Shared domain mapping and credential rules | `domain/RecordTransactionalPersistence`, `domain/DomainRecords` | Reuses `AbstractTransactionalPersistence` and extracts the existing domain algorithms into one implementation for all new adapters |
| Opaque transaction and storage contract | `api/DurablePrimitives`, `api/StorageFailure` | Experimental narrow backend replacement boundary |
| Native mechanics | `jdbc/JdbcPrimitives`, `fdb/FdbPrimitives`, `spanner/SpannerPrimitives` | No Polaris entity, grant, policy or secret types imported |
| Runtime selection / realm bootstrap | `PrimitiveMetaStoreManagerFactory` | Existing `MetaStoreManagerFactory` and `LocalPolarisMetaStoreManagerFactory` contracts |
| Verification | `H2ManagerTest`, `NativeManagerTest` | Inherit the 28 upstream `BasePolarisMetaStoreManagerTest` tests |

The shared domain classes belong to the Manager implementation. They are not an
Orchestrator SPI and do not coordinate heterogeneous databases. Legacy TreeMap,
JDBC and NoSQL implementations are not migrated by this prototype. These existing paths already share substantial domain logic.

Why introduce a narrow interface? The current transactional persistence extension
surface includes secret rotation/reset, typed grant operations and other domain
behavior. The new adapters implement point reads, protected ranges, native writes
and commit outcome translation. Adding FDB or Spanner therefore does not require
another implementation of those business rules. The existing typed persistence
interface remains a compatibility surface above that narrower boundary.

## One terminal-attempt path

Every write workflow on the opt-in adapter now uses `begin()` and terminal
`Attempt.commit(mutations)`. There is no `beginLegacy()`, DML compatibility path,
or read-your-writes query overlay. The shared bridge rejects reads after staging
the first mutation. Grant/revoke, grouped catalog drop, entity batches, root
backfill and task claiming compute their final domain state before writing.

Reads keep the same native transaction through commit. The allocator holds its
prospective counter in domain planning state and appends one final counter write.
This is not a general record or query overlay. Independent ID reservations are
separate operations and may leave unused IDs after a later operation aborts.

`getMany` preserves input order, duplicates and missing-key observations. FDB
starts independent native reads together. Spanner uses a KeySet. JDBC chunks IN
queries on one connection. JDBC also batches adjacent statements of the same
shape, preserving the order of overlapping mutations. A write chunk is never a
separate commit. Spanner uses buffered mutations after all reads.

Pure composed reads use `readView()`. Spanner maps this to a native read-only
transaction. FDB and JDBC retain a snapshot in an ordinary transaction that is
closed without writes. A read-only view cannot later be attached to a write.
The shared domain bridge rejects mutations in read-only callbacks and retries
only confirmed transient conflicts for these side-effect-free logical reads.

Shared Manager validation now protects caller-observed ancestor entity versions
and repeats catalog/sibling location predicates within the publishing attempt.
It also compares all proposed entities in a batch, so two overlapping creations
in one batch cannot evade the range check. This implements the existing sibling
rule. The optional optimized global overlap lookup remains unsupported. The PoC
uses protected full listings and batched current-entity fetches, not a tuned
location index. Early Feature checks remain for feedback, not commit protection.
Views carry their location in internal metadata for this validation. The mapping
does not change their public storage-root restrictions.

## Operation outcome and retry ownership

Storage reports CONFLICT, REJECTED or UNKNOWN. The shared domain bridge translates
confirmed abort to `ConfirmedTransactionConflictException`, definite rejection to
`CommitRejectedException`, and uncertainty to `CommitOutcomeUnknownException`. An ordinary stale version exception is not a
license to replay. There is no generic receipt or exactly-once layer.

`ConfirmedConflictRetry` permits at most 32 attempts, with a two-second budget
for starting retries and interruptible backoff. It is not a deadline for an
in-flight driver call. Its outermost caller owns the budget. Nested uses execute
once. Manager-owned write retry is limited to ID reservations and task creation/claiming.
Pure composed reads also use this shared bounded policy.
Selected single-operation admin workflows own retry at Feature level, rebuilding
resolution and authorization each time. A previously authorized attempt may finish.
A fresh retry must authorize again. Multi-phase synthetic-entity workflows are
not automatically replayed.

Atomic local credential reset is exposed through a temporary compatibility hook
on the existing Manager. Supported implementations return principal and secrets
from one commit. An empty result explicitly means unsupported and no effects.
Legacy Feature behavior remains outside the replay loop. The hook does not claim
that old JDBC or NoSQL reset behavior has become atomic.

Catalog creation retains external secret references on UNKNOWN. Iceberg table,
view and multi-table publication translate uncertainty to
`CommitStateUnknownException` and retain potentially live metadata files. There
is no automatic replay or abort-assuming cleanup after that result. Confirmed
abort/rejection and stale-validation failures clean newly written multi-table
metadata. They never clean files as if an UNKNOWN were an abort.

## Contract and implementation limits

- One homogeneous transaction scope per atomic operation. Internal versions are
  allowed. Domain entity/grant versions remain for existing Polaris consumers.
- Serializable point, absence and range observations compose with writes in one
  attempt. Reads alone do not promise that each observed value remains unchanged
  until physical commit. Backend algorithms may differ.
- Only explicit commit publishes. Close aborts. Successful helper results do not
  escape the shared transaction wrapper until native commit returns.
- No adapter replays caller code. A confirmed native conflict is translated to
  `ConfirmedTransactionConflictException` at the compatibility boundary.
  Rejection and unknown stay distinct. A lost commit response is never converted
  to a conflict or automatically replayed.
- Atomic work is never silently split. Oversize/backend errors abort or remain
  unknown according to the evidence. There is no arbitrary-size drop promise.
- Protected empty-range checks use a bounded native query. Other inherited list
  and drop helpers still load potentially large sets. The common layout and
  adapter implementations are not a performance-optimized production schema.
- The shared counter is a contention point. This branch does not establish
  production throughput, RPC parity, or superiority over a tuned native schema.
- The JSON record layout is experimental and separate from existing JDBC/NoSQL
  tables. No data migration or schema compatibility is supplied. Secrets persist
  verification hashes rather than returned plaintext credentials.
- Event persistence is unsupported: the inherited `writeEvents` method rejects
  it explicitly. External storage integrations, STS effects, caller caches, full
  HTTP error mapping and production maintenance integration are not certified here.
- Spanner currently accepts only an explicitly configured local emulator, with
  explicit project and no ambient credential discovery. Production credentials,
  IAM, driver tuning and a production isolation audit remain onboarding work.
- Factory wiring is opt-in. Successful Manager tests are not an HTTP server
  end-to-end certificate or a claim that an upstream PR is merge-ready.

## Reproduce with upstream Gradle

Use the upstream toolchain prerequisites (JDK 21 and the Gradle wrapper). These
commands run Java code in the regular multi-project build. Native endpoints and
schema/bootstrap state must be disposable and explicitly configured.

Local Java wiring smoke test, including inherited Manager fixtures:

```sh
./gradlew :polaris-persistence-primitives:test \
  --tests '*H2ManagerTest' --tests '*ManagerFailureTest' --tests '*AttemptContractTest'
```

H2 is not evidence for PostgreSQL serializable range behavior.

PostgreSQL, using a disposable database and the PostgreSQL JDBC driver:

```sh
./gradlew :polaris-persistence-primitives:test \
  --tests '*NativeManagerTest' --tests '*ManagerFailureTest' --tests '*AttemptContractTest' \
  -Dpoc.backend=jdbc \
  -Dpoc.jdbc.url='jdbc:postgresql://127.0.0.1:5432/polaris_poc' \
  -Dpoc.jdbc.user=polaris_poc
```

Supply the password separately via the local test configuration when necessary.
The test harness explicitly initializes only `polaris_poc_records`. Production
runtime requests do not execute schema DDL.

CockroachDB uses the **same adapter and Java domain implementation**:

```sh
./gradlew :polaris-persistence-primitives:test \
  --tests '*NativeManagerTest' --tests '*ManagerFailureTest' --tests '*AttemptContractTest' \
  -Dpoc.backend=jdbc \
  -Dpoc.jdbc.url='jdbc:postgresql://127.0.0.1:26257/defaultdb?sslmode=disable' \
  -Dpoc.jdbc.user=root
```

FoundationDB: install the matching 7.3 client library, start/configure a disposable
server, and provide its cluster file. Make `libfdb_c.so` available to the Java
process through the normal native-library search path.

```sh
./gradlew :polaris-persistence-primitives:test \
  --tests '*NativeManagerTest' --tests '*ManagerFailureTest' --tests '*AttemptContractTest' \
  -Dpoc.backend=fdb -Dpoc.fdb.cluster=/absolute/path/to/fdb.cluster
```

Spanner: start a local emulator with a fresh instance namespace. The explicit
initialize flag creates the named instance/database and generic record table.
Use it only for a fresh disposable emulator.

```sh
./gradlew :polaris-persistence-primitives:test \
  --tests '*NativeManagerTest' --tests '*ManagerFailureTest' --tests '*AttemptContractTest' \
  -Dpoc.backend=spanner \
  -Dpoc.spanner.endpoint=127.0.0.1:9010 \
  -Dpoc.spanner.database=projects/polaris-poc/instances/java-poc/databases/metadata \
  -Dpoc.spanner.initialize=true
```

The unfiltered Spanner suite remains the default. The current run includes both
inherited parallel task tests. Older failed runs are retained as evidence.
For the separately reported **serial workflow** diagnostic, add
`-Dpoc.spanner.serial-fixtures=true`. This explicitly excludes the two inherited
parallel task tests. Three simultaneous read/write race schedules are explicitly
skipped on the emulator because its database-wide locking cannot execute that
schedule. This is not a way to certify production concurrency.

For opt-in runtime wiring, select `polaris.persistence.type=durable-primitives-poc`
and provide the corresponding `poc.*` Java system properties. Use the ordinary
Polaris realm bootstrap entry point after initializing the experimental schema.
The service module includes the adapter through the existing runtime dependency
mechanism. Other persistence selections are unchanged.

## Evidence to review

See [VALIDATION.md](VALIDATION.md) and the machine-readable run summaries in
`validation/`. Counts distinguish executed, failed and skipped tests. No Python
probe result is included as evidence for this Java branch.
