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
| Operation semantics | `TransactionalMetaStoreManagerImpl` in `polaris-core` | Reused; CreateCatalog now computes final grants/versions before writes |
| Shared domain mapping and credential rules | `domain/RecordTransactionalPersistence`, `domain/DomainRecords` | Reuses `AbstractTransactionalPersistence`; extracts the existing domain algorithms into one implementation for all new adapters |
| Opaque transaction and storage contract | `api/DurablePrimitives`, `api/StorageFailure` | Experimental narrow backend replacement boundary |
| Native mechanics | `jdbc/JdbcPrimitives`, `fdb/FdbPrimitives`, `spanner/SpannerPrimitives` | No Polaris entity, grant, policy or secret types imported |
| Runtime selection / realm bootstrap | `PrimitiveMetaStoreManagerFactory` | Existing `MetaStoreManagerFactory` and `LocalPolarisMetaStoreManagerFactory` contracts |
| Verification | `H2ManagerTest`, `NativeManagerTest` | Inherit the 28 upstream `BasePolarisMetaStoreManagerTest` tests |

The shared domain classes belong to the Manager implementation. They are not an
Orchestrator SPI and do not coordinate heterogeneous databases. Legacy TreeMap,
JDBC and NoSQL implementations are not migrated by this prototype. Their existing
shared logic is acknowledged; this branch does not claim it did not exist.

Why introduce a narrow interface? The current transactional persistence extension
surface includes secret rotation/reset, typed grant operations and other domain
behavior. The new adapters implement point reads, protected ranges, native writes
and commit outcome translation. Adding FDB or Spanner therefore does not require
another implementation of those business rules. The existing typed persistence
interface remains a compatibility surface above that narrower boundary.

## Two explicitly different execution paths

**Selected terminal-batch path:** `CreateCatalog` still enters the upstream
Manager. Its reads resolve name absence and current recipient roles; its last
read is ID allocation. Shared Java code computes the complete catalog, admin role,
initial grants and endpoint grant versions. The compatibility bridge then calls
`Attempt.commit(mutations)` on the **same attempt** used for reads.

`runInFinalBatchTransaction` is a small default hook on the existing
`TransactionalPersistence`; old implementations keep their current transaction
implementation. The new bridge rejects reads after the first staged write. It
has no read-your-writes query overlay. Spanner can use buffered mutations; FDB
uses sets/clears, JDBC executes its changes before the one native commit.

**Migration path:** other upstream Manager helpers still write and then read.
They explicitly use `beginLegacy()` and native read-your-writes: Spanner DML,
FDB native transaction state, and JDBC statements. This capability has a default
unsupported implementation and is not required by the terminal-batch interface.
A backend lacking it cannot serve the unmigrated workflows through this bridge.

This distinction is deliberate. The prototype does not silently give `write()`
different visibility across databases, and does not pretend that every operation
has already been converted to final-batch planning.

## Contract and implementation limits

- One homogeneous transaction scope per atomic operation. Internal versions are
  allowed; domain entity/grant versions remain for existing Polaris consumers.
- Serializable point, absence and range observations compose with writes in one
  attempt. Reads alone do not promise that each observed value remains unchanged
  until physical commit. Backend algorithms may differ.
- Only explicit commit publishes. Close aborts; successful helper results do not
  escape the shared transaction wrapper until native commit returns.
- No adapter replays caller code. A confirmed native conflict is translated to
  the existing `RetryOnConcurrencyException` at the compatibility boundary.
  Rejection and unknown stay distinct. A lost commit response is never converted
  to a conflict or automatically replayed.
- The ID reservation helper retries only confirmed-aborted ID reservations with
  a bounded loop. General operation retry and authorization ownership are not
  completed by this PoC. The Spanner parallel fixture results expose this gap.
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
- External storage integrations, STS effects, event persistence, caller caches,
  full HTTP error mapping and all maintenance writers are not certified here.
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
initialize flag creates the named instance/database and generic record table;
use it only for a fresh disposable emulator.

```sh
./gradlew :polaris-persistence-primitives:test \
  --tests '*NativeManagerTest' --tests '*ManagerFailureTest' --tests '*AttemptContractTest' \
  -Dpoc.backend=spanner \
  -Dpoc.spanner.endpoint=127.0.0.1:9010 \
  -Dpoc.spanner.database=projects/polaris-poc/instances/java-poc/databases/metadata \
  -Dpoc.spanner.initialize=true
```

The unfiltered Spanner suite remains the default and its failures are recorded.
For the separately reported **serial workflow** diagnostic, add
`-Dpoc.spanner.serial-fixtures=true`; this explicitly excludes the two inherited
parallel task tests. The cross-transaction empty-range race is also explicitly
skipped on the emulator because its database-wide locking cannot execute that
schedule. This is not a way to certify production concurrency.

For opt-in runtime wiring, select `polaris.persistence.type=durable-primitives-poc`
and provide the corresponding `poc.*` Java system properties. Use the ordinary
Polaris realm bootstrap entry point after initializing the experimental schema.
The service module includes the adapter through the existing runtime dependency
mechanism; other persistence selections are unchanged.

## Evidence to review

See [VALIDATION.md](VALIDATION.md) and the machine-readable run summaries in
`validation/`. Counts distinguish executed, failed and skipped tests. No Python
probe result is included as evidence for this Java branch.
