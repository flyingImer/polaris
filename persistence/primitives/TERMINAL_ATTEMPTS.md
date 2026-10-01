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

# Terminal-attempt prototype: decisions and coverage

This is an implementation checkpoint for `prototype/durable-java-primitives`,
continuing commit `0e0cccd577122544622068e74bcf80f6f673804d` on the pinned upstream
base `a919f11e51ff76b5ef56633d8cb0429ff2b4c100`. It records what the Java prototype
implements. The test outcomes and repository gates are in [VALIDATION.md](VALIDATION.md).
It is not a production migration or an assertion that every concern in the mailing
list threads has been closed.

## Accepted requirements and implementation

| Requirement | Implementation | Boundary |
|---|---|---|
| Homogeneous atomic scope. Internal backend versions allowed | One opaque native attempt per atomic domain operation | No cross-database Orchestrator or compensation protocol |
| Share domain semantics across databases | Existing transactional Manager plus shared `domain/` mapping/rules | FDB, JDBC and Spanner adapters import no Polaris entity, grant, policy or secret classes |
| Serializable writes include necessary reads | `Attempt.get/getMany/scan` and terminal `commit` use the same native context | No detached observation-token portability requirement. No promise that every read is physically locked until commit |
| Read/check then final mutations | All opt-in persistence callbacks use a strict final batch. Staged writes prohibit subsequent reads | No legacy read-your-writes mode, native DML fallback, or generic query overlay |
| One consistent logical read | `ReadView` supplies one snapshot. Spanner uses its native read-only transaction | Independent Manager calls and pages do not share a snapshot. Read-only views cannot be promoted to write attempts |
| Batch reads and writes | Native `getMany`. JDBC statement batches, FDB parallel read futures, Spanner KeySet/buffered mutations | Chunking stays in one transaction. Full drop/location scans and the shared ID counter remain costs |
| Retry only reconstructable operations | One bounded retry owner. Confirmed-abort marker distinct from stale caller expectations | Pure reads, ID allocation and tasks retry in shared domain code. Selected admin Features rebuild authorization. External-effect flows are not blanket-replayed |
| Already-authorized attempt may finish | Current attempt does not require grant versions to remain unchanged until commit | A new Feature attempt resolves and authorizes again. This is not commit-time authorization validity |
| Preserve uncertainty | `CommitOutcomeUnknownException` reaches callers. Iceberg translates it to `CommitStateUnknownException` | No generic receipt/exactly-once layer. Latest entity state alone is not proof about an earlier uncertain attempt |
| Atomic local credential reset | Principal/client-ID update and secret replacement share one commit | Temporary optional Manager hook preserves unmigrated implementations without claiming their reset is atomic |
| Atomic work may reject when too large | No adapter silently splits a logical commit. Definite rejection differs from unknown | No arbitrary-size deletion promise or new public multi-stage-delete protocol |

The retry policy currently permits at most 32 attempts and two seconds for
**starting** retries, with jitter and one outermost owner. It does not interrupt
an in-flight driver call. The earlier eight-attempt cap exhausted under the
Spanner emulator's database-wide writer contention. These are PoC policy values,
not workload-independent liveness or latency guarantees.

## The original community problems

The source problems remain the starting point:

- [T1: partial multi-object operations](https://lists.apache.org/thread/l7b4py5vl0x9rbn6soy4k9qxchmpmgrd).
- [T2: consistent multi-object changes](https://lists.apache.org/thread/vx0k8ow4k87m4y7cxpmojb0zy17t5ldy).
- [T3: persistence guarantees and publication](https://lists.apache.org/thread/rf5orxs815zs4h64p4rwp03q3pbgxb5r).

| Actual concern | What closes the selected path | Executable evidence | Remaining scope |
|---|---|---|---|
| Grant record and endpoint versions diverge | Read endpoint state before staging. Publish grant/index/version changes together | Inherited grant fixtures. Rejected revoke preserves a complete pre-attempt snapshot | Existing adapters and out-of-band writers are not migrated |
| Catalog creation stops partway | Catalog, admin role, grants and recipient versions form one final batch | Inherited catalog fixtures. Rejection leaves no changes. Response-lost reports UNKNOWN without replay | External integration/service-identity lifecycle is separate |
| Drop stops during cleanup | Read grants/policy mappings and surviving endpoints, then publish all deletions and version updates together | Inherited deletion fixtures. Injected cleanup failure leaves the entire snapshot unchanged | Large-set loading/capacity and production reclamation remain onboarding work |
| Validation uses unchanged objects | Manager revalidates caller-observed ancestor entity versions inside the publishing attempt | Stale unmodified ancestor rejects child creation with no writes | Only supplied observations are covered. The SPI cannot infer an arbitrary Feature precheck |
| Empty namespace becomes nonempty during deletion | Parent existence and child range belong to opposing protected attempts | Actual Manager create/delete race plus primitive race on FDB and PostgreSQL | Emulator cannot execute the simultaneous read/write rendezvous |
| Location range changes after early validation | Shared Manager rereads catalog/sibling range and current values. Validates the proposed batch against itself | Overlapping batch and sequential rejection tests. Concurrent sibling-location race on FDB and PostgreSQL | Optional optimized global lookup remains unsupported. Full scans are not a tuned schema |
| Composed entity/grant observation is inconsistent | Existing resolved-read workflows share a read view. Batch fetches preserve the same context | Inherited resolved-read fixtures. Snapshot retained across another commit on FDB, PostgreSQL and Spanner emulator | Arbitrary Feature call sequences, caller caches and multiple pages are not one snapshot |
| Conflict reaches the wrong retry boundary | Only confirmed abort enters bounded replay. Feature retry creates a fresh manifest and authorizes again | Admin Service tests with a mock authorizer: repeated check, deny stops second submission, stale/UNKNOWN are not replayed | No automatic replay added to multi-phase synthetic-entity or external-effect flows |
| Helper success is confused with durable publication | Native commit precedes return from the shared wrapper. Multi-table Feature workspace still has explicit final publication | Real Manager failures and actual Service calls using the PoC persistence | Existing workspace staging remains staging, not an independent durable success |
| Failure cleanup assumes abort after response loss | Table, view, multi-table and external-catalog callers retain potentially live metadata/secrets on UNKNOWN | Service tests inject uncertainty both before and after actual publication. Confirmed failures clean new multi-table files | No recovery receipt or automatic uncertainty resolver. Production failure injection still needed |

## What the interface changes actually bought

The new adapters implement storage behavior, not catalog creation or credential
rules. The typed `TransactionalPersistence` surface remains temporarily above
them as a bridge used by the existing Manager. This establishes a narrower
backend replacement boundary without replacing the Feature or domain Manager SPI.
Whether that boundary ultimately keeps the experimental name `DurablePrimitives`
or evolves an existing interface remains a publication/migration decision.

Removing read-your-writes exposed real shared-workflow changes: grant endpoints
must be read before mutation, catalog/admin deletion must be planned as a group,
bulk operations must validate before any writes, and task claiming must compute
versions from its protected read. ID allocation keeps a prospective counter in
domain planning state. That specific state is not a general record overlay.

The refactor exposed two traps in intermediate PoC code: batch name tracking
initially used a key without value equality, and an attempted list optimization
used the name-index payload as current entity state. Property-only updates do
not refresh that payload. The final shared implementation uses a value key for
planned names and batch-fetches current entities after reading membership.
Tests protect both rules across the adapters.

The full Service check caught a compatibility regression in the initial view
mapping: putting a view's metadata location into the public `baseLocation` field
also narrowed its permitted metadata-write paths. Views now pass that location as
an internal metadata fact to shared overlap validation. This retains the existing
storage-root semantics while protecting the publishing attempt. An actual Service
allowed-location test and a Manager test cover the two responsibilities separately.

The first Spanner run also exposed that a read-only Manager call was using a
read/write transaction. `ReadView` now gives the adapter enough information to
choose its native read-only path. This is a concrete reason for the additional
primitive capability. It does not add domain knowledge to the adapter.

## Writer and integration audit

The opt-in factory constructs one shared `RecordTransactionalPersistence` for
all new adapters. Its raw record writes go through `DomainRecords` and the strict
attempt scope. Grant/name/version/policy indexes are maintained there. Task
creation/claiming, bootstrap, realm clearing and credential operations use this
same path. Native adapters expose no domain-specific write entry points.

The source inventory distinguishes the currently connected writers:

| Entry path | Where it ends |
|---|---|
| Runtime metadata operations | Factory-selected transactional Manager and shared record persistence. No direct primitive imports in runtime main sources |
| ID reservation, tasks, bootstrap and realm clearing | The same shared transaction scope and record layout |
| Event append through `PolarisEventManager` | Inherited `writeEvents` explicitly throws `UnsupportedOperationException`. Event persistence is not implemented by this PoC |
| Existing NoSQL maintenance | Its existing NoSQL `Backend` and layout. It is not a writer to the new primitive record layout |

Only the primitive module's factory, shared domain classes and native adapters
reference the experimental storage API or physical record tables in main code.
This is a source-path inventory plus the inherited Manager fixtures, not a
certificate for every production maintenance job. Legacy JDBC/NoSQL paths,
external writers, data migration, external event delivery, production Spanner
credentials and performance tuning are outside the prototype. A maintenance
writer migrating to the new layout must preserve the shared index and validation
protocol. Bypassing it is not supported by the contract.

The generic JSON-record layout is intentionally separate from current tables.
Full listings, per-entity domain planning, global ID allocation contention and
extra early Feature checks remain optimization opportunities. Batching reduces
per-key calls but does not by itself prove native RPC parity or production speed.

## Completion boundary

The selected Java workflows and caller outcomes can now be reviewed against a
single terminal-attempt model. PostgreSQL now passes all 47 native tests on
commit `641cd2cda997eeb6dea2095a28c00715881be1d9`, including the simultaneous races.
Clean CI also passes formatting, compilation and all three touched-module checks,
including Service integration tests. Cloud tests remain skipped.

The PoC implementation and selected native evidence are now recorded on the branch.
A read-only audit found that the passing Service integration gate includes five
existing CockroachDB suites, with 481 passed and 11 skipped. They use the legacy
`relational-jdbc` implementation, not the new Primitives adapter. The earlier
report that CI did not start CockroachDB was incorrect. See
[VALIDATION.md](VALIDATION.md) for the corrected execution and permission boundary.

Following explicit authorization for isolated execution, the current Primitives
adapter passes all 47 native tests on CockroachDB v26.3.1. PostgreSQL also passes
all 47 again on the same source. The database ran on an internal Docker network,
with verified metadata deny rules on the runner. No Java change was needed.
See the isolated native reports linked from [VALIDATION.md](VALIDATION.md).

The repaired repository-wide gate passed on `90af530` in run 36632123504. The
later corrected request-validation source `725d7b8` also passed format/compile,
clean formatting, BOM/license checks and the full repository `check` in run
36785806874. All 15 selected Service cases passed, including the two PostgreSQL
request races with no skips, and the current Primitives adapter passed 47/0/0 on
CockroachDB.

That workflow still concluded `failure`: its post-Service XML verifier searched
parameterized case names for the Java method name. It stopped before the
corrected-source 47-case PostgreSQL Primitives suite. The earlier PostgreSQL
47/0/0 result remains historical evidence, not a rerun of the repaired
worker-result tests. Exact artifacts and counts are recorded in
[VALIDATION.md](VALIDATION.md). Production Spanner concurrency, performance,
data migration and exhaustive community-requirement coverage remain unverified.
