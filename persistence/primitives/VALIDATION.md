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

# Java prototype validation

Base: `apache/polaris main @ a919f11e51ff76b5ef56633d8cb0429ff2b4c100`.
Executed on 2026-09-23 with JDK 21 and the upstream Gradle 9.7.1 wrapper.

## Recorded runs

| Backend / gate | Passed | Failed | Skipped / excluded | Interpretation |
|---|---:|---:|---|---|
| FDB 7.3.77, final native Java run | 35 | 0 | None | 28 unchanged upstream Manager fixtures, 4 injected-failure tests, 3 primitive contract tests |
| CockroachDB 25.4.0, final native Java run | 35 | 0 | None | Same Manager and mapping code; generic PostgreSQL JDBC adapter |
| Spanner emulator 1.5.57, serial workflow run | 32 | 0 | 1 race skipped; 2 inherited parallel tests explicitly excluded | 26 upstream Manager fixtures, 4 fault tests, 2 primitive checks |
| Spanner emulator, initial unfiltered run | 28 | 5 | 1 race skipped | Preserved failure evidence; see explanation below |
| `polaris-core:check` | 1,001 | 0 | 16 tests skipped by upstream | Core tests, checkstyle and formatting checks passed |
| `polaris-persistence-primitives:check`, default local configuration | 34 | 0 | 28 native fixtures unconfigured; 1 H2 race skipped | H2 Java wiring and fault tests; not native isolation proof |
| PostgreSQL server | — | — | Not executed | Adapter compiled; a CockroachDB pass is not a PostgreSQL pass |

The Spanner client is pinned to 6.120.0 with explicit serializable isolation.
The emulator is connected with an explicit local endpoint and project, without
ambient credential discovery. The FDB Java client is pinned to 7.3.77.

The unfiltered Spanner run failed its two parallel task tests. Creation exposed
a confirmed abort reaching a caller without a complete operation retry loop;
concurrent task claiming exceeded the upstream fixture's 30-second deadline.
Workers still active after that deadline interfered with three later setup
transactions. The separate serial run used a fresh emulator and an explicitly
named test filter. It does not erase the unfiltered result, establish production
performance, or certify Spanner concurrency. The initial run predates the final
idempotent close guard and additional drop-failure test; it remains initial-run
evidence, not a claim that every final-code failure was reproduced unchanged.

PostgreSQL was not executed in this workspace: no PostgreSQL service or Docker
runtime was available, and the workspace runs as root. The branch supplies the
same JDBC adapter and an explicit PostgreSQL test command. No PostgreSQL result
is inferred from H2 or CockroachDB.

Machine-readable native summaries are in `validation/`. They contain test names
and results, not credentials, raw record contents or noisy server logs. Gradle can
retain XML from earlier test filters; summaries for a native run select only the
native fixture class plus the explicitly selected fault/contract classes.

## What was actually exercised

- Real `TransactionalMetaStoreManagerImpl`, `PolarisCallContext`, domain entities,
  grant/version logic, `AbstractTransactionalPersistence`, and upstream fixtures.
- One shared mapping/rule implementation, backed by actual native Java clients.
- CreateCatalog's terminal batch: the bridge throws on any read after staging a
  write, so passing native Manager catalog fixtures also checks this phase rule.
- Catalog commit rejection: no metadata changes become visible.
- Revoke failure after native writes: grant indexes and endpoint versions remain
  equal to the pre-attempt snapshot.
- Drop failure during cleanup: metadata and related versions remain unchanged.
- Commit success followed by injected response loss: Manager reports UNKNOWN,
  does not replay, and subsequent independent reads observe the committed data.
- FDB and CockroachDB race: deletion observes an empty child range while creation
  observes the parent. They cannot both commit and leave an orphan child.
- Close aborts native staged changes; terminal batches combine updates/deletes.
- Confirmed conflicts map to the exception existing callers recognize. Explicit
  rollback followed by a business failure result does not accidentally commit.

Fault injection is around real native attempts. It verifies Java propagation
and cleanup behavior, not every possible server/driver network failure class.

## Coverage against the community problems

| Original concern | Evidence here | Still outside this branch's claim |
|---|---|---|
| T1: partial catalog initialization | Real Manager catalog fixtures and rejection/response-lost injection | External storage integration effects |
| T1: grant/version mismatch | Shared grant/revoke fixtures; failed revoke snapshot equality | Every maintenance writer and deployment-specific writer |
| T1: interrupted drop | Inherited deletion fixtures; failure inside cleanup rolls back all records | Arbitrary-size drop completion and deferred reclamation |
| T2: unchanged-object / absence dependencies | Native parent/empty-child-range race | Bringing every feature-level precheck into a Manager attempt |
| T2: composed reads | Existing resolved-read fixtures use the shared transaction view | HTTP list filtering, independent calls, caches and multipage snapshots |
| T2: retry / independent RBAC / credentials | Explicit conflict translation, no unknown replay; shared credential code | Complete bounded business-attempt retry, authorization policy and STS |
| T3: staged helper versus successful publication | The wrapper returns success only after native commit | All caller contracts, protocol outcome mappings and external cleanup |

## Repository gates

`polaris-core:check` and `polaris-persistence-primitives:check` have passed,
including their formatter and checkstyle tasks. The repository-wide
`format compileAll` and `check` gates are being evaluated separately; they are
not covered by the module-level result above. This prototype is not declared
merge-ready or production-ready.

## What the result supports

The narrower backend boundary is useful in the real Java implementation:
new native adapters do not implement Polaris catalog, grant, policy or secret
rules. Existing Manager semantics are largely reusable. The CreateCatalog phase
refactor can preserve native transaction context and use Spanner buffered
mutations without a generic query overlay.

The result does not prove that a new public SPI is the only way to achieve this,
that all workflows are migrated, or that the common record layout is optimal.
The remaining work is concrete: bounded operation retry with its caller contract,
PostgreSQL execution, production Spanner validation, runtime HTTP tests, complete
writer/dependency coverage, schema migration, and workload measurements.
