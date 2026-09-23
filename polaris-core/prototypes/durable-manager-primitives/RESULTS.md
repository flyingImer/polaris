<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Native experiment results

## Outcome

One shared Manager executed against **CockroachDB, FoundationDB and the Spanner
emulator**. Ten deterministic scenarios produced identical published records.
PostgreSQL's SQL adapter and local launcher are present, but its server could not
run in this root-only environment, which disallows switching OS users.

This supports the proposed encapsulation for the selected metadata workflows.
It does not establish a full Polaris backend, complete dev-list problem coverage,
or equivalent production performance.

| Backend | Runtime | Completed scenarios | Qualification |
| --- | --- | ---: | --- |
| PostgreSQL | `pgserver` local launcher + shared SQL adapter | 0 | Blocked by OS-user restrictions; not substituted with another database |
| CockroachDB | Native single-node 25.4.0 | 14 | Real server; no distributed-failure or throughput qualification |
| FoundationDB | Native single-node 7.3.77, API 730 | 15 | Includes one deliberately unsafe negative control |
| Spanner | Emulator 1.5.57, Python client 3.71.0 | 14 | API/semantics experiment, not production Spanner concurrency/performance evidence |

The [machine-readable receipt](results/native-results.json) preserves actual
published rows, traces, versions, runtime download metadata, core source hashes
and per-backend execution provenance. It is marked **partial**.

## What the runs demonstrated

| Case | Observed result |
| --- | --- |
| CreateCatalog | Catalog, admin role, three grants, name bindings and version trackers published together. Final grant versions were catalog=3, admin=4, service_admin=2. |
| Catalog identity/name checks | Existing-ID path returned current stored versions without another mutation batch; a different ID with the occupied name was rejected. |
| Precommit failure | Injected failure after the first native staging operation left prior published state intact. Spanner staging here is client-buffered, not remotely executed DML. |
| Grant/revoke | Each grant mutation published together with both endpoint versions and trackers. Injecting failure after grant staging published neither half. |
| Leaf drop | Removed incoming/outgoing grants and the leaf; incremented each surviving counterpart once, including two grants to the same counterpart. Injected interruption rolled back. |
| Namespace delete vs child creation | The protected empty range and parent dependency prevented orphan publication. FDB/CockroachDB's fresh delete rejected the now nonempty namespace; the emulator's fresh child attempt rejected the deleted parent. |
| Retry and unchanged parent | FDB and CockroachDB aborted the child's first attempt after concurrent parent deletion. The same Manager retried, reread the missing parent, and rejected. The emulator selected the competing delete as victim in this run, so that case did not exercise automatic Manager retry there. |
| Composed read during a grant change | FDB and CockroachDB retained a coherent read view; the emulator aborted the stale read attempt. A fresh view agreed on entity version, tracker and grants. |
| Lost acknowledgement | After native commit success, the injected result suppression yielded Unknown, withheld the candidate and did not trigger ordinary replay. |
| Capacity | An injected two-mutation limit rejected catalog initialization without publication. This is contract evidence, not a measured database limit. |
| FDB negative control | Replacing the protected child range read with a snapshot read allowed deletion to commit after child creation, leaving an orphan. The scenario passes by detecting the intentionally broken invariant. |

The child-create/delete races have more than one legal serial outcome. An earlier
comparison incorrectly demanded identical final rows for a race where the backends
selected different victims. The comparison now excludes race winner equality;
the per-backend checks retain the no-orphan and fresh-validation requirements.

## Cost visible at this boundary

For the default CreateCatalog path, all three executed backends recorded:

- Four point reads through one attempt.
- Eleven logical final mutations and one native commit call.
- No reads after applying the final batch.

FDB performed fourteen native `set` calls because the three grant records also
maintain a declared secondary index. Spanner buffered eleven mutations. The SQL
adapter currently executes eleven mutation statements. These are method/staging
counts, **not RPC counts, latency measurements or an optimized native baseline**.
The contract permits an adapter to batch physical writes; this prototype does not
establish the best batching strategy.

No workflow branch selects a database. The only backend-specific decisions live
in runtime setup, generic schema/index translation and native attempt execution.
This is the concrete encapsulation benefit demonstrated here.

## Verification limits and environment interruptions

The final receipt combines completed workers from more than one execution. The
core files (`manager.py`, `bindings.py`, `primitives.py`, `adapters.py`,
`scenarios.py`) have identical hashes across the compared executions. Runtime
startup/reporting and receipt persistence were adjusted separately.

Automatic approval review interrupted combined runs because a native/SDK startup
path attempted cloud metadata discovery. Local SDK/reporting opt-outs were added,
but a later combined run was still interrupted. No approval to access cloud
metadata was requested, and the probe was not rerouted through another service.
Completed worker evidence was preserved. **An uninterrupted four-backend run is
not claimed.** A clean local test environment is still needed to close that gate
and the PostgreSQL execution gap.

Repository gates were attempted:

| Check | Result |
| --- | --- |
| `./gradlew format compileAll` | Blocked before compilation: Gradle 9.7.1 distribution download reported `Network is unreachable` |
| `./gradlew check` | Same distribution-download failure; module/downstream checks did not execute |
| Native workflow scenarios | Completed worker results above; PostgreSQL blocked |
| HTML explanation | Its five model schedules and JavaScript syntax were checked with Node; browser rendering was not verified because the Chromium download failed |

The branch is an experimental WIP, not ready for a production or upstream PR.
No production Java code or existing persistence SPI is replaced. The source cases
come from `TransactionalMetaStoreManagerImpl`: grant helpers, CreateCatalog,
leaf deletion and resolved reads, at the pinned commit in the README.

The remaining acceptance work includes full service/Java integration, all writers
and caches, authorization filtering, multi-table updates, credential vending,
real network ambiguity, production Spanner, and actual load/capacity measurements.
