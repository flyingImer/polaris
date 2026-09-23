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

# Shared Manager / transaction-scoped Primitives prototype

This disposable prototype asks one question: **can one metadata workflow retain
its business rules while different databases implement protected reads and one
atomic publication underneath it?**

The native runner uses the same Python Manager with PostgreSQL, CockroachDB,
FoundationDB and the Spanner emulator. It extracts bounded workflows from Polaris;
it does **not** register a Java persistence extension or run Polaris REST endpoints.
Nothing in the production source tree is rewired.

Start with [the recorded results](RESULTS.md), then
[the shared Manager](native/manager.py). The exact source baseline is
[`f68699517cac761f9240f71279f26e4850b7d0c8`](https://github.com/apache/polaris/commit/f68699517cac761f9240f71279f26e4850b7d0c8).

## The proposed boundary

```python
attempt = primitives.begin(scope)
try:
    # Domain code reads and checks through this same protected attempt.
    # New objects and final versions are computed in memory.
    prepared = create_catalog(attempt, command.id, command.name)
    attempt.commit(prepared.changes)
    return prepared.candidate  # Only after confirmed commit.
finally:
    attempt.close()            # Closing never commits.
```

- The **Manager** owns catalog initialization, grant endpoint versions, deletion
  grouping, metadata checks, and bounded retries after confirmed conflicts.
- **Primitives** owns a serializable attempt, generic point/prefix access,
  terminal mutations, commit/abort and truthful outcomes. It never calls back
  into the Manager or interprets a catalog, role, or privilege.
- **Bindings** declare record fields, primary identities and lookup paths. The
  domain supplies this layout; adapters translate the declarations into native
  keys, tables and indexes. This prototype's schema is deliberately small.

All relevant metadata belongs to one backend transaction scope. No cross-database
orchestration is attempted. Native concurrency control can use internal versions;
the Manager receives no native transaction ID or database-specific evidence token.
The explicit entity/grant versions here retain their domain-visible meanings.

The Manager completes reads and validation before submitting its final batch.
There is no generic write/read overlay, detached evidence protocol, or hidden
driver replay of business code. SQL uses SERIALIZABLE transactions, FDB uses
tracked reads and native writes, and Spanner uses a read/write transaction with
terminal buffered mutations.

The existing `TransactionalMetaStoreManagerImpl` already shares substantial
operation semantics. This experiment tests a narrower database extension boundary;
it does not claim that CreateCatalog sharing is new, or that a new public SPI is
required to obtain it.

## Which community problems does this exercise?

The starting points are [T1: atomic changes][T1],
[T2: consistent multi-object operations][T2], and
[T3: staged persistence success][T3]. They are broader than these experiments.

| Problem from those discussions | Executable case | What remains outside this prototype |
| --- | --- | --- |
| Grant mutation and endpoint versions can publish separately | Grant/revoke; abort after staging the grant; alias accounting | Complete privilege validation, Java callers and caches |
| A catalog can be partially initialized | Catalog, admin role, three grants, versions and names in one publication; precommit abort | External storage integration, alternate initial roles and full service path |
| Deletion can leave partial cleanup | Leaf-grantee deletion; both grant directions; deduplicated counterpart version updates; rollback | Cascade, policies, secrets, task lifecycle and large cardinalities |
| Earlier validation omits unchanged objects or range dependencies | Namespace create/delete races; FDB unprotected-range negative control | Every writer, maintenance, cache and existing-state migration |
| Composed reads can combine incompatible observations | Entity/version/grants in one attempt, including an overlapping grant publication | Authorization-filtered pagination and all cache paths |
| Transient retry or staged success misrepresents completion | Manager-owned fresh attempt; candidate withheld on Unknown; capacity rejection | Real transport ambiguity, full exception taxonomy, external cleanup and all feature callers |
| Entity/RBAC independence, multi-table commits and credential vending | Not exercised | These remain design/integration acceptance work, not covered by secret reset or catalog tests |

In particular, this is **not evidence that all three threads are fully resolved**.
It tests a necessary storage boundary and selected failure schedules under the
declared assumptions. The in-flight authorization policy is not exercised here.

## Run it

Requirements: Linux x86-64, Python 3.12, an ordinary non-root development user,
and network access for initial package/runtime downloads. No cloud account,
production database, Docker daemon or real credentials are needed. The runner
creates temporary local stores, uses loopback database endpoints and stops its processes.
Each run uses new data directories. It does not accept a production connection URL.

From this directory:

```bash
python3 -m venv .venv
.venv/bin/pip install -r native/requirements.txt
.venv/bin/python native/fetch_native.py
.venv/bin/python native/run_native.py --output results/local-results.json
```

Or select installed runtimes:

```bash
.venv/bin/python native/run_native.py \
  --native-home /absolute/path/to/native-binaries \
  --backends cockroach fdb spanner \
  --output results/local-results.json
```

PostgreSQL comes from the `pgserver` wheel; it needs a non-root OS user. The runner
does not create users or change process credentials. The other binaries are
pinned by `fetch_native.py`. FDB and Spanner downloads have pinned digests;
CockroachDB's TLS download has a recorded checksum, not an independent signature
check. Database binaries keep their respective licenses and are not in this repo.

The runner requests that cloud credential discovery and optional reporting be
disabled, including [CockroachDB's diagnostics configuration][crdb-diagnostics]
before startup. This was **not sufficient to eliminate every background metadata
probe in the execution environment**: automatic approval review interrupted the
combined revalidation. No cloud metadata is needed for the storage contract.
The completed backend receipts and this startup limitation are recorded separately.

The runner records versions, source hashes, outcomes, final records and adapter
call counts. A nonzero exit means at least one backend was blocked, a scenario
failed, or deterministic final records differed. A concurrency race is checked
against its invariant and allowed serial outcomes, not an identical winner.

## Read the code

| File | Responsibility |
| --- | --- |
| [manager.py](native/manager.py) | One copy of domain workflows and the confirmed-conflict retry loop |
| [bindings.py](native/bindings.py) | Domain-supplied layout and final in-memory mutation collector |
| [primitives.py](native/primitives.py) | Attempt lifecycle, outcomes and deterministic fault injection |
| [adapters.py](native/adapters.py) | SQL, FDB and Spanner native mechanisms; no domain workflow imports |
| [scenarios.py](native/scenarios.py) | Schedules, expected invariants and evidence capture |
| [run_native.py](native/run_native.py) | Disposable runtime setup, execution and cross-backend comparison |
| [index.html](index.html) | Optional in-memory explanation of the state machine; never native evidence |

The HTML can be opened locally. It demonstrates provisional results, Unknown,
range protection and a coarse internal generation counter. Its counters and
records are a simplified model, separate from the Python/native experiments.

## Deliberate limits

1. **No performance equivalence claim.** Counts are adapter calls, not measured
   network RPCs. SQL currently stages one statement per logical mutation. Batching,
   SQL planning, session pooling and production throughput are unoptimized.
2. **A bounded layout.** FDB secondary index keys are derived from primary-key
   fields; its adapter rejects other layouts. This is not proof that arbitrary
   new record families work without binding or schema changes.
3. **Small admitted operations.** The capacity fault is an injected limit, not a
   measurement of native capacity. No arbitrary-size deletion or progress promise.
4. **Outcome boundary, not full recovery.** Lost acknowledgement is suppressed
   after the native SDK reports success. It proves the Manager does not release a
   candidate or ordinary-retry Unknown; it does not test every real network
   ambiguity. No exactly-once/deduplication layer is provided.
5. **Read/write fidelity is scoped.** The catalog case uses the default
   `service_admin` path, preserves its version increments and existing-ID return
   behavior, and omits provider work. The self-grant case is a helper accounting
   edge case, not a claim that an endpoint permits self-grants.
6. **Emulator qualification.** Spanner emulator outcomes cannot establish
   production locking behavior, latency, contention cost or distributed failures.
7. **No production adoption gate passed.** Full Java integration, every writer,
   migrations, authorization/listing/vending and production fault qualification
   still require their own evidence. This fork branch is a reviewable experiment.

[T1]: https://lists.apache.org/thread/l7b4py5vl0x9rbn6soy4k9qxchmpmgrd
[T2]: https://www.mail-archive.com/dev@polaris.apache.org/msg05418.html
[T3]: https://lists.apache.org/thread/rf5orxs815zs4h64p4rwp03q3pbgxb5r
[crdb-diagnostics]: https://github.com/cockroachdb/cockroach/blob/v25.4.0/pkg/util/log/logcrash/crash_reporting.go
