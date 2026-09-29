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
Validation uses JDK 21 and the upstream Gradle 9.7.1 wrapper. Native results are
versioned per checkpoint. An older result does not certify later code.

## Repaired repository gate, 2026-09-29

[Run 36632123504](https://github.com/flyingImer/polaris/actions/runs/36632123504)
completed successfully at 2026-09-29 22:26 UTC. Its tested source is
`90af5300fbdec108e038d7b211616d269b0d0e41`. This section supersedes the
pending root-gate status below for that source only. The evidence commit changes
validation records only. It does not certify later Java, build or test changes.

The job logs and downloaded JUnit XML were checked independently of the workflow
summary. All three artifact SHA-256 digests match GitHub's artifact metadata.
Machine-readable results, artifact identities and all XML task-group skip counts
are in [validation/ci-repaired-gate.json](validation/ci-repaired-gate.json).

| Gate | Actual result |
|---|---|
| Metadata-denial preflight, all three jobs | Passed before databases/tests. Host and container IPv4 denial counters each reached 3. IPv6 reported no route |
| `format compileAll --max-workers=2` | BUILD SUCCESSFUL in 7m 14s. Subsequent `git diff --exit-code` passed |
| Early `:polaris-bom:check :polaris-server:generateLicenseReport --max-workers=2` | BUILD SUCCESSFUL in 47s. Both previously failing tasks executed successfully |
| Full `check --continue --max-workers=2` | BUILD SUCCESSFUL in 1h 1m 35s. No failed task or JUnit failure/error |
| PostgreSQL 17, new Primitives adapter | 47 passed, 0 failed, 0 skipped |
| CockroachDB 26.3.1, new Primitives adapter | 47 passed, 0 failed, 0 skipped. Docker network remained internal, with no published ports |

The root artifact records Service unit tests as 23,751 passed / 43 skipped,
Service integration tests as 2,820 passed / 70 skipped, and cloud tests as
0 passed / 897 skipped. Core has 1,004 passed / 16 skipped. The default
Primitives configuration has 43 passed / 32 skipped, including unconfigured
native fixtures and H2 isolation schedules. The two configured native jobs
provide separate evidence and are not inferred from those skips.

Five legacy CockroachDB Service integration suites have 481 passed / 11 skipped.
They are already included in the Service integration total and use
`relational-jdbc`, not the new Primitives adapter.

The retained nftables artifacts confirm host output and forwarded-container
metadata rejection rules. CockroachDB's network artifact reports `Internal=true`.
Counters cover the entire runner and do not identify which process attempted
traffic. IPv6 rule presence is not proof of packet rejection on this IPv4-only
execution.

Earlier FDB 47/0/0 and Spanner emulator 44/0/3 evidence remains historical
native evidence for unchanged Java sources. This run does not re-execute those
backends. The three Spanner race skips remain. No production performance,
production Spanner concurrency, migration or complete community-requirement
claim follows from this gate.

The fresh-authorization claim in the earlier checkpoint refers to a mock
authorizer test. This tested source does not establish real RBAC with a warmed
cache after revocation, the later Service table-create validation scenarios, or
the later UNKNOWN metadata pointer/content assertions. Its concurrent worker
tests also predate the subsequent result-collection repair. Those later changes
need their own results.

## Terminal-attempt checkpoint, 2026-09-29

This checkpoint continues `0e0cccd577122544622068e74bcf80f6f673804d` on the same
upstream base. It supersedes the earlier workflow and retry limitations only
where the new evidence below says so. The historical results remain below.
See [TERMINAL_ATTEMPTS.md](TERMINAL_ATTEMPTS.md) for accepted decisions, the original
three mailing-list problems, their implementation paths and remaining boundaries.

| Backend / gate | Passed | Failed | Skipped | Interpretation |
|---|---:|---:|---:|---|
| FDB 7.3.77, current native Java suite | 47 | 0 | 0 | All 28 inherited Manager fixtures, 14 Manager failure/dependency tests and 5 primitive tests |
| Spanner emulator 1.5.57, current unfiltered suite | 44 | 0 | 3 | Both inherited parallel task tests included. Three simultaneous read/write rendezvous races skipped because of emulator locking |
| `polaris-core:check`, local and CI | 1,004 | 0 | 16 | Full module check passed, including the bounded retry policy tests |
| `polaris-persistence-primitives:check`, local and CI default configuration | 43 | 0 | 32 | Full module check passed. 28 unconfigured native fixtures and 4 H2 isolation schedules skipped |
| Focused Service / compatibility checks | 120 | 0 | 0 | Admin 21, metadata cleanup 10, mapper 70, allowed locations 18, tracing integration 1. Separate from the full Service gate |
| Repository `format compileAll` | N/A | N/A | N/A | Passed locally and in clean CI. Formatting left CI sources unchanged |
| CockroachDB 26.3.1, current Primitives adapter in isolated CI | 47 | 0 | 0 | Same 28 Manager, 14 failure/dependency and 5 primitive tests. All simultaneous race schedules included |
| PostgreSQL 17, native Java suite in fork CI | 47 | 0 | 0 | Same 28 Manager, 14 failure/dependency and 5 primitives tests. All simultaneous race schedules included |
| Full CI Service `test`, JUnit XML totals | 23,751 | 0 | 43 | CloudWatch 4/4, tracing and all 18 allowed-location tests pass |
| Full CI Service `intTest` | 2,820 | 0 | 70 | Integration tests also passed |
| Existing CockroachDB `relational-jdbc`, subset of Service `intTest` | 481 | 0 | 11 | Five existing Service suites. Not the new Primitives adapter or an additional test total |
| CI Service `cloudTest` | 0 | 0 | 897 | Entire cloud suite skipped. No production-cloud verification claimed |
| Full local `polaris-runtime-service:check` | 23,741 | 1 | 43 | Only failure is CloudWatch test initialization without Docker |
| Local root `check`, admin tests | 18 | 31 | 0 | All failures are Docker/Testcontainers initialization, downstream checks did not all run |

Local gate results are recorded in `validation/gates-terminal.json`. Service
counts use the Gradle run summary. The retained XML aggregate has six additional
cases and is recorded separately. The local root gate did not pass. The repaired CI root gate above passed.

The isolated repository run
[36617991766](https://github.com/flyingImer/polaris/actions/runs/36617991766)
finished at 2026-09-29 20:32 UTC with two failed tasks. Formatting and compilation
passed. Root `check --continue` ran for 1h 5m 41s and reported only
`:polaris-bom:verifyBomDependencies` and
`:polaris-server:generateLicenseReport` as failures. The new Primitives module
was missing from the BOM, and twelve FDB/Spanner-related dependency mentions were
missing from the server distribution LICENSE. No test task reported failure.
This is a completed failed gate, not an ongoing run or an environment blocker.
Its native CockroachDB and PostgreSQL jobs both passed, as recorded below.

The follow-up adds those declarations without changing Java sources or dependency
versions. CI now checks BOM and distribution licenses before the full test phase,
so the same omissions will fail early. The repaired repository gate passed in run 36632123504, as recorded above.

The earlier `durable-java-poc.yml` run executed formatting, compilation
and complete checks for the three touched modules on a Docker-capable runner.
A separate job ran the same native Manager and attempt suite on PostgreSQL 17.
The PostgreSQL job passed on commit `641cd2cda997eeb6dea2095a28c00715881be1d9`.
Its XML summary is `validation/postgresql-terminal.json`. The module gate job
also passed, including Service integration tests. Counts and individual suite
summaries are in `validation/ci-terminal.json`. These use the artifact's JUnit
XML totals. That run did not execute root `check` and is not a passing
repository-wide gate.

A subsequent read-only audit corrected an earlier reporting error: omitting root
`check` did not exclude CockroachDB. Service `intTest` also starts it. Five existing
CockroachDB Service suites report 492 cases, with 481 passed and 11 skipped. Their
lifecycle configuration selects `polaris.persistence.type=relational-jdbc`, not
the new Primitives adapter. The configured container image is
`cockroachdb/cockroach:v26.3.1`. These results are already included in the Service
integration total above. They do not certify the new adapter's 47-case suite or
authorize further execution of the blocked startup.

[CI run 36526283593](https://github.com/flyingImer/polaris/actions/runs/36526283593)
finished successfully. Formatting and compilation took 7m 6s. The three module
checks took 43m 46s, including roughly 20 additional minutes of Service integration
tests after the unit tests. Local failure had prevented those later tasks from
running. This is a test-run duration, not a backend performance measurement.

FDB used a disposable single-node memory-engine configuration. Spanner used the
local emulator. These runs do not measure crash recovery, replicated availability
or production throughput.

The native suites execute the same Java Manager and shared record implementation.
All write callbacks now enforce reads-before-final-mutations. The old native
read-your-writes compatibility path has been removed. Batch reads preserve order,
duplicates and missing keys. JDBC writes batch adjacent statements on the same
transaction. Failure after an executed write chunk still rolls back the whole
operation. FDB and Spanner retain their native transaction through final commit.

The new coverage includes stale unchanged ancestors, location overlap within a
batch, protected namespace and sibling-location races, atomic local credential
reset, and a composed read retaining one snapshot across another publication.
The latter passes on FDB, PostgreSQL and the Spanner emulator. H2 is only a wiring
and rollback test, not an isolation proof.

The first current-code Spanner run used a read/write context for pure reads and
failed a contended inherited task test. Introducing an explicit read-only view
lets Spanner use its native read-only transaction. The next run showed that the
initial eight-attempt write retry cap was too short for emulator contention.
The final policy permits at most 32 attempts within a two-second budget for
starting retries. The final unfiltered suite passes both parallel task tests.
This is compatibility evidence, not a production latency or concurrency claim.
The initial failed result is retained in `validation/spanner-terminal-initial.json`.

Actual Service tests cover fresh authorization on conflict retry, stopping when
permission is revoked, no retry for stale expectations or UNKNOWN, retention of
external-catalog secret references on uncertainty, and table/view/multi-table
metadata retention when publication may have happened. Multi-table confirmed
failures clean the newly written files. Uncertainty is injected both before and
after actual PoC publication. This tests caller behavior for both possible states,
not a real network partition or an automatic recovery protocol.

The first full Service run was stopped after finding a real view compatibility
regression. The prototype had persisted the view location as public
`baseLocation`, unintentionally narrowing allowed metadata paths. The corrected
mapping stores the validation fact in internal metadata instead. All 18 existing
allowed-location tests pass, and both native suites pass the new test proving
that shared Manager overlap validation still protects this internal location.

The earlier tracing failure now has an isolated control on the current code:
`InMemoryBufferEventListenerIntegrationTest` fails with the host's 1% sampling
configuration (`02` instead of the expected `03`), and passes after removing
`OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG`. The test and its assertions
are unchanged. The final gates use that isolated test environment. No repository
sampling policy has been changed. See `validation/callers-terminal.json`.

Local CockroachDB native-suite startup was initially blocked after the runner triggered a
request to a cloud metadata endpoint. The later module CI nevertheless started legacy CockroachDB
through Service integration tests, which the earlier report overlooked. No
metadata-network-access audit was collected for those containers. This corrects
the earlier claim that CI had not started CockroachDB. The older
`cockroachdb-final.json` remains baseline evidence only.
PostgreSQL is not inferred from H2, CockroachDB, compilation or common JDBC code.

After explicit authorization for an isolated execution plan, native CockroachDB
verification passed in [CI run 36617991766](https://github.com/flyingImer/polaris/actions/runs/36617991766)
on commit `f2997415de7a06f457756bb5510fa06a325f8bed`. The Java sources are unchanged
from `641cd2cda997eeb6dea2095a28c00715881be1d9`. PostgreSQL also passed all 47 tests
again in this run. Their individual test results are in
`validation/cockroachdb-isolated.json` and `validation/postgresql-isolated.json`.

Before starting databases, every job installs a separate nftables table rejecting
metadata-address ranges from host and forwarded container traffic. TCP-only
probes require explicit denial and matching firewall counter increments from
both paths. They send no application data. IPv6 rules are installed, but the
IPv6 probe reports no route, not empirical packet rejection. The CockroachDB
native job additionally uses an internal Docker network without external routing
or published ports. The archived network configuration and server output confirm
`Internal=true` and CockroachDB v26.3.1. Firewall counters are runner-wide and must
not be interpreted as CockroachDB-specific traffic counts.

The first isolated startup failed before Java tests: the official image entrypoint
rejected an explicit `--listen-addr=0.0.0.0:26257`. Using the image's default
listener configuration fixed startup without changing the internal network,
metadata deny rules or adapter. The diagnostic run is retained in the native
report. These are disposable single-node in-memory results, not replicated
availability, crash recovery or production performance evidence.

The workspace mirrored stale Spotless task outputs and temporary `.rsync-tmp`
class paths. Local verification forced fresh Spotless computation and excluded
only those temporary class paths from discovery. Java compilation ran without
incremental compilation or build-cache reuse. Formatting and lint checks remained
enabled. These workspace workarounds are not changes to the repository build.

## Historical checkpoint, 2026-09-23

The remainder of this document describes the previously committed baseline,
including its failures and unfinished work. It is preserved for comparison.
Its open retry/read-your-writes items should not be read as the current status.

### Recorded runs

| Backend / gate | Passed | Failed | Skipped / excluded | Interpretation |
|---|---:|---:|---|---|
| FDB 7.3.77, final native Java run | 35 | 0 | None | 28 unchanged upstream Manager fixtures, 4 injected-failure tests, 3 primitive contract tests |
| CockroachDB 25.4.0, final native Java run | 35 | 0 | None | Same Manager and mapping code with the generic PostgreSQL JDBC adapter |
| Spanner emulator 1.5.57, serial workflow run | 32 | 0 | 1 race skipped, 2 inherited parallel tests explicitly excluded | 26 upstream Manager fixtures, 4 fault tests, 2 primitive checks |
| Spanner emulator, initial unfiltered run | 28 | 5 | 1 race skipped | Preserved failure evidence, explained below |
| `polaris-core:check`, local and CI | 1,001 | 0 | 16 tests skipped by upstream | Core tests, checkstyle and formatting checks passed |
| `polaris-persistence-primitives:check`, default local configuration | 34 | 0 | 28 native fixtures unconfigured, 1 H2 race skipped | H2 Java wiring and fault tests. These do not prove native isolation |
| PostgreSQL server | N/A | N/A | Not executed | Adapter compiled. CockroachDB results do not establish PostgreSQL behavior |
| Repository `format compileAll` | N/A | N/A | Passed | Build successful, with 1,027 actionable tasks |
| Repository `check`, admin test result | 18 | 31 | Stopped at admin tests | All 31 failures report Docker/Testcontainers startup. Downstream checks did not all execute |

The Spanner client is pinned to 6.120.0 with explicit serializable isolation.
The emulator is connected with an explicit local endpoint and project, without
ambient credential discovery. The FDB Java client is pinned to 7.3.77.

The unfiltered Spanner run failed its two parallel task tests. Creation exposed
a confirmed abort reaching a caller without a complete operation retry loop.
Concurrent task claiming exceeded the upstream fixture's 30-second deadline.
Workers still active after that deadline interfered with three later setup
transactions. The separate serial run used a fresh emulator and an explicitly
named test filter. It does not erase the unfiltered result, establish production
performance, or certify Spanner concurrency. The initial run predates the final
idempotent close guard and additional drop-failure test. It remains initial-run
evidence, not a claim that every final-code failure was reproduced unchanged.

PostgreSQL was not executed in this workspace: no PostgreSQL service or Docker
runtime was available, and the workspace runs as root. The branch supplies the
same JDBC adapter and an explicit PostgreSQL test command. No PostgreSQL result
is inferred from H2 or CockroachDB.

Machine-readable native summaries are in `validation/`. They contain test names
and results, not credentials, raw record contents or noisy server logs. Gradle can
retain XML from earlier test filters. Summaries for a native run select only the
native fixture class plus the explicitly selected fault/contract classes.

### What was actually exercised

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
- Close aborts native staged changes. Terminal batches combine updates/deletes.
- Confirmed conflicts map to the exception existing callers recognize. Explicit
  rollback followed by a business failure result does not accidentally commit.

Fault injection is around real native attempts. It verifies Java propagation
and cleanup behavior, not every possible server/driver network failure class.

### Coverage against the community problems

| Original concern | Evidence here | Still outside this branch's claim |
|---|---|---|
| T1: partial catalog initialization | Real Manager catalog fixtures and rejection/response-lost injection | External storage integration effects |
| T1: grant/version mismatch | Shared grant/revoke fixtures and failed-revoke snapshot equality | Every maintenance writer and deployment-specific writer |
| T1: interrupted drop | Inherited deletion fixtures and rollback of all records on cleanup failure | Arbitrary-size drop completion and deferred reclamation |
| T2: unchanged-object / absence dependencies | Native parent/empty-child-range race | Bringing every feature-level precheck into a Manager attempt |
| T2: composed reads | Existing resolved-read fixtures use the shared transaction view | HTTP list filtering, independent calls, caches and multipage snapshots |
| T2: retry / independent RBAC / credentials | Explicit conflict translation, no unknown replay, shared credential code | Complete bounded business-attempt retry, authorization policy and STS |
| T3: staged helper versus successful publication | The wrapper returns success only after native commit | All caller contracts, protocol outcome mappings and external cleanup |

### Repository gates

`polaris-core:check` and `polaris-persistence-primitives:check` have passed,
including their formatter and checkstyle tasks. The repository-wide
`format compileAll` gate also passed, including service-side Quarkus code
generation and downstream Java compilation.

Repository `check` failed at `polaris-admin:test`: 49 tests ran, 18 passed and
31 failed. Inspection of all 31 failure traces found Docker/Testcontainers
startup failures. The workspace has no working Docker runtime. No tests were
excluded to turn this result into a pass, and downstream checks cannot be
inferred from this interrupted root run.

The separate `polaris-runtime-service:check` run also failed. Gradle reported
23,769 completed tests, 2 failures and 43 skipped tests. The failures were:

- `AwsCloudWatchEventListenerTest`: Docker/Testcontainers initialization.
- `InMemoryBufferEventListenerIntegrationTest`: expected sampled trace flags
  `03`, observed unsampled flags `02`.

The host sets `OTEL_TRACES_SAMPLER=parentbased_traceidratio` and
`OTEL_TRACES_SAMPLER_ARG=0.01`, which is a candidate explanation for the tracing
assertion. This is not yet a confirmed root-cause result. A single-test rerun
with those two variables removed did not finish and was interrupted. A control
run on an untouched checkout of the same upstream commit also did not finish
within its 240-second limit, including build setup. Neither diagnostic run is
counted as passed or as proof of a baseline regression. The tracing result
remains unresolved.

The root `AGENTS.md` requires both full gates before declaring completion or
opening a PR. This remains a reviewable WIP prototype, not a merge-ready or
production-ready change.

### What the result supports

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
