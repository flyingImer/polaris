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

# Community alignment PoC: decisions 2–9

Starting point: `prototype/durable-java-primitives` at
`9141a0d708841f04d1133d73b21b7bdcc49b7273` (2026-10-30).
This checkpoint extends the Java PoC. It is not a production rollout or a proposed
migration of existing stores. Current-run results belong in `VALIDATION.md`;
older validation checkpoints do not certify these changes.

## Decision coverage

| Decision | Implementation and observable result | Main regression evidence |
|---|---|---|
| 2. Cross-object check-only dependencies | Requirement-only B participates in A's publishing attempt; B's entity, metadata pointer and files stay unchanged. Reciprocal dependencies cannot both succeed. | `PrimitiveCommitDependencyTest`, `ManagerFailureTest.reciprocalCheckOnlyDependenciesCannotBothCommit` |
| 3. Internal validation reads | Manager reads sibling state within its protected attempt. The caller acquires no sibling read privilege; public errors omit hidden names and locations. | `PrimitiveTableCreateTest.internalSiblingValidationDoesNotGrantReadAccessOrRevealMetadata` uses the real authorizer, a denied sibling read and an allowed non-overlapping create. |
| 4. Manifest observations | FOUND/ABSENT/UNRESOLVED are distinct. Metadata dependency comes from the snapshot actually used for validation; fresh passthrough cannot replace that token. | `PolarisResolutionManifestLookupKeyTest`, `optionalAbsentThenCreatedBeforeCommit`, `passthroughSnapshotChangedBeforeCommit` |
| 5. Identity-based detach | Detach checks target/policy identity and mapping, without adding unrelated metadata-version fences. Existing implementation retained. | `ManagerFailureTest.detachUsesIdentityAfterUnrelatedMetadataChanges` |
| 6. Invalid duplicate mutations | Final duplicate entity batch fails as invalid input before version checks or writes. Legal same-table REST requests still coalesce. | `duplicateMutationIsInvalidWithoutPartialWrites`, `repeatedTableRequestsCoalesceIntoOneMutation`, `IcebergExceptionMapperTest` |
| 7. Outcomes and retry owner | APPLIED, confirmed-abort CONFLICT, definite-no-effects REJECTED and maybe-applied UNKNOWN stay distinct. Only the operation owner may retry known-safe confirmed aborts within a bound and with fresh authorization. UNKNOWN has no replay or abort cleanup. | Existing RBAC/retry/fault tests plus `unknownWithCheckOnlyDependencyRetainsMetadata` for both applied and unapplied injections. |
| 8. Correctness before optimization | Retain broad protected sibling scans and the separate ID reservation transaction. Measure predicate contention separately from sequence contention. No schema/index/allocator optimization. | `ConflictGranularityTest`: eight cases (0/32 initial siblings, 1/4 writers, range/sequence), attempts, aborts, retries and p95. |
| 9. Staged backend qualification | Use the same correctness tests on supported native environments; require non-skipped named regressions and retain JUnit and measurements per backend. | `durable-java-poc.yml` and `poc-verify-dependencies.py`; PostgreSQL, CockroachDB and FDB jobs. |

## What the Primitives contract says

The API remains `begin`, protected point/batch/range reads, and one terminal
`commit` on that same attempt. Missing keys and empty ranges participate.
Check-only reads are meaningful even when their keys never appear in mutations.
Serialization is the correctness guarantee, not equality of every read value at
the instant of physical commit. The one-way schedule (T1 reads B, T2 writes B and
commits, T1 writes A) may legally serialize T1 before T2; conservative abort is
also legal. `oneWayDependencyMaySerializeBeforeEarlierPhysicalCommit` records
which outcome the tested backend produced. Reciprocal dependencies must not both
commit. The Manager test uses independent principals to keep sibling range scans
from masking a missing read-dependency implementation.

Operation-specific validation chooses what must join the attempt. Authorization
resolution is not a universal commit-time authorization fence. An already
authorized attempt may finish; a retry reconstructs resolution and authorizes
again. Policy detach illustrates why unrelated metadata should not automatically
become a precondition.

Invalid input is not a retryable conflict. A lost commit response is not a
confirmed abort. Backend algorithms may conservatively abort valid operations;
conflict rate and retry cost are performance results, not weaker correctness
modes. APPLIED is represented by successful return, not a new result enum.

## Community scenarios

- [#5807](https://github.com/apache/polaris/issues/5807): write A with a schema
  assertion on B and no updates to B, paired with the reciprocal transaction.
  The new request-level test covers this missing dependency; it also checks B
  remains unchanged and that a newer B snapshot cannot replace the validated one.
- [#5740](https://github.com/apache/polaris/pull/5740): moving three tables' main
  refs to branch heads. Snapshot/ref tests preserve all-or-nothing promotion,
  including two concurrent promotions and promotion versus one table's main
  update. These preserve the existing write-set guarantee and are distinct from
  the requirement-only regression.

## Interpreting measurements and qualification

The contention experiment is a small isolated workload, not production table
creation throughput. Range cases scan the sibling prefix then insert a distinct
record; sequence cases only reserve a value from one shared counter. Successful
operations, attempts, confirmed conflicts (also retry count), abort fraction and
p95 include retry latency. They do not include Feature authorization, object
storage writes or the entire Manager operation. No performance threshold is a
correctness criterion. Initial sibling counts are fixed, and range cases grow
as operations complete. Sequence cases keep siblings inert as a control.

H2 supports fast functional regressions; it does not qualify native serializable
races. FDB tests qualify the shared Manager and primitive boundary, not the JDBC
service fixture. Spanner emulator evidence, where explicitly run, does not
qualify production Spanner concurrency. Production Spanner, crash recovery,
large-scale performance, mixed writers, schema migration and rollout are not
established here. NoSQL remains on its existing persistence implementation;
a future mapping onto Primitives remains an architectural option.

Skipped, absent or environment-blocked cases must remain visible. Backend name,
version, tested revision, environment and executed/failed/skipped counts accompany
results. Decision 10 (migration and rollout) remains deferred.
