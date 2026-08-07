/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.spi.durable;

/**
 * The kinds of record the durable data model defines.
 *
 * <p>This enum is the vocabulary every other type in this package addresses records by. It exists
 * so that {@link RecordRef}, {@link Mutation} and the read operations can be record-kind agnostic:
 * adding a kind adds a constant here and a mapper registration in whichever implementation stores
 * it, and changes no method signature. That property is the reason the write side collapses to one
 * commit operation rather than one method per kind.
 *
 * <p>Required versus optional is a property of the durable data model, not of this enum, and is
 * documented in {@code docs/contracts/durable-logical-data-model.md}. Two kinds are deliberately
 * absent, and their absence is a decision rather than an omission:
 *
 * <ul>
 *   <li><b>Idempotency records</b> — the shipped {@code IdempotencyStore} has one implementor and
 *       zero production callers, so it is unwired scaffolding rather than a record kind anything
 *       depends on.
 *   <li><b>Metrics reports</b> — optional by explicit design; the consuming reporter documents that
 *       metrics are silently discarded when the backing store does not support them.
 * </ul>
 */
public enum RecordKind {
  /**
   * A catalog, namespace, table-like, generic-table, principal, principal-role, catalog-role or
   * policy record. Its logical identity is {@code (realm, id)} and its logical uniqueness is {@code
   * (realm, parent, type, name)} — identity is deliberately wider than uniqueness, which is why
   * {@link RecordRef} needs both addressing modes.
   */
  ENTITY,

  /**
   * A privilege granted on a securable to a grantee. Identity and uniqueness are the same tuple,
   * {@code (realm, securable, grantee, privilege)}, because every field of the record is part of
   * its own key. Re-asserting one is therefore naturally idempotent and carries {@link
   * Precondition#none()} rather than a must-not-exist check.
   */
  GRANT_RECORD,

  /**
   * An attachment of one policy to one target. Identity and uniqueness are both {@code (realm,
   * target, policy-type, policy)}. Note that the policy type sits in the key alongside the policy,
   * which is why the stronger "at most one inheritable policy of a given type per target" rule
   * cannot be enforced by this key and lives in the selected implementation instead.
   */
  POLICY_MAPPING,

  /**
   * A principal's secret material. Identity is {@code (realm, client-id)}, because authentication
   * looks secrets up before the principal is resolved. A second uniqueness rule — at most one live
   * set per principal — is stated by the data model and is not enforced by any shipped backend.
   */
  PRINCIPAL_SECRETS,

  /**
   * An audit event. Optional: two of three shipped backends do not implement event persistence at
   * all, so a caller must be able to detect its absence rather than assume it.
   */
  EVENT,
}
