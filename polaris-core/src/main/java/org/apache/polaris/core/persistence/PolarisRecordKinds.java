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
package org.apache.polaris.core.persistence;

import org.apache.polaris.spi.durable.RecordKind;

/**
 * The record kinds Polaris itself defines.
 *
 * <p><b>Why this lives here and not in the SPI package.</b> Issue 47's S4 requires that adding a
 * record kind must not change the primitives contract. If these constants sat beside {@link
 * RecordKind} in {@code org.apache.polaris.spi.durable}, adding one would edit the contract. Here,
 * adding a kind edits Polaris's own declaration and registers one mapper with whichever store holds
 * it — the SPI is untouched. An extension that defines its own record family declares its own
 * constants in its own module the same way, and namespacing keeps the identifiers from colliding.
 *
 * <p>Required-versus-optional is a property of the durable data model rather than of this list, and
 * is documented in {@code docs/contracts/durable-logical-data-model.md}. Two kinds are deliberately
 * absent, and the absence is a decision rather than an oversight:
 *
 * <ul>
 *   <li><b>Idempotency records</b> — the shipped {@code IdempotencyStore} has one implementor and
 *       zero production callers, so it is unwired scaffolding rather than a kind anything depends
 *       on.
 *   <li><b>Metrics reports</b> — optional by explicit design; the consuming reporter's own javadoc
 *       states that metrics are silently discarded when the backing store does not support them.
 * </ul>
 */
public final class PolarisRecordKinds {

  /**
   * A catalog, namespace, table-like, generic-table, principal, principal-role, catalog-role or
   * policy record. Logical identity is {@code (realm, id)}; logical uniqueness is {@code (realm,
   * parent, type, name)}. Identity is deliberately wider than uniqueness, which is why a reference
   * needs both addressing modes.
   */
  public static final RecordKind ENTITY = RecordKind.of("polaris.entity");

  /**
   * A privilege granted on a securable to a grantee. Identity and uniqueness are the same tuple,
   * {@code (realm, securable, grantee, privilege)}, because every field is part of the key — so
   * re-asserting one is naturally idempotent and carries no must-not-exist condition.
   */
  public static final RecordKind GRANT_RECORD = RecordKind.of("polaris.grant-record");

  /**
   * An attachment of one policy to one target. Identity and uniqueness are both {@code (realm,
   * target, policy-type, policy)}. The policy type sits in the key alongside the policy, which is
   * why the stronger "at most one inheritable policy of a given type per target" rule cannot be
   * enforced by this key and is enforced by the selected implementation instead.
   */
  public static final RecordKind POLICY_MAPPING = RecordKind.of("polaris.policy-mapping");

  /**
   * A principal's secret material. Identity is {@code (realm, client-id)}, because authentication
   * resolves secrets before it resolves the principal. A second uniqueness rule — at most one live
   * set per principal — is stated by the data model and is not enforced by any shipped backend.
   */
  public static final RecordKind PRINCIPAL_SECRETS = RecordKind.of("polaris.principal-secrets");

  /**
   * An audit event. Optional: two of three shipped backends do not implement event persistence at
   * all, so a caller must be able to detect its absence rather than assume it.
   */
  public static final RecordKind EVENT = RecordKind.of("polaris.event");

  private PolarisRecordKinds() {}
}
