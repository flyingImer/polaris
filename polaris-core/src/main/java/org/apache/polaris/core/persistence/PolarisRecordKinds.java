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

import org.apache.polaris.spi.durable.LookupPath;
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

  // ------------------------------------------------------------- declared lookup paths
  //
  // Each kind's lookup paths are declared in the durable logical data model (each kind's "other
  // lookup paths" section) and realized in every implementation registered for the kind; these
  // constants are the code-side spelling of those declarations, sited here and not in the SPI
  // package for the same S4 reason the kinds are. Principal secrets and events declare no list
  // paths: the model's by-principal and enumeration paths for secrets are documented gaps no
  // shipped backend serves, and serving them would be a new path, which only a data-model change
  // introduces.

  /**
   * {@link #ENTITY}'s children listing: anchors are the parent address, {@code (parent-catalog,
   * parent)}, optionally narrowed by one trailing subtype code. Realized against the shipped
   * children query's exact filter columns.
   */
  public static final LookupPath ENTITY_BY_PARENT = LookupPath.of("by-parent");

  /**
   * {@link #ENTITY}'s location scan, serving the overlapping-location check: anchors are {@code
   * (catalog, location-prefix)}. The catalog anchor follows the shipped overlap query, which
   * filters by catalog rather than by parent.
   */
  public static final LookupPath ENTITY_BY_LOCATION_PREFIX = LookupPath.of("by-location-prefix");

  /**
   * {@link #GRANT_RECORD}s on a securable: anchors are {@code (securable-catalog, securable)}.
   * Load-bearing rather than an optimization — entity drop finds the grants to clean up through it.
   */
  public static final LookupPath GRANT_RECORD_BY_SECURABLE = LookupPath.of("by-securable");

  /**
   * {@link #GRANT_RECORD}s to a grantee: anchors are {@code (grantee-catalog, grantee)}.
   * Authorization resolves a grantee's privileges through it.
   */
  public static final LookupPath GRANT_RECORD_BY_GRANTEE = LookupPath.of("by-grantee");

  /**
   * {@link #POLICY_MAPPING}s on a target — "what policies apply to this table": anchors are {@code
   * (target-catalog, target)}.
   */
  public static final LookupPath POLICY_MAPPING_BY_TARGET = LookupPath.of("by-target");

  /**
   * {@link #POLICY_MAPPING}s of a policy — "what does this policy apply to": anchors are {@code
   * (policy-catalog, policy)}.
   */
  public static final LookupPath POLICY_MAPPING_BY_POLICY = LookupPath.of("by-policy");

  private PolarisRecordKinds() {}
}
