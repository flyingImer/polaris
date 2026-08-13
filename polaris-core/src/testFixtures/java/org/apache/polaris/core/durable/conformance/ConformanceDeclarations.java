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
package org.apache.polaris.core.durable.conformance;

import java.util.List;
import java.util.Map;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.spi.durable.RecordRef;
import org.jspecify.annotations.Nullable;

/**
 * The Polaris record family's conformance declarations: one {@link PathCase} per (kind × declared
 * lookup path), the code-side mirror of {@code PolarisRecordKinds}' path constants and the durable
 * logical data model's per-kind anchor signatures.
 *
 * <p>Principal secrets and events appear in no entry because they declare no list paths — a
 * documented gap in the data model, not an omission here. A store's registration serving these six
 * declarations passes the generated path cases; a store missing one fails them, which is the
 * declared-once-normative discipline made executable.
 */
public final class ConformanceDeclarations {

  private ConformanceDeclarations() {}

  /** The six declared (kind × path) pairs of the Polaris record family. */
  public static List<PathCase> polarisPathCases() {
    return List.of(
        entityByParent(), entityByLocationPrefix(),
        grantBySecurable(), grantByGrantee(),
        policyByTarget(), policyByPolicy());
  }

  // ---------------------------------------------------------------- entity

  private static PathCase entityByParent() {
    return new PathCase(
        PolarisRecordKinds.ENTITY,
        PolarisRecordKinds.ENTITY_BY_PARENT,
        List.of(Long.class, Long.class),
        Integer.class,
        seed -> List.of(1L, 100L + seed),
        PolarisEntitySubType.GENERIC_TABLE.getCode(),
        (anchors, ordinal, trailing) -> {
          long parentId = (Long) anchors.get(1);
          return entityBuilder(parentId * 1000 + ordinal, parentId, "child-" + ordinal, trailing)
              .build();
        },
        record -> entityIdentityRef((PolarisBaseEntity) record));
  }

  private static PathCase entityByLocationPrefix() {
    return new PathCase(
        PolarisRecordKinds.ENTITY,
        PolarisRecordKinds.ENTITY_BY_LOCATION_PREFIX,
        List.of(Long.class, String.class),
        null,
        seed -> List.of(1L, "s3://bucket/conformance-w" + seed),
        null,
        (anchors, ordinal, trailing) -> {
          String prefix = (String) anchors.get(1);
          // ids and names must not collide across anchor tuples: fold the prefix's seed digitally
          long seedPart = prefix.chars().sum();
          return entityBuilder(
                  500_000L + seedPart * 100 + ordinal, 1L, "loc-" + seedPart + "-" + ordinal, null)
              .propertiesAsMap(
                  Map.of(PolarisEntityConstants.ENTITY_BASE_LOCATION, prefix + "/t" + ordinal))
              .build();
        },
        record -> entityIdentityRef((PolarisBaseEntity) record));
  }

  private static PolarisBaseEntity.Builder entityBuilder(
      long id, long parentId, String name, @Nullable Object subTypeCode) {
    // real type/subtype codes: a conforming store may validate codes when converting rows.
    // A trailing subtype value mints a TABLE_LIKE, matching how the vocabulary pairs subtypes.
    int typeCode =
        subTypeCode == null
            ? PolarisEntityType.NAMESPACE.getCode()
            : PolarisEntityType.TABLE_LIKE.getCode();
    int subtype =
        subTypeCode == null ? PolarisEntitySubType.NULL_SUBTYPE.getCode() : (Integer) subTypeCode;
    return new PolarisBaseEntity.Builder()
        .catalogId(1L)
        .id(id)
        .typeCode(typeCode)
        .subTypeCode(subtype)
        .parentId(parentId)
        .name(name)
        .entityVersion(1)
        .propertiesAsMap(Map.of())
        .internalPropertiesAsMap(Map.of());
  }

  private static RecordRef entityIdentityRef(PolarisBaseEntity e) {
    return RecordRef.byIdentity(PolarisRecordKinds.ENTITY, List.of(e.getId()));
  }

  // ---------------------------------------------------------------- grant record

  private static PathCase grantBySecurable() {
    return new PathCase(
        PolarisRecordKinds.GRANT_RECORD,
        PolarisRecordKinds.GRANT_RECORD_BY_SECURABLE,
        List.of(Long.class, Long.class),
        null,
        seed -> List.of(1L, 200L + seed),
        null,
        (anchors, ordinal, trailing) ->
            new PolarisGrantRecord(
                (Long) anchors.get(0), (Long) anchors.get(1), 1L, 300L + ordinal, 3),
        record -> grantIdentityRef((PolarisGrantRecord) record));
  }

  private static PathCase grantByGrantee() {
    return new PathCase(
        PolarisRecordKinds.GRANT_RECORD,
        PolarisRecordKinds.GRANT_RECORD_BY_GRANTEE,
        List.of(Long.class, Long.class),
        null,
        seed -> List.of(1L, 400L + seed),
        null,
        (anchors, ordinal, trailing) ->
            new PolarisGrantRecord(
                1L, 500L + ordinal, (Long) anchors.get(0), (Long) anchors.get(1), 3),
        record -> grantIdentityRef((PolarisGrantRecord) record));
  }

  private static RecordRef grantIdentityRef(PolarisGrantRecord g) {
    return RecordRef.byIdentity(
        PolarisRecordKinds.GRANT_RECORD,
        List.of(
            g.getSecurableCatalogId(),
            g.getSecurableId(),
            g.getGranteeCatalogId(),
            g.getGranteeId(),
            g.getPrivilegeCode()));
  }

  // ---------------------------------------------------------------- policy mapping

  private static PathCase policyByTarget() {
    return new PathCase(
        PolarisRecordKinds.POLICY_MAPPING,
        PolarisRecordKinds.POLICY_MAPPING_BY_TARGET,
        List.of(Long.class, Long.class),
        null,
        seed -> List.of(1L, 600L + seed),
        null,
        (anchors, ordinal, trailing) ->
            new PolarisPolicyMappingRecord(
                (Long) anchors.get(0), (Long) anchors.get(1), 1L, 700L + ordinal, 5, "{}"),
        record -> policyIdentityRef((PolarisPolicyMappingRecord) record));
  }

  private static PathCase policyByPolicy() {
    return new PathCase(
        PolarisRecordKinds.POLICY_MAPPING,
        PolarisRecordKinds.POLICY_MAPPING_BY_POLICY,
        List.of(Long.class, Long.class),
        null,
        seed -> List.of(1L, 800L + seed),
        null,
        (anchors, ordinal, trailing) ->
            new PolarisPolicyMappingRecord(
                1L, 900L + ordinal, (Long) anchors.get(0), (Long) anchors.get(1), 5, "{}"),
        record -> policyIdentityRef((PolarisPolicyMappingRecord) record));
  }

  private static RecordRef policyIdentityRef(PolarisPolicyMappingRecord p) {
    return RecordRef.byIdentity(
        PolarisRecordKinds.POLICY_MAPPING,
        List.of(
            p.getTargetCatalogId(),
            p.getTargetId(),
            p.getPolicyTypeCode(),
            p.getPolicyCatalogId(),
            p.getPolicyId()));
  }
}
