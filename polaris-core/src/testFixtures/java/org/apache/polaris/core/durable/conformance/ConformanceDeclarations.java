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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.policy.PolarisPolicyMappingRecord;
import org.apache.polaris.spi.durable.LookupPath;
import org.apache.polaris.spi.durable.RecordKind;
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

  /**
   * One cross-path case per kind declaring more than one lookup path: a single record whose field
   * values serve every declared path of its kind at once. The values are hand-authored (a canonical
   * record's mutually-compatible fields cannot be derived mechanically), but the coverage is
   * validated mechanically: the covered kind/path sets must exactly equal the multi-path kinds of
   * {@link #polarisPathCases()}, so a kind gaining a second path without a cross-path row fails
   * loudly here instead of silently losing coverage.
   */
  public static List<CrossPathCase> crossPathCases() {
    List<CrossPathCase> cases = List.of(entityCrossPaths(), grantCrossPaths(), policyCrossPaths());

    Map<RecordKind, Set<LookupPath>> multiPath =
        polarisPathCases().stream()
            .collect(
                Collectors.groupingBy(
                    PathCase::kind, Collectors.mapping(PathCase::path, Collectors.toSet())))
            .entrySet()
            .stream()
            .filter(e -> e.getValue().size() > 1)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Map<RecordKind, Set<LookupPath>> covered =
        cases.stream()
            .collect(Collectors.toMap(CrossPathCase::kind, c -> c.anchorsPerPath().keySet()));
    if (!multiPath.equals(covered)) {
      throw new IllegalStateException(
          "cross-path rows out of step with the path declarations: multi-path kinds declare "
              + multiPath
              + " but the cross-path rows cover "
              + covered);
    }
    return cases;
  }

  private static CrossPathCase entityCrossPaths() {
    PolarisBaseEntity entity =
        entityBuilder(3_150_777L, 3L, 150L, "cross-path-entity", null)
            .propertiesAsMap(
                Map.of(PolarisEntityConstants.ENTITY_BASE_LOCATION, "s3://bucket/cross-w/e1"))
            .build();
    return new CrossPathCase(
        PolarisRecordKinds.ENTITY,
        entity,
        Map.of(
            PolarisRecordKinds.ENTITY_BY_PARENT, List.of(3L, 150L),
            PolarisRecordKinds.ENTITY_BY_LOCATION_PREFIX, List.of(3L, "s3://bucket/cross-w")),
        record -> entityIdentityRef((PolarisBaseEntity) record));
  }

  private static CrossPathCase grantCrossPaths() {
    PolarisGrantRecord grant = new PolarisGrantRecord(3L, 210L, 3L, 310L, 3);
    return new CrossPathCase(
        PolarisRecordKinds.GRANT_RECORD,
        grant,
        Map.of(
            PolarisRecordKinds.GRANT_RECORD_BY_SECURABLE, List.of(3L, 210L),
            PolarisRecordKinds.GRANT_RECORD_BY_GRANTEE, List.of(3L, 310L)),
        record -> grantIdentityRef((PolarisGrantRecord) record));
  }

  private static CrossPathCase policyCrossPaths() {
    PolarisPolicyMappingRecord mapping =
        new PolarisPolicyMappingRecord(3L, 610L, 3L, 710L, 5, "{}");
    return new CrossPathCase(
        PolarisRecordKinds.POLICY_MAPPING,
        mapping,
        Map.of(
            PolarisRecordKinds.POLICY_MAPPING_BY_TARGET, List.of(3L, 610L),
            PolarisRecordKinds.POLICY_MAPPING_BY_POLICY, List.of(3L, 710L)),
        record -> policyIdentityRef((PolarisPolicyMappingRecord) record));
  }

  // ---------------------------------------------------------------- entity

  private static PathCase entityByParent() {
    return new PathCase(
        PolarisRecordKinds.ENTITY,
        PolarisRecordKinds.ENTITY_BY_PARENT,
        List.of(Long.class, Long.class),
        Integer.class,
        seed -> List.of(1L + seed, 100L + seed),
        PolarisEntitySubType.GENERIC_TABLE.getCode(),
        (anchors, ordinal, trailing) -> {
          long catalogId = (Long) anchors.get(0);
          long parentId = (Long) anchors.get(1);
          return entityBuilder(
                  catalogId * 1_000_000 + parentId * 1000 + ordinal,
                  catalogId,
                  parentId,
                  "child-" + ordinal,
                  trailing)
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
        seed -> List.of(1L + seed, "s3://bucket/conformance-w" + seed),
        null,
        (anchors, ordinal, trailing) -> {
          long catalogId = (Long) anchors.get(0);
          String prefix = (String) anchors.get(1);
          // ids and names must not collide across anchor tuples: fold the catalog anchor and the
          // prefix's seed digitally
          long seedPart = prefix.chars().sum();
          return entityBuilder(
                  catalogId * 10_000_000 + 500_000L + seedPart * 100 + ordinal,
                  catalogId,
                  1L,
                  "loc-" + catalogId + "-" + seedPart + "-" + ordinal,
                  null)
              .propertiesAsMap(
                  Map.of(PolarisEntityConstants.ENTITY_BASE_LOCATION, prefix + "/t" + ordinal))
              .build();
        },
        record -> entityIdentityRef((PolarisBaseEntity) record),
        // The ancestor direction the shipped overlap query also matches (ticket 87's fix): a
        // record whose location IS a slash-terminated ancestor segment of the queried prefix.
        // Derived from the anchor: the prefix truncated to its last slash, inclusive.
        (anchors, ordinal, trailing) -> {
          long catalogId = (Long) anchors.get(0);
          String prefix = (String) anchors.get(1);
          String ancestor = prefix.substring(0, prefix.lastIndexOf('/') + 1);
          return entityBuilder(
                  catalogId * 10_000_000 + 400_000L + ordinal,
                  catalogId,
                  1L,
                  "loc-anc-" + catalogId + "-" + ordinal,
                  null)
              .propertiesAsMap(Map.of(PolarisEntityConstants.ENTITY_BASE_LOCATION, ancestor))
              .build();
        });
  }

  private static PolarisBaseEntity.Builder entityBuilder(
      long id, long catalogId, long parentId, String name, @Nullable Object subTypeCode) {
    // real type/subtype codes: a conforming store may validate codes when converting rows.
    // A trailing subtype value mints a TABLE_LIKE, matching how the vocabulary pairs subtypes.
    int typeCode =
        subTypeCode == null
            ? PolarisEntityType.NAMESPACE.getCode()
            : PolarisEntityType.TABLE_LIKE.getCode();
    int subtype =
        subTypeCode == null ? PolarisEntitySubType.NULL_SUBTYPE.getCode() : (Integer) subTypeCode;
    return new PolarisBaseEntity.Builder()
        .catalogId(catalogId)
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
        seed -> List.of(1L + seed, 200L + seed),
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
        seed -> List.of(1L + seed, 400L + seed),
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
        seed -> List.of(1L + seed, 600L + seed),
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
        seed -> List.of(1L + seed, 800L + seed),
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
