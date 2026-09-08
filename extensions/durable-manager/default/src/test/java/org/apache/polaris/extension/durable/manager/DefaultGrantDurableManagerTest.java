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
package org.apache.polaris.extension.durable.manager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.PolarisRecordKinds;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.LoadGrantsResult;
import org.apache.polaris.core.persistence.dao.entity.PrivilegeResult;
import org.apache.polaris.extension.orchestration.DefaultDurableOrchestrator;
import org.apache.polaris.extension.primitives.routing.MappedDurableRecordStoreLocator;
import org.apache.polaris.extension.primitives.routing.RoutingDurableRecordStore;
import org.apache.polaris.persistence.treemap.TreeMapDurableRecordStore;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Drives the six grant operations ticket 91 implements this increment — {@code
 * grantUsageOnRoleToGrantee}, {@code revokeUsageOnRoleFromGrantee}, {@code
 * grantPrivilegeOnSecurableToRole}, {@code revokePrivilegeOnSecurableFromRole}, {@code
 * loadGrantsOnSecurable}, {@code loadGrantsToGrantee} — against {@link DefaultDurableManager}
 * assembled the same way {@link AbstractDefaultDurableManagerTest} assembles it. Scaffolding, not
 * the ticket's parity evidence: {@code BaseDurableManagerTest} is the judge of that, and as of
 * increment 4 it runs (bootstrap works) and its grant-related tests — {@code testPrivileges},
 * {@code testGrantRecordWriteIsIdempotent}, {@code testLoadGrantsGranteeVsSecurableRecords} — pass.
 * These tests mirror that fixture's own assertions rather than inventing looser ones.
 */
class DefaultGrantDurableManagerTest {

  private PolarisCallContext callCtx;
  private DefaultDurableManager manager;
  private DefaultGrantDurableManager grants;

  @BeforeEach
  void setup() {
    DurableRecordStore store = new TreeMapDurableRecordStore(new PolarisDefaultDiagServiceImpl());
    DurableRecordStore primitives =
        new RoutingDurableRecordStore(
            new MappedDurableRecordStoreLocator(
                Map.of(
                    PolarisRecordKinds.ENTITY, "main",
                    PolarisRecordKinds.GRANT_RECORD, "main",
                    PolarisRecordKinds.POLICY_MAPPING, "main",
                    PolarisRecordKinds.PRINCIPAL_SECRETS, "main",
                    PolarisRecordKinds.EVENT, "main"),
                Map.of("main", store)),
            List.of(store),
            store);
    var orchestrator = new DefaultDurableOrchestrator(primitives);
    manager =
        new DefaultDurableManager(
            Clock.systemUTC(),
            new PolarisDefaultDiagServiceImpl(),
            orchestrator,
            primitives,
            PrincipalSecretsGenerator.RANDOM_SECRETS);
    grants =
        new DefaultGrantDurableManager(
            new PolarisDefaultDiagServiceImpl(), orchestrator, primitives);
    RealmContext realmContext = () -> "testRealm";
    callCtx = new PolarisCallContext(realmContext, new NeverCallOldPrimitives());
  }

  private PolarisBaseEntity newEntity(
      PolarisEntityType type, long catalogId, long parentId, String name) {
    long id = manager.generateNewEntityId(callCtx).getId();
    PolarisBaseEntity built =
        new PolarisBaseEntity.Builder()
            .catalogId(catalogId)
            .id(id)
            .typeCode(type.getCode())
            .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
            .parentId(parentId)
            .name(name)
            .propertiesAsMap(Map.of())
            .internalPropertiesAsMap(Map.of())
            .build();
    var result = manager.createEntityIfNotExists(callCtx, null, built);
    assertThat(result.isSuccess()).isTrue();
    return result.getEntity();
  }

  private int grantRecordsVersionOf(PolarisBaseEntity entity) {
    var tracking =
        manager
            .loadEntitiesChangeTracking(
                callCtx, List.of(new PolarisEntityId(entity.getCatalogId(), entity.getId())))
            .getChangeTrackingVersions();
    return tracking.get(0).grantRecordsVersion();
  }

  @Test
  void grantThenBothLoadDirectionsSeeIt() {
    PolarisBaseEntity catalog = newEntity(PolarisEntityType.CATALOG, 0L, 0L, "cat1");
    PolarisBaseEntity namespace =
        newEntity(PolarisEntityType.NAMESPACE, catalog.getId(), catalog.getId(), "ns1");
    PolarisBaseEntity role =
        newEntity(PolarisEntityType.CATALOG_ROLE, catalog.getId(), catalog.getId(), "role1");

    PrivilegeResult granted =
        grants.grantPrivilegeOnSecurableToRole(
            callCtx, role, null, namespace, PolarisPrivilege.TABLE_READ_DATA);
    assertThat(granted.isSuccess()).isTrue();

    LoadGrantsResult onSecurable = grants.loadGrantsOnSecurable(callCtx, namespace);
    assertThat(onSecurable.isSuccess()).isTrue();
    assertThat(onSecurable.getGrantRecords())
        .extracting(PolarisGrantRecord::getGranteeId, PolarisGrantRecord::getPrivilegeCode)
        .containsExactly(tuple(role.getId(), PolarisPrivilege.TABLE_READ_DATA.getCode()));
    assertThat(onSecurable.getEntities())
        .extracting(PolarisBaseEntity::getId)
        .containsExactly(role.getId());

    LoadGrantsResult toGrantee = grants.loadGrantsToGrantee(callCtx, role);
    assertThat(toGrantee.isSuccess()).isTrue();
    assertThat(toGrantee.getGrantRecords())
        .extracting(PolarisGrantRecord::getSecurableId, PolarisGrantRecord::getPrivilegeCode)
        .containsExactly(tuple(namespace.getId(), PolarisPrivilege.TABLE_READ_DATA.getCode()));
    assertThat(toGrantee.getEntities())
        .extracting(PolarisBaseEntity::getId)
        .containsExactly(namespace.getId());
  }

  @Test
  void grantingIdenticalPrivilegeTwiceLeavesExactlyOneRecord() {
    PolarisBaseEntity catalog = newEntity(PolarisEntityType.CATALOG, 0L, 0L, "cat2");
    PolarisBaseEntity namespace =
        newEntity(PolarisEntityType.NAMESPACE, catalog.getId(), catalog.getId(), "ns2");
    PolarisBaseEntity role =
        newEntity(PolarisEntityType.CATALOG_ROLE, catalog.getId(), catalog.getId(), "role2");

    grants.grantPrivilegeOnSecurableToRole(
        callCtx, role, null, namespace, PolarisPrivilege.TABLE_READ_DATA);
    grants.grantPrivilegeOnSecurableToRole(
        callCtx, role, null, namespace, PolarisPrivilege.TABLE_READ_DATA);

    assertThat(grants.loadGrantsOnSecurable(callCtx, namespace).getGrantRecords())
        .filteredOn(
            g ->
                g.getGranteeId() == role.getId()
                    && g.getSecurableId() == namespace.getId()
                    && g.getPrivilegeCode() == PolarisPrivilege.TABLE_READ_DATA.getCode())
        .hasSize(1);
    assertThat(grants.loadGrantsToGrantee(callCtx, role).getGrantRecords())
        .filteredOn(
            g ->
                g.getGranteeId() == role.getId()
                    && g.getSecurableId() == namespace.getId()
                    && g.getPrivilegeCode() == PolarisPrivilege.TABLE_READ_DATA.getCode())
        .hasSize(1);
  }

  @Test
  void revokeRemovesTheGrant() {
    PolarisBaseEntity catalog = newEntity(PolarisEntityType.CATALOG, 0L, 0L, "cat3");
    PolarisBaseEntity namespace =
        newEntity(PolarisEntityType.NAMESPACE, catalog.getId(), catalog.getId(), "ns3");
    PolarisBaseEntity role =
        newEntity(PolarisEntityType.CATALOG_ROLE, catalog.getId(), catalog.getId(), "role3");

    grants.grantPrivilegeOnSecurableToRole(
        callCtx, role, null, namespace, PolarisPrivilege.TABLE_READ_DATA);
    PrivilegeResult revoked =
        grants.revokePrivilegeOnSecurableFromRole(
            callCtx, role, null, namespace, PolarisPrivilege.TABLE_READ_DATA);
    assertThat(revoked.isSuccess()).isTrue();

    assertThat(grants.loadGrantsOnSecurable(callCtx, namespace).getGrantRecords()).isEmpty();
    assertThat(grants.loadGrantsToGrantee(callCtx, role).getGrantRecords()).isEmpty();
  }

  @Test
  void revokingAnAbsentGrantReturnsGrantNotFound() {
    PolarisBaseEntity catalog = newEntity(PolarisEntityType.CATALOG, 0L, 0L, "cat4");
    PolarisBaseEntity namespace =
        newEntity(PolarisEntityType.NAMESPACE, catalog.getId(), catalog.getId(), "ns4");
    PolarisBaseEntity role =
        newEntity(PolarisEntityType.CATALOG_ROLE, catalog.getId(), catalog.getId(), "role4");

    PrivilegeResult result =
        grants.revokePrivilegeOnSecurableFromRole(
            callCtx, role, null, namespace, PolarisPrivilege.TABLE_READ_DATA);
    assertThat(result.isSuccess()).isFalse();
    assertThat(result.getReturnStatus()).isEqualTo(BaseResult.ReturnStatus.GRANT_NOT_FOUND);
  }

  @Test
  void bothCounterpartsVersionAdvancesAfterGrantAndRevoke() {
    PolarisBaseEntity catalog = newEntity(PolarisEntityType.CATALOG, 0L, 0L, "cat5");
    PolarisBaseEntity namespace =
        newEntity(PolarisEntityType.NAMESPACE, catalog.getId(), catalog.getId(), "ns5");
    PolarisBaseEntity role =
        newEntity(PolarisEntityType.CATALOG_ROLE, catalog.getId(), catalog.getId(), "role5");

    int namespaceBefore = grantRecordsVersionOf(namespace);
    int roleBefore = grantRecordsVersionOf(role);

    grants.grantPrivilegeOnSecurableToRole(
        callCtx, role, null, namespace, PolarisPrivilege.TABLE_READ_DATA);
    assertThat(grantRecordsVersionOf(namespace)).isEqualTo(namespaceBefore + 1);
    assertThat(grantRecordsVersionOf(role)).isEqualTo(roleBefore + 1);

    grants.revokePrivilegeOnSecurableFromRole(
        callCtx, role, null, namespace, PolarisPrivilege.TABLE_READ_DATA);
    assertThat(grantRecordsVersionOf(namespace)).isEqualTo(namespaceBefore + 2);
    assertThat(grantRecordsVersionOf(role)).isEqualTo(roleBefore + 2);
  }

  @Test
  void granteeAndSecurableDirectionsStayDisjoint() {
    PolarisBaseEntity catalog = newEntity(PolarisEntityType.CATALOG, 0L, 0L, "cat6");
    PolarisBaseEntity namespace =
        newEntity(PolarisEntityType.NAMESPACE, catalog.getId(), catalog.getId(), "ns6");
    PolarisBaseEntity role =
        newEntity(PolarisEntityType.CATALOG_ROLE, catalog.getId(), catalog.getId(), "role6");
    PolarisBaseEntity principalRole = newEntity(PolarisEntityType.PRINCIPAL_ROLE, 0L, 0L, "pr6");

    // `role` is the GRANTEE of a privilege on `namespace` (a securable)...
    grants.grantPrivilegeOnSecurableToRole(
        callCtx, role, null, namespace, PolarisPrivilege.TABLE_READ_DATA);
    // ...and simultaneously the SECURABLE of a usage grant to `principalRole`.
    grants.grantUsageOnRoleToGrantee(callCtx, catalog, role, principalRole);

    // As securable: only the usage-to-principalRole grant, never the one where it's the grantee.
    LoadGrantsResult onRoleAsSecurable = grants.loadGrantsOnSecurable(callCtx, role);
    assertThat(onRoleAsSecurable.getGrantRecords())
        .extracting(PolarisGrantRecord::getGranteeId, PolarisGrantRecord::getPrivilegeCode)
        .containsExactly(
            tuple(principalRole.getId(), PolarisPrivilege.CATALOG_ROLE_USAGE.getCode()));

    // As grantee: only the table-read grant on namespace, never the usage grant it holds.
    LoadGrantsResult toRoleAsGrantee = grants.loadGrantsToGrantee(callCtx, role);
    assertThat(toRoleAsGrantee.getGrantRecords())
        .extracting(PolarisGrantRecord::getSecurableId, PolarisGrantRecord::getPrivilegeCode)
        .containsExactly(tuple(namespace.getId(), PolarisPrivilege.TABLE_READ_DATA.getCode()));
  }
}
