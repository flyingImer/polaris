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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.collection.AttributeMap;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.exceptions.CommitConflictException;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest.ObservationState;
import org.apache.polaris.core.persistence.resolver.Resolvable;
import org.apache.polaris.core.persistence.resolver.ResolvedPathKey;
import org.apache.polaris.core.persistence.resolver.Resolver;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PolarisResolutionManifestLookupKeyTest {

  @Test
  void pathKeyLookupResolvesRegisteredPath() {
    Resolver resolver = Mockito.mock(Resolver.class);
    ResolverFactory resolverFactory = Mockito.mock(ResolverFactory.class);
    RealmContext realmContext = Mockito.mock(RealmContext.class);
    when(resolverFactory.createResolver(any(), anyString())).thenReturn(resolver);
    when(resolver.resolveAll()).thenReturn(new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS));
    when(resolver.getIsPassthroughFacade()).thenReturn(false);

    ResolvedPolarisEntity referenceCatalog = Mockito.mock(ResolvedPolarisEntity.class);
    ResolvedPolarisEntity resolvedLeaf = Mockito.mock(ResolvedPolarisEntity.class);
    when(resolver.getResolvedReferenceCatalog()).thenReturn(referenceCatalog);
    when(resolver.getResolvedPaths()).thenReturn(List.of(List.of(resolvedLeaf)));

    PolarisResolutionManifest manifest =
        new PolarisResolutionManifest(
            new PolarisDefaultDiagServiceImpl(),
            realmContext,
            resolverFactory,
            PolarisPrincipal.of("p", AttributeMap.EMPTY, Set.of()),
            "catalog");

    ResolverPath tablePath = new ResolverPath(List.of("ns1", "tbl1"), PolarisEntityType.TABLE_LIKE);
    manifest.addPath(tablePath);
    manifest.resolveAll();

    var byKey = manifest.getResolvedPath(ResolvedPathKey.of(tablePath), false);
    assertThat(byKey.getResolvedFullPath()).containsExactly(referenceCatalog, resolvedLeaf);
  }

  @Test
  void passthroughResolvedPathResolvesOnlyRequestedPathsNotFullGraph() {
    Resolver resolver = Mockito.mock(Resolver.class);
    ResolverFactory resolverFactory = Mockito.mock(ResolverFactory.class);
    RealmContext realmContext = Mockito.mock(RealmContext.class);
    when(resolverFactory.createResolver(any(), anyString())).thenReturn(resolver);
    when(resolver.resolveSelections(Set.of(Resolvable.REQUESTED_PATHS)))
        .thenReturn(new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS));

    ResolvedPolarisEntity referenceCatalog = resolved("catalog", PolarisEntityType.CATALOG, 1);
    ResolvedPolarisEntity resolvedLeaf = resolved("tbl1", PolarisEntityType.TABLE_LIKE, 1);
    when(resolver.getResolvedReferenceCatalog()).thenReturn(referenceCatalog);
    when(resolver.getResolvedPath()).thenReturn(List.of(resolvedLeaf));

    PolarisResolutionManifest manifest =
        new PolarisResolutionManifest(
            new PolarisDefaultDiagServiceImpl(),
            realmContext,
            resolverFactory,
            PolarisPrincipal.of("p", AttributeMap.EMPTY, Set.of()),
            "catalog");

    ResolverPath tablePath = new ResolverPath(List.of("tbl1"), PolarisEntityType.TABLE_LIKE);
    manifest.addPassthroughPath(tablePath);

    var resolved = manifest.getPassthroughResolvedPath(ResolvedPathKey.of(tablePath));

    assertThat(resolved.getResolvedFullPath()).containsExactly(referenceCatalog, resolvedLeaf);
    verify(resolver).resolveSelections(Set.of(Resolvable.REQUESTED_PATHS));
    verify(resolver, never()).resolveAll();
  }

  @Test
  void observationDistinguishesUnresolvedFromConfirmedAbsence() {
    Resolver resolver = Mockito.mock(Resolver.class);
    when(resolver.resolveAll()).thenReturn(new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS));
    when(resolver.getResolvedReferenceCatalog())
        .thenReturn(resolved("catalog", PolarisEntityType.CATALOG, 1));
    when(resolver.getResolvedPaths()).thenReturn(List.of(List.of()));
    var manifest = manifest(resolver);
    var key = ResolvedPathKey.of(List.of("missing"), PolarisEntityType.TABLE_LIKE);
    assertThat(manifest.observePath(key).state()).isEqualTo(ObservationState.UNRESOLVED);
    manifest.addPath(new ResolverPath(key, true));
    assertThat(manifest.observePath(key).state()).isEqualTo(ObservationState.UNRESOLVED);
    manifest.resolveAll();
    var observation = manifest.observePath(key);
    assertThat(observation.state()).isEqualTo(ObservationState.ABSENT);
    assertThat(observation.snapshot()).hasSize(1);
    assertThat(observation.snapshot().get(0).getName()).isEqualTo("catalog");
  }

  @Test
  void passthroughObservationsKeepTheirOwnSnapshot() {
    Resolver resolver = Mockito.mock(Resolver.class);
    when(resolver.resolveSelections(Set.of(Resolvable.REQUESTED_PATHS)))
        .thenReturn(new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS));
    when(resolver.getResolvedReferenceCatalog())
        .thenReturn(resolved("catalog", PolarisEntityType.CATALOG, 1));
    when(resolver.getResolvedPath())
        .thenReturn(List.of(resolved("table", PolarisEntityType.TABLE_LIKE, 7)))
        .thenReturn(List.of(resolved("table", PolarisEntityType.TABLE_LIKE, 8)));
    var manifest = manifest(resolver);
    var key = ResolvedPathKey.of(List.of("table"), PolarisEntityType.TABLE_LIKE);
    manifest.addPassthroughPath(new ResolverPath(key, true));
    var validated = manifest.observePassthroughPath(key);
    var later = manifest.observePassthroughPath(key);
    assertThat(validated.state()).isEqualTo(ObservationState.FOUND);
    assertThat(validated.snapshot().get(validated.snapshot().size() - 1).getEntityVersion())
        .isEqualTo(7);
    assertThat(later.snapshot().get(later.snapshot().size() - 1).getEntityVersion()).isEqualTo(8);
    assertThat(validated.snapshot().get(validated.snapshot().size() - 1).getEntityVersion())
        .isEqualTo(7);
  }

  @Test
  void failedPassthroughResolutionIsNotAbsence() {
    Resolver resolver = Mockito.mock(Resolver.class);
    when(resolver.resolveSelections(Set.of(Resolvable.REQUESTED_PATHS)))
        .thenReturn(new ResolverStatus(ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED));
    var manifest = manifest(resolver);
    var key = ResolvedPathKey.of(List.of("table"), PolarisEntityType.TABLE_LIKE);
    manifest.addPassthroughPath(new ResolverPath(key, true));
    assertThat(manifest.observePassthroughPath(key).state()).isEqualTo(ObservationState.UNRESOLVED);
    assertThatThrownBy(() -> manifest.getPassthroughResolvedPath(key))
        .isInstanceOf(CommitConflictException.class);
    when(resolver.getIsPassthroughFacade()).thenReturn(true);
    assertThatThrownBy(() -> manifest.getPassthroughResolvedPath(key))
        .isInstanceOf(CommitConflictException.class);
  }

  private static PolarisResolutionManifest manifest(Resolver resolver) {
    return new PolarisResolutionManifest(
        new PolarisDefaultDiagServiceImpl(),
        () -> "test",
        (principal, catalog) -> resolver,
        PolarisPrincipal.of("p", AttributeMap.EMPTY, Set.of()),
        "catalog");
  }

  private static ResolvedPolarisEntity resolved(String name, PolarisEntityType type, int version) {
    var entity =
        new PolarisBaseEntity.Builder(
                new PolarisBaseEntity(
                    0, type.getCode(), type, PolarisEntitySubType.NULL_SUBTYPE, 0, name))
            .entityVersion(version)
            .build();
    return new ResolvedPolarisEntity(
        new PolarisDefaultDiagServiceImpl(), entity, List.of(), entity.getGrantRecordsVersion());
  }

  @Test
  void pathKeyLookupFailsForUnregisteredPath() {
    Resolver resolver = Mockito.mock(Resolver.class);
    ResolverFactory resolverFactory = Mockito.mock(ResolverFactory.class);
    RealmContext realmContext = Mockito.mock(RealmContext.class);
    when(resolverFactory.createResolver(any(), anyString())).thenReturn(resolver);
    when(resolver.resolveAll()).thenReturn(new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS));
    when(resolver.getIsPassthroughFacade()).thenReturn(false);
    when(resolver.getResolvedReferenceCatalog())
        .thenReturn(Mockito.mock(ResolvedPolarisEntity.class));
    when(resolver.getResolvedPaths()).thenReturn(List.of());

    PolarisResolutionManifest manifest =
        new PolarisResolutionManifest(
            new PolarisDefaultDiagServiceImpl(),
            realmContext,
            resolverFactory,
            PolarisPrincipal.of("p", AttributeMap.EMPTY, Set.of()),
            "catalog");
    manifest.resolveAll();

    assertThatThrownBy(
            () ->
                manifest.getResolvedPath(
                    ResolvedPathKey.of(List.of("does_not_exist"), PolarisEntityType.NAMESPACE),
                    false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("never_registered_key_for_resolved_path");
  }

  @Test
  void pathKeyLookupUsesLastRegistrationForDuplicateCanonicalPath() {
    Resolver resolver = Mockito.mock(Resolver.class);
    ResolverFactory resolverFactory = Mockito.mock(ResolverFactory.class);
    RealmContext realmContext = Mockito.mock(RealmContext.class);
    when(resolverFactory.createResolver(any(), anyString())).thenReturn(resolver);
    when(resolver.resolveAll()).thenReturn(new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS));
    when(resolver.getIsPassthroughFacade()).thenReturn(false);

    ResolvedPolarisEntity resolvedLeaf = Mockito.mock(ResolvedPolarisEntity.class);
    when(resolver.getResolvedReferenceCatalog())
        .thenReturn(Mockito.mock(ResolvedPolarisEntity.class));
    when(resolver.getResolvedPaths())
        .thenReturn(List.of(List.of(resolvedLeaf), List.of(resolvedLeaf)));

    PolarisResolutionManifest manifest =
        new PolarisResolutionManifest(
            new PolarisDefaultDiagServiceImpl(),
            realmContext,
            resolverFactory,
            PolarisPrincipal.of("p", AttributeMap.EMPTY, Set.of()),
            "catalog");

    ResolverPath nonOptionalPath =
        new ResolverPath(List.of("ns1", "tbl1"), PolarisEntityType.TABLE_LIKE, false);
    ResolverPath optionalPath =
        new ResolverPath(List.of("ns1", "tbl1"), PolarisEntityType.TABLE_LIKE, true);

    manifest.addPath(nonOptionalPath);
    manifest.addPath(optionalPath);
    manifest.resolveAll();

    assertThat(manifest.getResolvedPath(ResolvedPathKey.of(optionalPath), false)).isNull();
  }
}
