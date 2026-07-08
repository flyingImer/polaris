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
package org.apache.polaris.core.persistence.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.junit.jupiter.api.Test;

/**
 * Verifies the {@link DefaultEntityResolver} adapter's translation logic with the underlying {@link
 * Resolver} engine mocked (no metastore needed): request paths -> {@code addPath} in order,
 * resolver outputs -> request-addressable {@link ResolutionResult}, and the empty-paths /
 * non-success guards.
 */
class DefaultEntityResolverTest {

  private record TestName(PolarisEntityType entityType, String entityName, boolean optional)
      implements ResolverEntityName {}

  private DefaultEntityResolver newResolver(Resolver mockResolver) {
    ResolverFactory factory = mock(ResolverFactory.class);
    when(factory.createResolver(any(), any())).thenReturn(mockResolver);
    return new DefaultEntityResolver(factory);
  }

  @Test
  void resolvesPathsAndMapsThemByKeyInAddOrder() {
    Resolver resolver = mock(Resolver.class);
    when(resolver.resolveAll()).thenReturn(new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS));

    ResolvedPolarisEntity leafA = mock(ResolvedPolarisEntity.class);
    ResolvedPolarisEntity leafB = mock(ResolvedPolarisEntity.class);
    ResolvedPolarisEntity principal = mock(ResolvedPolarisEntity.class);
    when(resolver.getResolvedPaths()).thenReturn(List.of(List.of(leafA), List.of(leafB)));
    when(resolver.getResolvedCallerPrincipal()).thenReturn(principal);
    when(resolver.getResolvedCallerPrincipalRoles()).thenReturn(List.of());
    when(resolver.getResolvedReferenceCatalog()).thenReturn(null);
    when(resolver.getResolvedCatalogRoles()).thenReturn(null);

    ResolverPath pathA = new ResolverPath(List.of("ns", "tblA"), PolarisEntityType.TABLE_LIKE);
    ResolverPath pathB = new ResolverPath(List.of("ns", "tblB"), PolarisEntityType.TABLE_LIKE);
    ResolutionRequest request =
        ResolutionRequest.ofPaths(mock(PolarisPrincipal.class), "cat", List.of(pathA, pathB));

    ResolutionResult result = newResolver(resolver).resolve(request);

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.resolvedCallerPrincipal()).isSameAs(principal);
    // Each requested key maps to its resolved path, aligned with add order.
    assertThat(result.resolvedPath(pathA.key())).containsExactly(leafA);
    assertThat(result.resolvedPath(pathB.key())).containsExactly(leafB);

    verify(resolver).addPath(pathA);
    verify(resolver).addPath(pathB);
  }

  @Test
  void addsTopLevelNamesAndMapsResolvedEntityByTypeAndName() {
    Resolver resolver = mock(Resolver.class);
    when(resolver.resolveAll()).thenReturn(new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS));
    when(resolver.getResolvedCallerPrincipal()).thenReturn(mock(ResolvedPolarisEntity.class));
    when(resolver.getResolvedCallerPrincipalRoles()).thenReturn(List.of());
    when(resolver.getResolvedReferenceCatalog()).thenReturn(null);
    when(resolver.getResolvedCatalogRoles()).thenReturn(null);
    ResolvedPolarisEntity role = mock(ResolvedPolarisEntity.class);
    when(resolver.getResolvedEntity(PolarisEntityType.CATALOG_ROLE, "admin")).thenReturn(role);

    ResolutionRequest request =
        new ResolutionRequest(
            mock(PolarisPrincipal.class),
            "cat",
            List.of(),
            List.of(new TestName(PolarisEntityType.CATALOG_ROLE, "admin", false)));

    ResolutionResult result = newResolver(resolver).resolve(request);

    verify(resolver).addEntityByName(PolarisEntityType.CATALOG_ROLE, "admin");
    assertThat(result.resolvedTopLevelEntity(PolarisEntityType.CATALOG_ROLE, "admin"))
        .isSameAs(role);
    // No paths were added, so the positional path accessor must not be touched (it asserts >=1).
    verify(resolver, never()).getResolvedPaths();
  }

  @Test
  void nonSuccessStatusReturnsFailureResultWithoutReadingPaths() {
    Resolver resolver = mock(Resolver.class);
    when(resolver.resolveAll())
        .thenReturn(new ResolverStatus(ResolverStatus.StatusEnum.ENTITY_COULD_NOT_BE_RESOLVED));

    ResolverPath path = new ResolverPath(List.of("ns", "tbl"), PolarisEntityType.TABLE_LIKE);
    ResolutionRequest request =
        ResolutionRequest.ofPaths(mock(PolarisPrincipal.class), "cat", List.of(path));

    ResolutionResult result = newResolver(resolver).resolve(request);

    assertThat(result.isSuccess()).isFalse();
    assertThat(result.resolvedPaths()).isEmpty();
    assertThat(result.resolvedPath(path.key())).isNull();
    verify(resolver, never()).getResolvedPaths();
  }

  @Test
  void emptyRequestResolvesPrincipalOnlyWithoutReadingPaths() {
    Resolver resolver = mock(Resolver.class);
    when(resolver.resolveAll()).thenReturn(new ResolverStatus(ResolverStatus.StatusEnum.SUCCESS));
    ResolvedPolarisEntity principal = mock(ResolvedPolarisEntity.class);
    when(resolver.getResolvedCallerPrincipal()).thenReturn(principal);
    when(resolver.getResolvedCallerPrincipalRoles()).thenReturn(List.of());
    when(resolver.getResolvedReferenceCatalog()).thenReturn(null);
    when(resolver.getResolvedCatalogRoles()).thenReturn(null);

    ResolutionResult result =
        newResolver(resolver).resolve(ResolutionRequest.of(mock(PolarisPrincipal.class), null));

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.resolvedPaths()).isEmpty();
    assertThat(result.resolvedCallerPrincipal()).isSameAs(principal);
    verify(resolver, never()).getResolvedPaths();
  }
}
