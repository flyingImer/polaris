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
package org.apache.polaris.extension.primitives.routing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import org.apache.polaris.core.persistence.DurableRecordStoreLocator;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.spi.durable.CommitResult;
import org.apache.polaris.spi.durable.DurableRecordStore;
import org.apache.polaris.spi.durable.LookupPath;
import org.apache.polaris.spi.durable.Mutation;
import org.apache.polaris.spi.durable.RecordKind;
import org.apache.polaris.spi.durable.RecordRef;
import org.apache.polaris.spi.durable.RecordVersions;
import org.jspecify.annotations.NonNull;

/**
 * The routing implementation of the primitives SPI: fronts several backends behind ONE {@link
 * DurableRecordStore} handle, resolving each request's record kind to the backend holding it
 * through a privately held {@link DurableRecordStoreLocator}. This is where kind-to-store routing
 * lives — nothing above the primitives layer can reach the locator, so manager and orchestration
 * hold one handle and never learn that stores exist.
 *
 * <p><b>One implementation instance, many atomicity domains.</b> Each backend keeps its own {@link
 * #domainOf} declaration, forwarded untouched, so this instance deliberately contains as many
 * domains as its backends declare (the shape C16/R1 always permitted; ADR-0014's 2026-08-18
 * amendment restates the containment rule as "a commit cannot span atomicity DOMAINS" for exactly
 * this class). Forwarding untouched is what keeps adjacent same-domain merging across kinds
 * working: two kinds on one backend report that backend's own domain value, compare equal, and
 * merge into one commit upstream. Two distinct backends can never collide in practice — the shipped
 * implementations declare identity-distinct domain objects — and a hypothetical third-party
 * collision degrades loudly, never silently: the merged group reaches {@link #commit}, whose
 * single-backend check refuses it as {@link CommitResult.Failure#DOMAIN_MISMATCH}.
 *
 * <p><b>{@code commit} requires one backend and never splits, never compensates.</b> Every
 * mutation's target kind must resolve to the same backend; otherwise the commit is refused with
 * {@code DOMAIN_MISMATCH} before anything is applied. Splitting a batch or compensating across
 * backends is orchestration's job, above this SPI — an implementation that quietly did either would
 * be making the exact silent semantic change S12 forbids. A precondition referencing a kind held by
 * a different backend is left to the holding backend, which rejects it loudly (it either holds no
 * mapper for the kind or declares a different domain); the orchestrator already refuses such
 * mutations before they reach any commit.
 *
 * <p><b>{@code generateNewId} delegates to one designated backend</b>, giving realm-wide id
 * uniqueness a single owner. The owner is a construction-time designation, part of the deployment's
 * assembly the same way the mapping is.
 *
 * <p><b>The kind-less union {@code list} concatenates the backends' answers.</b> Each backend
 * evaluates the path over the kinds it registers and answers with its own page — an empty page from
 * a backend that holds no matching data is a correct contribution, not an error. A backend that
 * declares no kind for the path rejects; if every backend rejects, the union rejects, naming the
 * path. What this class does NOT solve, disclosed rather than hidden: a paging token composed
 * across backends. If any backend answers with a next-page token, the union throws rather than
 * silently truncating; today no declared path spans backends with enough data to page, and the
 * cross-store paging token is a recorded open question, not a shipped mechanism.
 *
 * <p><b>{@code maxItemsPerCommit} declares the minimum across backends</b> — the one cap every
 * routed commit can honor. The known wart, declared rather than solved here: a commit routed to a
 * roomier backend could have carried more, and the holding backend remains the enforcer either way.
 *
 * <p>Temporary name, migration note: this class is named after {@link DurableRecordStore} and is
 * renamed with it at the contract step ({@code RoutingDurablePrimitives}, the recorded pairing).
 */
public class RoutingDurableRecordStore implements DurableRecordStore {

  private final DurableRecordStoreLocator locator;
  private final List<DurableRecordStore> backends;
  private final DurableRecordStore idGenerationOwner;
  private final int maxItemsPerCommit;

  /**
   * @param locator resolves each record kind to the backend holding it; held privately
   * @param backends every backend in this assembly, in a deterministic order (used for the union
   *     read and the cap declaration; the locator's single resolution method deliberately cannot
   *     enumerate, so the assembly site supplies the set it already holds)
   * @param idGenerationOwner the designated backend all id generation delegates to; must be one of
   *     {@code backends}
   */
  public RoutingDurableRecordStore(
      @NonNull DurableRecordStoreLocator locator,
      @NonNull Collection<DurableRecordStore> backends,
      @NonNull DurableRecordStore idGenerationOwner) {
    if (backends.isEmpty()) {
      throw new IllegalArgumentException("An assembly needs at least one backend");
    }
    List<DurableRecordStore> distinct = new ArrayList<>();
    for (DurableRecordStore backend : backends) {
      if (distinct.stream().noneMatch(b -> b == backend)) {
        distinct.add(backend);
      }
    }
    if (distinct.stream().noneMatch(b -> b == idGenerationOwner)) {
      throw new IllegalArgumentException(
          "The id-generation owner must be one of the assembly's backends");
    }
    this.locator = locator;
    this.backends = List.copyOf(distinct);
    this.idGenerationOwner = idGenerationOwner;
    this.maxItemsPerCommit =
        this.backends.stream().mapToInt(DurableRecordStore::maxItemsPerCommit).min().orElseThrow();
  }

  @Override
  public @NonNull CommitResult commit(@NonNull List<Mutation> mutations) {
    if (mutations.isEmpty()) {
      // No kind, no backend: nothing to route and nothing to apply. Vacuously applied; the
      // orchestrator never produces an empty group.
      return CommitResult.applied();
    }
    DurableRecordStore backend = locator.forKind(mutations.get(0).target().kind());
    for (Mutation mutation : mutations) {
      if (locator.forKind(mutation.target().kind()) != backend) {
        return CommitResult.domainMismatch();
      }
    }
    return backend.commit(mutations);
  }

  @Override
  public long generateNewId() {
    return idGenerationOwner.generateNewId();
  }

  @Override
  public @NonNull <T> Optional<T> get(@NonNull RecordRef ref, @NonNull Class<T> type) {
    return locator.forKind(ref.kind()).get(ref, type);
  }

  @Override
  public @NonNull <T> List<Optional<T>> getMany(
      @NonNull List<RecordRef> refs, @NonNull Class<T> type) {
    return scatterGather(refs, (backend, slice) -> backend.getMany(slice, type));
  }

  @Override
  public @NonNull <T> Page<T> list(
      @NonNull RecordKind kind,
      @NonNull LookupPath path,
      @NonNull List<Object> anchors,
      @NonNull PageToken pageToken,
      @NonNull Class<T> type) {
    return locator.forKind(kind).list(kind, path, anchors, pageToken, type);
  }

  @Override
  public @NonNull <T> Page<T> list(
      @NonNull LookupPath path,
      @NonNull List<Object> anchors,
      @NonNull PageToken pageToken,
      @NonNull Class<T> type) {
    List<T> union = new ArrayList<>();
    List<IllegalArgumentException> rejections = new ArrayList<>();
    boolean served = false;
    for (DurableRecordStore backend : backends) {
      Page<T> page;
      try {
        page = backend.list(path, anchors, pageToken, type);
      } catch (IllegalArgumentException e) {
        // This backend declares no kind for the path (or refuses these anchors); with a shared
        // kind vocabulary every backend validates the same way, so a genuine anchor error rejects
        // on all backends and surfaces below rather than being swallowed here.
        rejections.add(e);
        continue;
      }
      if (page.encodedResponseToken() != null) {
        throw new UnsupportedOperationException(
            "The union over lookup path '"
                + path.name()
                + "' needs a paging token composed across stores, which is a recorded open"
                + " question, not a shipped mechanism. Narrow the read or raise the page size.");
      }
      union.addAll(page.items());
      served = true;
    }
    if (!served) {
      IllegalArgumentException rejection =
          new IllegalArgumentException(
              "No registered kind declares lookup path '"
                  + path.name()
                  + "' on any store in this assembly");
      rejections.forEach(rejection::addSuppressed);
      throw rejection;
    }
    return Page.page(pageToken, union, null);
  }

  @Override
  public @NonNull List<Optional<RecordVersions>> versionsOf(@NonNull List<RecordRef> refs) {
    return scatterGather(refs, DurableRecordStore::versionsOf);
  }

  @Override
  public @NonNull Object domainOf(@NonNull RecordRef target) {
    return locator.forKind(target.kind()).domainOf(target);
  }

  @Override
  public int maxItemsPerCommit() {
    return maxItemsPerCommit;
  }

  /**
   * Routes each ref to its backend, issues one batched call per backend, and scatters the results
   * back into request order. Resolution runs first for every ref, so an unknown kind rejects the
   * whole read before any backend is asked, matching single-store behaviour.
   */
  private <R> List<R> scatterGather(
      List<RecordRef> refs, BiFunction<DurableRecordStore, List<RecordRef>, List<R>> read) {
    List<DurableRecordStore> resolved = new ArrayList<>(refs.size());
    for (RecordRef ref : refs) {
      resolved.add(locator.forKind(ref.kind()));
    }
    DurableRecordStore first = resolved.isEmpty() ? null : resolved.get(0);
    if (resolved.stream().allMatch(b -> b == first)) {
      return first == null ? List.of() : read.apply(first, refs);
    }
    Map<DurableRecordStore, List<Integer>> indexesByBackend = new LinkedHashMap<>();
    for (int i = 0; i < refs.size(); i++) {
      indexesByBackend.computeIfAbsent(resolved.get(i), b -> new ArrayList<>()).add(i);
    }
    List<R> out = new ArrayList<>(Collections.<R>nCopies(refs.size(), null));
    indexesByBackend.forEach(
        (backend, indexes) -> {
          List<RecordRef> slice = indexes.stream().map(refs::get).toList();
          List<R> answers = read.apply(backend, slice);
          for (int i = 0; i < indexes.size(); i++) {
            out.set(indexes.get(i), answers.get(i));
          }
        });
    return out;
  }
}
