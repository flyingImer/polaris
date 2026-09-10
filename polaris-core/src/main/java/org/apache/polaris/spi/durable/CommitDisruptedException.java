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

import org.apache.polaris.core.exceptions.PolarisException;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Infrastructure stopped a {@link DurableRecordStore#commit} from reporting an outcome at all.
 *
 * <p>This is not a rejected commit. A rejection is a {@link CommitResult}: the store reached a
 * verdict and named it. This exception says the store never got a verdict to report, so the caller
 * cannot read the absence of a result as "nothing applied".
 *
 * <p><b>What a caller may infer is exactly {@link #durableEffect()}, and nothing more.</b> A store
 * that can prove no mutation reached durable storage reports {@link DurableEffect#NONE}, and the
 * caller may then treat the commit as never having happened — retrying the identical request is
 * safe. A store that cannot prove that reports {@link DurableEffect#UNKNOWN}, and the caller may
 * not claim the mutations are absent. Reading nothing at all is the conservative direction: a
 * caller that ignores this distinction treats every disruption as {@code UNKNOWN}, which is never
 * wrong, only pessimistic.
 *
 * <p>The distinction is about durable effect, not about how far execution got. A store may well
 * have started writing and still prove that nothing survived — an aborted transaction is the
 * ordinary case — so "how far it got" is not the question a portable caller can act on.
 *
 * <p><b>A verdict a store has reached is never downgraded, and {@code UNKNOWN} is never overwritten
 * by {@code NONE}.</b> Once a store establishes what a commit left behind, a later failure on the
 * way out — cleaning up, returning a connection, anything after the fact — adds itself to that
 * verdict as a suppressed exception rather than replacing it. The rule matters because the two
 * mistakes are not symmetric: reporting {@code UNKNOWN} where {@code NONE} was provable costs a
 * caller a safe retry, while reporting {@code NONE} where the outcome was unknown invites it to act
 * as though a write that may be in storage is absent.
 */
public class CommitDisruptedException extends PolarisException {

  /** The stable identifier for this failure, independent of this class's name. */
  private static final String ERROR_CODE = "durable.commit_disrupted";

  /** What the store can say about the mutations' durable effect. */
  public enum DurableEffect {
    /**
     * The store proves no mutation reached durable storage. The commit can be treated as never
     * having happened, and the identical request may be retried.
     */
    NONE,
    /**
     * The store cannot prove either way. The mutations may or may not be in storage, so a caller
     * must not report them as absent, and must not retry as though nothing had happened.
     */
    UNKNOWN,
  }

  private final DurableEffect durableEffect;

  public CommitDisruptedException(@NonNull DurableEffect durableEffect, @NonNull String message) {
    this(durableEffect, message, null);
  }

  public CommitDisruptedException(
      @NonNull DurableEffect durableEffect, @NonNull String message, @Nullable Throwable cause) {
    super(message, cause);
    this.durableEffect = durableEffect;
  }

  /** What the store can say about the mutations' durable effect. Never null. */
  public @NonNull DurableEffect durableEffect() {
    return durableEffect;
  }

  /** The stable identifier for this failure, for a caller that reports rather than branches. */
  public @NonNull String errorCode() {
    return ERROR_CODE;
  }
}
