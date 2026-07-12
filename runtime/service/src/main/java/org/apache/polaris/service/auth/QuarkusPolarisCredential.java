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
package org.apache.polaris.service.auth;

import io.quarkus.security.credential.Credential;
import java.util.Set;
import org.apache.polaris.core.auth.PolarisCredential;
import org.jspecify.annotations.Nullable;

/**
 * Adapts a framework-agnostic {@link PolarisCredential} into a Quarkus Security {@link
 * Credential} so it can be stored on and retrieved from a {@code SecurityIdentity}.
 *
 * <p>Not {@code @PolarisImmutable}: deliberately hand-written rather than a second
 * Immutables-generated type, so that {@link PolarisCredential#getToken()}'s {@code
 * @Value.Redacted} contract isn't put at risk of not propagating across the annotation processor,
 * and this class's {@code toString()} stays the safe default (no fields).
 */
public final class QuarkusPolarisCredential implements PolarisCredential, Credential {

  private final PolarisCredential delegate;

  private QuarkusPolarisCredential(PolarisCredential delegate) {
    this.delegate = delegate;
  }

  public static QuarkusPolarisCredential of(PolarisCredential delegate) {
    return new QuarkusPolarisCredential(delegate);
  }

  @Override
  public @Nullable Long getPrincipalId() {
    return delegate.getPrincipalId();
  }

  @Override
  public @Nullable String getPrincipalName() {
    return delegate.getPrincipalName();
  }

  @Override
  public Set<String> getPrincipalRoles() {
    return delegate.getPrincipalRoles();
  }

  @Override
  public @Nullable String getToken() {
    return delegate.getToken();
  }
}
