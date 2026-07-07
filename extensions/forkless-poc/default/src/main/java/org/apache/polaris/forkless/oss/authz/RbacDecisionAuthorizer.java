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
package org.apache.polaris.forkless.oss.authz;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.polaris.authz.model.AuthorizationDecision;
import org.apache.polaris.authz.model.AuthorizationEvaluationException;
import org.apache.polaris.authz.model.AuthorizationIntent;
import org.apache.polaris.authz.model.AuthorizationRequest;
import org.apache.polaris.authz.model.PolarisPrincipalRef;
import org.apache.polaris.spi.substrate.PolarisDecisionAuthorizer;

/**
 * OSS-default authorizer that decides directly from an in-memory set of grants. It AND-combines the
 * intents of a request and returns on the first deny. Intents are validated and evaluated one at a
 * time, so a short-circuit on an early deny never touches a later intent.
 */
public class RbacDecisionAuthorizer implements PolarisDecisionAuthorizer {

  /**
   * A single grant: {@code subject} (a principal name or a role name) may exercise {@code
   * privilege} on {@code path}. A targetless grant uses an empty path.
   */
  public record Grant(String subject, String privilege, List<String> path) {
    public Grant {
      Objects.requireNonNull(subject, "subject must be non-null");
      Objects.requireNonNull(privilege, "privilege must be non-null");
      path = List.copyOf(path);
    }
  }

  private final Set<Grant> grants;

  public RbacDecisionAuthorizer(Set<Grant> grants) {
    this.grants = Set.copyOf(grants);
  }

  @Override
  public AuthorizationDecision authorize(AuthorizationRequest request) {
    if (request == null) {
      throw new AuthorizationEvaluationException("request must be non-null");
    }
    PolarisPrincipalRef principal = request.principal();
    for (AuthorizationIntent intent : request.intents()) {
      AuthorizationDecision decision = decideOne(principal, intent);
      if (!decision.isAllowed()) {
        return decision; // short-circuit: one deny denies the whole batch
      }
    }
    return AuthorizationDecision.allow();
  }

  private AuthorizationDecision decideOne(
      PolarisPrincipalRef principal, AuthorizationIntent intent) {
    String privilege = intent.privilege();
    if (privilege.isBlank()) {
      throw new AuthorizationEvaluationException("intent has a blank privilege");
    }
    if (intent instanceof AuthorizationIntent.SingleTarget single) {
      return requireGrant(principal, privilege, single.securablePath());
    }
    if (intent instanceof AuthorizationIntent.Rename rename) {
      if (!isGranted(principal, privilege, rename.fromPath())) {
        return AuthorizationDecision.deny(denyMessage(principal, privilege, rename.fromPath()));
      }
      return requireGrant(principal, privilege, rename.toPath());
    }
    if (intent instanceof AuthorizationIntent.Targetless) {
      return requireGrant(principal, privilege, List.of());
    }
    throw new AuthorizationEvaluationException(
        "unknown intent type: " + intent.getClass().getName());
  }

  private AuthorizationDecision requireGrant(
      PolarisPrincipalRef principal, String privilege, List<String> path) {
    return isGranted(principal, privilege, path)
        ? AuthorizationDecision.allow()
        : AuthorizationDecision.deny(denyMessage(principal, privilege, path));
  }

  private boolean isGranted(PolarisPrincipalRef principal, String privilege, List<String> path) {
    for (Grant grant : grants) {
      if (grant.privilege().equals(privilege)
          && grant.path().equals(path)
          && subjectMatches(principal, grant.subject())) {
        return true;
      }
    }
    return false;
  }

  private static boolean subjectMatches(PolarisPrincipalRef principal, String subject) {
    return subject.equals(principal.principalName()) || principal.roleNames().contains(subject);
  }

  private static String denyMessage(
      PolarisPrincipalRef principal, String privilege, List<String> path) {
    return "principal '" + principal.principalName() + "' lacks '" + privilege + "' on " + path;
  }
}
