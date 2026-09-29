#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Disposable PoC CI runners only. Do not alter a workstation's firewall.
set -euo pipefail
[[ "${GITHUB_ACTIONS:-}" == true ]]
[[ "${RUNNER_ENVIRONMENT:-}" == github-hosted ]]

if ! command -v nft >/dev/null; then
  sudo apt-get update -qq
  sudo apt-get install -y nftables
fi

# A separate table leaves Docker's rules intact. These hooks also protect
# host-network containers. Setup failure aborts before any database starts.
sudo nft add table inet polaris_poc_guard
for chain in output forward; do
  sudo nft add chain inet polaris_poc_guard "$chain" \
    "{ type filter hook $chain priority -150; policy accept; }"
  sudo nft add counter inet polaris_poc_guard "${chain}_denied"
  sudo nft add rule inet polaris_poc_guard "$chain" \
    ip daddr '{ 169.254.0.0/16, 100.100.100.200 }' \
    counter name "${chain}_denied" reject with icmpx type admin-prohibited
  sudo nft add rule inet polaris_poc_guard "$chain" \
    ip6 daddr '{ fc00::/7, fe80::/10 }' \
    counter name "${chain}_denied" reject with icmpx type admin-prohibited
done

# No HTTP requests, tokens, headers or response bodies. Run only after every
# deny rule was installed. A failed connection alone is not evidence: require
# the corresponding firewall counter to advance for both execution paths.
probe() {
  "$@" - <<'PY'
import errno
import json
import socket

results = []
for address in ("169.254.169.254", "169.254.170.2", "100.100.100.200", "fd00:ec2::254"):
    family = socket.AF_INET6 if ":" in address else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as connection:
        connection.settimeout(3)
        result = connection.connect_ex((address, 80))
    allowed_errors = {errno.EACCES, errno.EPERM, errno.EHOSTUNREACH, errno.ENETUNREACH}
    if result not in allowed_errors:
        raise SystemExit(f"Expected explicit network denial for {address}, got errno {result}")
    results.append({"address": address, "errno": result, "sent_application_data": False})
print(json.dumps(results))
PY
}

probe python3
probe docker run --rm --interactive --network bridge --cap-drop ALL \
  --security-opt no-new-privileges python:3.12-alpine python3

for chain in output forward; do
  sudo nft -j list counter inet polaris_poc_guard "${chain}_denied" |
    python3 -c 'import json,sys; counters=[x["counter"] for x in json.load(sys.stdin)["nftables"] if "counter" in x]; assert len(counters)==1 and counters[0]["packets"]>=3, counters; print(json.dumps(counters[0]))'
done
sudo nft list table inet polaris_poc_guard
