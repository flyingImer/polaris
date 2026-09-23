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

"""Attempt lifecycle and outcomes, intentionally independent of domain operations."""
from collections import Counter


class Conflict(Exception):
    """Native storage confirms this attempt did not commit and can be retried."""


class Unknown(Exception):
    """Publication may have occurred. Ordinary retry is forbidden."""


class Capacity(Exception):
    pass


class InjectedAbort(Exception):
    pass


class Attempt:
    def __init__(self, *, fail_after=None, lose_ack=False, capacity=None, unsafe_empty=False):
        self.state = "OPEN"
        self.events = []
        self.fail_after, self.lose_ack = fail_after, lose_ack
        self.capacity, self.unsafe_empty = capacity, unsafe_empty

    def record(self, event, **detail):
        self.events.append({"event": event, **detail})

    def require_open(self):
        if self.state != "OPEN":
            raise RuntimeError("attempt is terminal: " + self.state)

    def commit(self, changes):
        self.require_open()
        if self.capacity is not None and len(changes) > self.capacity:
            self.state = "REJECTED"
            raise Capacity("injected adapter capacity; not a database's real limit")
        self.state = "APPLYING"
        try:
            for number, change in enumerate(changes, 1):
                self._apply(change)
                if number == self.fail_after:
                    raise InjectedAbort("injected failure after native staging, before commit")
            self.state = "COMMITTING"
            self.record("commit")
            self._commit()
            if self.lose_ack:
                # Suppress a confirmed native result; this is NOT a network fault injector.
                raise Unknown("injected loss of acknowledgement after native success")
            self.state = "COMMITTED"
        except Exception as error:
            outcome = self._classify(error)
            self.state = "UNKNOWN" if isinstance(outcome, Unknown) else "ABORTED"
            raise outcome from (error if outcome is not error else None)

    def _classify(self, error):
        return error

    def summary(self):
        return {"state": self.state, "calls": dict(Counter(x["event"] for x in self.events))}

    def close(self):
        if self.state not in ("COMMITTED", "UNKNOWN"):
            self._rollback()
            if self.state == "OPEN":
                self.state = "ABORTED"
        self._close()
