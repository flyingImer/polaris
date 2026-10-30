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
import pathlib
import sys
import xml.etree.ElementTree as ET


def verify(directory, suffix, required, minimum):
    reports = list(pathlib.Path(directory).glob("TEST-*" + suffix + ".xml"))
    assert len(reports) == 1, f"Expected one {suffix} report, got {reports}"
    cases = ET.parse(reports[0]).getroot().findall("testcase")
    assert len(cases) >= minimum, f"Missing {suffix} cases"
    names = {case.attrib["name"].split("(", 1)[0] for case in cases}
    assert set(required) <= names, f"Missing required cases: {set(required) - names}"
    assert all(case.find(tag) is None for case in cases for tag in ("skipped", "failure", "error")), f"Incomplete {suffix} evidence"


if sys.argv[1] == "service":
    verify("runtime/service/build/test-results/test", "PrimitiveCommitDependencyTest",
           ["nativeReciprocalRequirementsCannotBothSucceed",
            "passthroughSnapshotChangedBeforeCommit",
            "requirementOnlyTableRetainsEntityAndMetadata",
            "repeatedTableRequestsCoalesceIntoOneMutation"], 10)
else:
    directory = "persistence/primitives/build/test-results/test"
    verify(directory, "ManagerFailureTest",
           ["reciprocalCheckOnlyDependenciesCannotBothCommit",
            "staleCheckOnlyDependencyRejectsWholeBatch",
            "checkOnlyDependencyPublishesNoMutation",
            "duplicateMutationIsInvalidWithoutPartialWrites",
            "detachUsesIdentityAfterUnrelatedMetadataChanges"], 5)
    verify(directory, "AttemptContractTest", ["oneWayDependencyMaySerializeBeforeEarlierPhysicalCommit"], 1)
    verify(directory, "ConflictGranularityTest", [], 8)
    assert len(list(pathlib.Path("persistence/primitives/build/reports/primitives-contention").glob("*.json"))) == 8
