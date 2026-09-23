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

"""Run isolated native experiments. No existing database or credentials are used."""
import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone

from adapters import FdbStore, SpannerStore, SqlStore, spanner_schema
from scenarios import run_scenarios


ROOT = Path(__file__).resolve().parent
BACKENDS = ("postgres", "cockroach", "fdb", "spanner")


class BackendUnavailable(Exception):
    pass


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_port(number, process):
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("server exited: " + str(process.returncode))
        try:
            with socket.create_connection(("127.0.0.1", number), timeout=.2):
                return
        except OSError:
            time.sleep(.1)
    raise TimeoutError("local server did not start")


@contextmanager
def local_store(backend, home, work):
    processes, logs = [], []
    pg = None
    # Native runtimes may link cloud SDKs even in a local configuration. No cloud
    # credential discovery is needed for this experiment, including AWS IMDS.
    empty_config = work / "empty-cloud-config"
    empty_config.write_text("")
    os.environ.update(AWS_EC2_METADATA_DISABLED="true", AWS_REGION="us-east-1",
                      AWS_SHARED_CREDENTIALS_FILE=str(empty_config), AWS_CONFIG_FILE=str(empty_config))

    def start(command, number):
        log = (work / "server.log").open("w")
        logs.append(log)
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        processes.append(process)
        wait_port(number, process)
        return process

    try:
        if backend == "postgres":
            if os.geteuid() == 0:
                raise BackendUnavailable("PostgreSQL requires a non-root OS user. This runner does not create users or change process credentials; run it as your ordinary development user.")
            import pgserver
            pg = pgserver.get_server(work / "pg", cleanup_mode="stop")
            yield SqlStore(pg.get_uri())
        elif backend == "cockroach":
            # Disable optional reporting before server startup: its hardware
            # collector otherwise probes cloud metadata even for a local node.
            os.environ.update(COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING="true",
                              COCKROACH_CRASH_REPORTS="", COCKROACH_UPDATE_CHECK_URL="",
                              COCKROACH_USAGE_REPORT_URL="")
            number = port()
            start([str(home / "cockroach"), "start-single-node", "--insecure",
                   "--listen-addr=127.0.0.1:" + str(number), "--http-addr=127.0.0.1:" + str(port()),
                   "--store=" + str(work / "crdb"), "--cache=128MiB", "--max-sql-memory=128MiB"], number)
            yield SqlStore(f"postgresql://root@127.0.0.1:{number}/defaultdb?sslmode=disable")
        elif backend == "fdb":
            number = port()
            (work / "logs").mkdir()
            cluster = work / "fdb.cluster"
            cluster.write_text(f"prototype:prototype@127.0.0.1:{number}\n")
            start([str(home / "fdbserver.x86_64"), "--cluster-file", str(cluster),
                   "--datadir", str(work / "data"), "--logdir", str(work / "logs"),
                   "--public-address", f"127.0.0.1:{number}", "--listen-address", f"127.0.0.1:{number}"], number)
            subprocess.run([str(home / "fdbcli.x86_64"), "-C", str(cluster), "--exec",
                            "configure new single memory", "--timeout", "25"], check=True,
                           capture_output=True, text=True, timeout=30)
            yield FdbStore(cluster)
        elif backend == "spanner":
            number = port()
            os.environ["SPANNER_EMULATOR_HOST"] = f"127.0.0.1:{number}"
            # Local-only emulator: disable environment/credential discovery.
            # Defensive host overrides keep library metadata discovery local too.
            os.environ.update(NO_GCE_CHECK="true", GCE_METADATA_HOST="127.0.0.1:9",
                              GCE_METADATA_IP="127.0.0.1:9", GCE_METADATA_ROOT="127.0.0.1:9",
                              GRPC_GCE_METADATA_HOST="127.0.0.1:9", SPANNER_DISABLE_BUILTIN_METRICS="true")
            start([str(home / "emulator_main"), f"--host_port=127.0.0.1:{number}"], number)
            from google.auth.credentials import AnonymousCredentials
            from google.cloud import spanner
            client = spanner.Client(project="polaris-prototype", credentials=AnonymousCredentials(), disable_builtin_metrics=True)
            instance = client.instance("prototype", configuration_name="projects/polaris-prototype/instanceConfigs/emulator-config", node_count=1)
            instance.create().result(timeout=30)
            database = instance.database("prototype", ddl_statements=spanner_schema())
            database.create().result(timeout=30)
            yield SpannerStore(database)
    finally:
        if pg is not None:
            pg.cleanup()
        for process in processes:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in logs:
            log.close()


def worker(args):
    result = {"backend": args.worker, "status": "failed",
              "source_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(ROOT.glob("*.py"))}}
    with tempfile.TemporaryDirectory(prefix="polaris-native-") as directory:
        try:
            with local_store(args.worker, args.native_home, Path(directory)) as store:
                result.update(version=store.version, scenarios=run_scenarios(store, args.worker), status="passed")
        except BackendUnavailable as error:
            result.update(status="blocked", error=str(error))
        except Exception as error:
            result.update(error=str(error), traceback=traceback.format_exc())
            log = Path(directory) / "server.log"
            if log.exists():
                result["server_log_tail"] = log.read_text(errors="replace")[-6000:]
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: result[key] for key in ("backend", "status")}), flush=True)
    return 0 if result["status"] == "passed" else 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--native-home", type=Path, default=ROOT / ".native")
    parser.add_argument("--output", type=Path, default=ROOT.parent / "results" / "native-results.json")
    parser.add_argument("--backends", nargs="+", choices=BACKENDS, default=list(BACKENDS))
    parser.add_argument("--worker", choices=BACKENDS, help=argparse.SUPPRESS)
    args = parser.parse_args()
    args.native_home = args.native_home.resolve()
    args.output = args.output.resolve()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.worker:
        return worker(args)
    receipt = {
        "status": "running",
        "scope": "Python extraction of bounded metadata workflows; not a Polaris Java backend",
        "time_utc": datetime.now(timezone.utc).isoformat(),
        "polaris_base": "f68699517cac761f9240f71279f26e4850b7d0c8",
        "source_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(ROOT.glob("*.py"))},
        "packages": {p: importlib.metadata.version(p) for p in ("foundationdb", "google-cloud-spanner", "psycopg", "pgserver")},
        "backends": [],
    }
    downloads = args.native_home / "downloads.json"
    if downloads.exists():
        receipt["downloads"] = json.loads(downloads.read_text())
    with tempfile.TemporaryDirectory(prefix="polaris-native-receipts-") as directory:
        for backend in args.backends:
            output = Path(directory) / (backend + ".json")
            env = dict(os.environ)
            env["LD_LIBRARY_PATH"] = str(args.native_home) + (":" + env["LD_LIBRARY_PATH"] if env.get("LD_LIBRARY_PATH") else "")
            completed = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--worker", backend,
                                        "--native-home", str(args.native_home), "--output", str(output)], env=env)
            receipt["backends"].append(json.loads(output.read_text()) if output.exists() else {
                "backend": backend, "status": "failed", "exit_code": completed.returncode})
            # Preserve completed backend evidence even if a later runtime is interrupted.
            args.output.write_text(json.dumps(receipt, indent=2) + "\n")
    successful = [b for b in receipt["backends"] if b["status"] == "passed"]
    # Compare the actual durable records, not only per-backend pass flags.
    cross = []
    if len(successful) > 1:
        baseline = successful[0]
        for scenario in baseline["scenarios"]:
            if "state" not in scenario or scenario["scenario"] in (
                "namespace child-create versus delete", "Manager retry reloads unchanged parent",
                "negative control: unprotected FDB range"):
                continue
            same = all(next(s for s in backend["scenarios"] if s["scenario"] == scenario["scenario"])["state"] == scenario["state"] for backend in successful[1:])
            cross.append({"scenario": scenario["scenario"], "identical_published_records": same})
    receipt["cross_backend_comparison"] = cross
    if all(c["identical_published_records"] for c in cross) and all(b["status"] != "failed" for b in receipt["backends"]):
        receipt["status"] = "passed" if len(successful) == len(args.backends) else "partial"
    else:
        receipt["status"] = "failed"
    args.output.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({"status": receipt["status"], "receipt": str(args.output)}), flush=True)
    return 0 if receipt["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
