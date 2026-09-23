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

"""Pinned disposable native runtimes, Linux x86-64 only."""
import argparse
import concurrent.futures
import hashlib
import json
import pathlib
import shutil
import tarfile
import urllib.request

FDB = {
    "fdbcli.x86_64": "c3435972a57d98f76edafe456f2c73f967b8adf9d14970edea0b7091cd86ff95",
    "fdbserver.x86_64": "6fe47a94e623feee8706dfac4c4577710431b050901e77d10d7b66a884b5ada1",
    "libfdb_c.x86_64.so": "617572467897d0360f76ffc33f24b58090ee691e3273750d2ff334cc71aae3fa",
}
SPANNER_MANIFEST = "4987860c9f8ecf1fffbbcdac115cb88cb9d1a42bd966c235a9ab843aea34fbd1"


def fetch(url, path, digest=None):
    if not path.exists():
        temporary = path.with_suffix(path.suffix + ".partial")
        with urllib.request.urlopen(url, timeout=60) as response, temporary.open("wb") as out:
            shutil.copyfileobj(response, out)
        temporary.rename(path)
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest and actual != digest:
        raise RuntimeError(f"Checksum mismatch for {path.name}")
    return actual


def extract_named(archive_path, names, directory):
    with tarfile.open(archive_path) as archive:
        for member in archive.getmembers():
            name = pathlib.PurePosixPath(member.name).name
            if member.isfile() and name in names:
                with archive.extractfile(member) as source, (directory / name).open("wb") as out:
                    shutil.copyfileobj(source, out)
                (directory / name).chmod(0o755)


def install_fdb(directory):
    for name, digest in FDB.items():
        fetch("https://github.com/apple/foundationdb/releases/download/7.3.77/" + name,
              directory / name, digest)
        (directory / name).chmod(0o755)
    link = directory / "libfdb_c.so"
    if not link.exists():
        link.symlink_to("libfdb_c.x86_64.so")
    return {"version": "7.3.77", "sha256": FDB}


def install_spanner(directory):
    origin = "https://gcr.io/v2/cloud-spanner-emulator/emulator/"
    request = urllib.request.Request(origin + "manifests/sha256:" + SPANNER_MANIFEST,
                                    headers={"Accept": "application/vnd.docker.distribution.manifest.v2+json"})
    with urllib.request.urlopen(request, timeout=60) as response:
        body = response.read()
    if hashlib.sha256(body).hexdigest() != SPANNER_MANIFEST:
        raise RuntimeError("Spanner manifest checksum mismatch")
    manifest = json.loads(body)
    for layer in reversed(manifest["layers"]):
        if layer["size"] < 1_000_000:
            continue
        digest = layer["digest"].split(":")[1]
        path = directory / (digest + ".tar.gz")
        fetch(origin + "blobs/" + layer["digest"], path, digest)
        extract_named(path, {"emulator_main", "gateway_main"}, directory)
        if (directory / "emulator_main").exists():
            return {"version": "1.5.57", "manifest_sha256": SPANNER_MANIFEST}
    raise RuntimeError("No Spanner emulator binary in pinned image")


def install_cockroach(directory):
    url = "https://binaries.cockroachdb.com/cockroach-v25.4.0.linux-amd64.tgz"
    path = directory / "cockroach.tgz"
    digest = fetch(url, path)
    extract_named(path, {"cockroach", "libgeos.so", "libgeos_c.so"}, directory)
    return {"version": "25.4.0", "url": url, "download_sha256": digest,
            "verification": "TLS download, locally recorded SHA-256, no independent checksum"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=pathlib.Path, default=pathlib.Path(__file__).parent / ".native")
    args = parser.parse_args()
    args.directory.mkdir(parents=True, exist_ok=True)
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        tasks = {pool.submit(fn, args.directory): name for name, fn in [
            ("fdb", install_fdb), ("spanner", install_spanner), ("cockroach", install_cockroach)]}
        for task in concurrent.futures.as_completed(tasks):
            name = tasks[task]
            try:
                results[name] = {"status": "ready", **task.result()}
            except Exception as error:
                results[name] = {"status": "blocked", "error": str(error)}
            print(json.dumps({name: results[name]}), flush=True)
    (args.directory / "downloads.json").write_text(json.dumps(results, indent=2))
    return 0 if all(value["status"] == "ready" for value in results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
