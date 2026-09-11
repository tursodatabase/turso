#!/usr/bin/env python3
# Copyright 2023-2026 the Turso authors. All rights reserved. MIT license.
"""Run the upstream PostgreSQL conformance tests against tursopg.

Builds the tursopg server and the pgregress runner, starts a server on a
fresh temporary database and a free port, runs the test corpus in schedule
order, and tears the server down.

Every corpus test runs on every full invocation and is judged against its
status in the STATUS table below, so the run both catches regressions in
what already works and notices when a known-bad test starts passing:

    pass - blessed: byte-exact output required; any diff or crash is a
           regression and fails the run.
    fail - known-bad: the test runs and its diff is reported, but does not
           fail the run. If it becomes byte-exact the run fails with a
           request to bless it, so the known-bad list only ever shrinks.
    skip - not run at all; reserved for tests that cannot run, with the
           reason noted. (Currently none.)

To bless a test after fixing its remaining diffs, change its status from
"fail" to "pass" in STATUS. Exit code: 0 when every test matched its
status, 1 on regressions or unexpected passes.

Usage:
    postgres/conformance/run.py                # whole corpus
    postgres/conformance/run.py boolean        # single test by name
    postgres/conformance/run.py --max-diff-lines 0 boolean

Bare arguments are resolved as test names in postgres/conformance/upstream/;
arguments starting with `--` are passed through to the pgregress runner.
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
UPSTREAM = REPO_ROOT / "postgres" / "conformance" / "upstream"

# Status of every corpus test, in schedule order. See the module docstring
# for the pass/fail/skip semantics and how to bless a test.
STATUS = {
    "test_setup": "fail",

    "boolean": "fail",
    "char": "fail",
    "name": "fail",
    "varchar": "fail",
    "text": "fail",
    "int2": "fail",
    "int4": "fail",
    "int8": "fail",
    "oid": "fail",
    "float4": "fail",
    "float8": "fail",
    "bit": "fail",
    "numeric": "fail",
    "uuid": "fail",
    "enum": "fail",
    "money": "fail",
    "rangetypes": "fail",

    "strings": "fail",
    "md5": "fail",
    "numerology": "fail",
    "date": "fail",
    "time": "fail",
    "timetz": "fail",
    "timestamp": "fail",
    "timestamptz": "fail",
    "interval": "fail",
    "inet": "fail",
    "macaddr": "fail",
    "macaddr8": "fail",
    "multirangetypes": "fail",

    "horology": "fail",
    "regex": "fail",
    "comments": "pass",
    "expressions": "fail",
    "unicode": "fail",

    "copy": "fail",
    "copyselect": "fail",
    "copydml": "fail",
    "insert": "fail",
    "insert_conflict": "fail",

    "create_misc": "fail",
    "create_table": "fail",
    "create_schema": "fail",

    "create_index": "fail",
    "create_view": "fail",
    "index_including": "fail",

    "constraints": "fail",
    "select": "fail",
    "drop_if_exists": "fail",
    "updatable_views": "fail",
    "errors": "fail",

    "select_into": "fail",
    "select_distinct": "fail",
    "select_distinct_on": "fail",
    "select_implicit": "fail",
    "select_having": "fail",
    "subselect": "fail",
    "union": "fail",
    "case": "fail",
    "join": "fail",
    "aggregates": "fail",
    "transactions": "fail",
    "random": "fail",
    "portals": "fail",
    "arrays": "fail",
    "update": "fail",
    "delete": "fail",
    "namespace": "fail",

    "matview": "fail",
    "tablesample": "fail",
    "groupingsets": "fail",
    "identity": "fail",
    "generated_stored": "fail",

    "create_table_like": "fail",
    "merge": "fail",
    "tsrf": "fail",

    "select_views": "fail",
    "portals_p2": "fail",
    "foreign_key": "fail",
    "guc": "fail",
    "window": "fail",
    "functional_deps": "fail",

    "json": "fail",
    "jsonb": "fail",
    "json_encoding": "fail",
    "jsonpath": "fail",
    "jsonpath_encoding": "fail",
    "jsonb_jsonpath": "fail",
    "sqljson": "fail",
    "sqljson_queryfuncs": "fail",
    "sqljson_jsontable": "fail",

    "limit": "fail",
    "copy2": "fail",
    "temp": "fail",
    "domain": "fail",
    "prepare": "fail",
    "truncate": "fail",
    "alter_table": "fail",
    "sequence": "fail",
    "rowtypes": "fail",
    "returning": "fail",
    "with": "fail",
}


def target_dir() -> Path:
    out = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return Path(json.loads(out.stdout)["target_directory"])


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_server(proc: subprocess.Popen, port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            sys.exit(f"error: tursopg exited with status {proc.returncode} during startup")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.05)
    sys.exit(f"error: tursopg did not accept connections on port {port} within {timeout}s")


def schedule_order() -> list[str]:
    tests = []
    for line in (UPSTREAM / "schedule").read_text().splitlines():
        if line.startswith("test:"):
            tests += line.removeprefix("test:").split()
    return tests


def validate_status() -> None:
    """Every scheduled test needs a status and vice versa, so a corpus or
    schedule edit cannot silently drop a test out of the ratchet."""
    scheduled = set(schedule_order())
    listed = set(STATUS)
    if scheduled - listed:
        sys.exit(f"error: tests missing from STATUS: {sorted(scheduled - listed)}")
    if listed - scheduled:
        sys.exit(f"error: STATUS lists tests not in the schedule: {sorted(listed - scheduled)}")


def parse_argv() -> tuple[list[str], list[str]]:
    runner_args = []
    tests = []
    for arg in sys.argv[1:]:
        (runner_args if arg.startswith("-") else tests).append(arg)
    return runner_args, tests


def resolve_paths(tests: list[str]) -> list[str]:
    paths = []
    for test in tests:
        path = Path(test)
        if not path.exists():
            path = UPSTREAM / f"{test}.sql"
        if not path.exists():
            sys.exit(f"error: no such test: {test}")
        paths.append(str(path))
    return paths


def test_status(name: str) -> tuple[str, str]:
    """Status and skip reason of a test. Tests outside the corpus must pass."""
    status = STATUS.get(name, "pass")
    if isinstance(status, str):
        return status, ""
    return status


def stop(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


class Tally:
    """Counts test outcomes against their statuses and renders the summary."""

    def __init__(self) -> None:
        self.ok = 0
        self.known_bad = 0
        self.skipped = 0
        self.restarts = 0
        self.regressions: list[str] = []
        self.unexpected_passes: list[str] = []

    def classify(self, name: str, status: str, returncode: int) -> None:
        if returncode == 0:
            if status == "fail":
                self.unexpected_passes.append(name)
            else:
                self.ok += 1
        elif status == "fail":
            self.known_bad += 1
        else:
            self.regressions.append(name)

    def report(self) -> int:
        print(f"\n=== total: {self.ok} blessed ok, {self.known_bad} known-bad "
              f"failure(s), {len(self.regressions)} regression(s), "
              f"{len(self.unexpected_passes)} unexpected pass(es), "
              f"{self.skipped} skipped, {self.restarts} server restart(s) ===")
        for name in self.regressions:
            print(f'REGRESSION: {name} is blessed "pass" but its output diverged')
        for name in self.unexpected_passes:
            print(f'XPASS: {name} now passes byte-exact; bless it by changing its '
                  f'STATUS entry to "pass"')
        return 1 if self.regressions or self.unexpected_passes else 0


def main() -> int:
    validate_status()
    runner_args, tests = parse_argv()
    paths = resolve_paths(tests or schedule_order())

    subprocess.run(
        ["cargo", "build", "-p", "tursopg", "-p", "turso_pg_regress"],
        cwd=REPO_ROOT,
        check=True,
    )
    bins = target_dir() / "debug"

    port = free_port()
    # The environment pg_regress passes to psql, consumed by the scripts via
    # \getenv: test_setup resolves data files relative to PG_ABS_SRCDIR, and
    # copy tests write result files under PG_ABS_BUILDDIR/results. The
    # regress C library does not exist here; statements loading it fail
    # visibly in the diffs.
    env = dict(
        os.environ,
        PG_ABS_SRCDIR=str(UPSTREAM),
        PG_ABS_BUILDDIR=str(REPO_ROOT / "postgres" / "regress"),
        PG_LIBDIR=str(UPSTREAM),
        PG_DLSUFFIX=".so",
    )
    with tempfile.TemporaryDirectory(prefix="pgregress-") as tmp:
        db_file = Path(tmp) / "regression.db"

        def start_server() -> subprocess.Popen:
            proc = subprocess.Popen(
                [bins / "tursopg", "--server", f"127.0.0.1:{port}", db_file],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            wait_for_server(proc, port)
            return proc

        def run_one(path: str) -> int:
            try:
                result = subprocess.run(
                    [bins / "pgregress", "--dsn", f"postgres://127.0.0.1:{port}/regression"]
                    + runner_args
                    + [path],
                    cwd=REPO_ROOT,
                    env=env,
                    timeout=60,
                )
                return result.returncode
            except subprocess.TimeoutExpired:
                print(f"{Path(path).stem} ... FAILED (runner timeout)")
                return 3

        # One runner invocation per test, restarting the server whenever a
        # test crashed or wedged it (exit code 3 = transport failure, or a
        # runner timeout). A crash still fails the test that caused it, but
        # stops poisoning every test after it — needed until the engine
        # guarantees a query cannot take the server down, and harmless
        # after. The database file persists across restarts, so fixtures
        # survive.
        server = start_server()
        tally = Tally()
        try:
            for path in paths:
                name = Path(path).stem
                status, reason = test_status(name)
                if status == "skip":
                    print(f"{name} ... skip ({reason})")
                    tally.skipped += 1
                    continue
                if server.poll() is not None:
                    server = start_server()
                    tally.restarts += 1
                returncode = run_one(path)
                tally.classify(name, status, returncode)
                if returncode == 3:
                    stop(server)
                    server = start_server()
                    tally.restarts += 1
        finally:
            stop(server)
        return tally.report()


if __name__ == "__main__":
    sys.exit(main())
