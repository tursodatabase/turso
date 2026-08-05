#!/usr/bin/env python3
# Copyright 2023-2026 the Turso authors. All rights reserved. MIT license.
"""Run the upstream PostgreSQL conformance tests against tursopg.

Builds the tursopg server and the pgregress runner, starts a server on a
fresh temporary database and a free port, runs the test corpus, and tears
the server down. Exit code follows the runner: 0 all passed, 1 failures.

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


def main() -> int:
    runner_args = []
    tests = []
    for arg in sys.argv[1:]:
        (runner_args if arg.startswith("-") else tests).append(arg)

    if not tests:
        tests = schedule_order()
    paths = []
    for test in tests:
        path = Path(test)
        if not path.exists():
            path = UPSTREAM / f"{test}.sql"
        if not path.exists():
            sys.exit(f"error: no such test: {test}")
        paths.append(str(path))

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

        # One runner invocation per test, restarting the server whenever a
        # test crashed or wedged it (exit code 3 = transport failure, or a
        # runner timeout). A crash still fails the test that caused it, but
        # stops poisoning every test after it — needed until the engine
        # guarantees a query cannot take the server down, and harmless
        # after. The database file persists across restarts, so fixtures
        # survive.
        server = start_server()
        passed = failed = restarts = 0
        try:
            for path in paths:
                if server.poll() is not None:
                    server = start_server()
                    restarts += 1
                try:
                    result = subprocess.run(
                        [bins / "pgregress", "--dsn", f"postgres://127.0.0.1:{port}/regression"]
                        + runner_args
                        + [path],
                        cwd=REPO_ROOT,
                        env=env,
                        timeout=60,
                    )
                    returncode = result.returncode
                except subprocess.TimeoutExpired:
                    print(f"{Path(path).stem} ... FAILED (runner timeout)")
                    returncode = 3
                if returncode == 0:
                    passed += 1
                else:
                    failed += 1
                if returncode == 3:
                    server.terminate()
                    try:
                        server.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        server.kill()
                    server = start_server()
                    restarts += 1
        finally:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
        print(f"\n=== total: {passed} of {passed + failed} test(s) passed, "
              f"{restarts} server restart(s) ===")
        return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
