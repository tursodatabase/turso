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


def main() -> int:
    runner_args = []
    tests = []
    for arg in sys.argv[1:]:
        (runner_args if arg.startswith("-") else tests).append(arg)

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
    with tempfile.TemporaryDirectory(prefix="pgregress-") as tmp:
        server = subprocess.Popen(
            [bins / "tursopg", "--server", f"127.0.0.1:{port}", Path(tmp) / "regression.db"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            wait_for_server(server, port)
            result = subprocess.run(
                [bins / "pgregress", "--dsn", f"postgres://127.0.0.1:{port}/regression"]
                + runner_args
                + paths,
                cwd=REPO_ROOT,
            )
            return result.returncode
        finally:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()


if __name__ == "__main__":
    sys.exit(main())
