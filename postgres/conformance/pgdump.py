import argparse
import difflib
import os
import re
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit


def main() -> int:
    conformance = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="Compare PostgreSQL and Turso pg_dump files byte for byte.")
    parser.add_argument("fixtures", nargs="*", type=Path)
    parser.add_argument("--binary", type=Path, default=conformance.parents[1] / "target/debug/tursopg")
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--pg-dump", default="pg_dump")
    parser.add_argument("--output-dir", type=Path, default=conformance / "pgdump/results")
    args = parser.parse_args()
    reference = os.environ.get("PG_DSN", "")
    if urlsplit(reference).scheme not in ("postgres", "postgresql"):
        parser.error("set PG_DSN to a disposable PostgreSQL server with CREATEDB permission, or use with-postgres.sh")
    fixtures = args.fixtures or sorted((conformance / "pgdump").glob("*.sql"))
    if not fixtures:
        parser.error("no SQL fixtures found")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="run-", dir=args.output_dir)).resolve()
    print(f"Results: {output}", flush=True)
    failures = 0
    for fixture in fixtures:
        results = output / fixture.stem
        results.mkdir()
        try:
            passed = run_fixture(args, reference, fixture.resolve(), results)
        except (OSError, RuntimeError) as error:
            print(f"FAIL {fixture.stem}: {error}", flush=True)
            failures += 1
        else:
            if not passed:
                failures += 1
    print(f"{len(fixtures) - failures} passed, {failures} failed", flush=True)
    return int(failures != 0)


def run_fixture(args, reference: str, fixture: Path, results: Path) -> bool:
    database = f"pgdump_{uuid.uuid4().hex}"
    parts = urlsplit(reference)
    query = urlencode(
        [(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True) if key != "dbname"],
        quote_via=quote,
    )
    postgres = urlunsplit(parts._replace(path=f"/{database}", query=query))
    psql = [args.psql, "-X", "--no-password", "--set", "ON_ERROR_STOP=1"]
    try:
        run_command(
            [*psql, "--dbname", reference, "--command", f'CREATE DATABASE "{database}" TEMPLATE template0'],
            results / "create.log",
        )
        with tempfile.TemporaryDirectory(prefix="pgdump-") as temporary:
            server_log = results / "server.log"
            with server_log.open("wb") as log:
                server = subprocess.Popen(
                    [str(args.binary.resolve()), "--server", "127.0.0.1:0", str(Path(temporary) / "test.db")],
                    cwd=temporary,
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
                try:
                    port = wait_for_server(server, server_log)
                    turso = f"postgresql://turso@127.0.0.1:{port}/main?sslmode=disable"
                    for name, dsn in (("postgres", postgres), ("turso", turso)):
                        run_command(
                            [*psql, "--dbname", dsn, "--file", str(fixture)],
                            results / f"{name}-bootstrap.log",
                        )
                        run_command(
                            [
                                args.pg_dump,
                                "--no-password",
                                "--restrict-key=TursoPgdumpConformance",
                                "--dbname",
                                dsn,
                                "--file",
                                str(results / f"{name}.sql"),
                            ],
                            results / f"{name}-dump.log",
                        )
                finally:
                    stop_server(server)
    finally:
        run_command(
            [*psql, "--dbname", reference, "--command", f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'],
            results / "drop.log",
        )
    expected = (results / "postgres.sql").read_bytes()
    actual = (results / "turso.sql").read_bytes()
    if expected == actual:
        print(f"PASS {fixture.stem}: identical dump files", flush=True)
        return True
    difference = b"".join(
        difflib.diff_bytes(
            difflib.unified_diff,
            expected.splitlines(keepends=True),
            actual.splitlines(keepends=True),
            fromfile=b"postgres.sql",
            tofile=b"turso.sql",
        )
    )
    (results / "dump.diff").write_bytes(difference)
    print(f"FAIL {fixture.stem}: dump files differ", flush=True)
    print(difference.decode("utf-8", errors="replace"), end="", flush=True)
    return False


def wait_for_server(server: subprocess.Popen, log: Path) -> int:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if server.poll() is not None:
            raise RuntimeError(f"tursopg exited during startup; see {log}")
        match = re.search(r"PostgreSQL server listening on 127\.0\.0\.1:(\d+)", log.read_text(errors="replace"))
        if match:
            return int(match[1])
        time.sleep(0.05)
    raise RuntimeError(f"tursopg startup timed out; see {log}")


def stop_server(server: subprocess.Popen) -> None:
    if server.poll() is None:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()


def run_command(command: list[str], log: Path) -> None:
    with log.open("wb") as output:
        try:
            result = subprocess.run(
                command,
                check=False,
                stdin=subprocess.DEVNULL,
                stdout=output,
                stderr=subprocess.STDOUT,
                timeout=60,
                env={**os.environ, "PGCONNECT_TIMEOUT": "5"},
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"{log.stem} timed out; see {log}") from None
    if result.returncode != 0:
        raise RuntimeError(f"{log.stem} exited with status {result.returncode}; see {log}")


if __name__ == "__main__":
    sys.exit(main())
