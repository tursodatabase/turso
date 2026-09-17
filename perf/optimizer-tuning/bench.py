#!/usr/bin/env python3
"""Run the bundled TPC-H and ClickBench queries against one `tursodb` binary.

The tuner uses this module for three things: read the two query sets, get the
bytecode each query compiles to under a given cost-parameter file, and measure
how long each query runs.

Build the binary with the `optimizer_params` feature, or the
`TURSO_OPTIMIZER_PARAMS` variable does nothing:

    cargo build --release -p turso_cli --bin tursodb --features optimizer_params
"""

import hashlib
import json
import os
import re
import subprocess
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TPCH_DIR = os.path.join(REPO_ROOT, "perf", "tpc-h")
CLICKBENCH_DIR = os.path.join(REPO_ROOT, "perf", "clickbench")
DEFAULT_BINARY = os.path.join(REPO_ROOT, "target", "release", "tursodb")

DIRECTIVE = re.compile(r"^-- (LIMBO_SKIP|SQLITE_SKIP)(:.*)?$", re.MULTILINE)
EXECUTION_TOTAL = re.compile(r"Execution: .*total=([0-9.]+) (ns|us|ms|s)")
ERROR_MARK = re.compile(r"(Parse error|Runtime error|error:|× )")
UNIT_TO_MS = {"ns": 1e-6, "us": 1e-3, "ms": 1.0, "s": 1000.0}
SCHEMA_COOKIE = re.compile(r"^(\d+\|Transaction\|[^|]*\|[^|]*\|)[^|]*", re.MULTILINE)


class Query:
    def __init__(self, suite, name, sql, db_path):
        self.suite = suite
        self.name = name
        self.sql = sql
        self.db_path = db_path

    @property
    def key(self):
        return f"{self.suite}/{self.name}"


def load_workload(suites=("tpch", "clickbench")):
    queries = []
    if "tpch" in suites:
        queries.extend(load_tpch_queries())
    if "clickbench" in suites:
        queries.extend(load_clickbench_queries())
    return queries


def load_tpch_queries():
    db_path = os.environ.get("TURSO_TPCH_DB", os.path.join(TPCH_DIR, "TPC-H.db"))
    queries_dir = os.path.join(TPCH_DIR, "queries")
    names = sorted(
        (f for f in os.listdir(queries_dir) if f.endswith(".sql")),
        key=lambda f: int(f[:-4]),
    )
    queries = []
    for file_name in names:
        text = open(os.path.join(queries_dir, file_name)).read()
        if "-- LIMBO_SKIP" in text:
            continue
        sql = DIRECTIVE.sub("", text).strip()
        queries.append(Query("tpch", file_name[:-4], sql, db_path))
    return queries


def load_clickbench_queries():
    db_path = os.environ.get("TURSO_CLICKBENCH_DB", os.path.join(CLICKBENCH_DIR, "mydb"))
    lines = open(os.path.join(CLICKBENCH_DIR, "queries.sql")).read().splitlines()
    queries = []
    for line in lines:
        if line.startswith("--") or not line.strip():
            continue
        queries.append(Query("clickbench", str(len(queries) + 1), line.strip(), db_path))
    return queries


def write_params(params, path):
    with open(path, "w") as handle:
        json.dump(params, handle, indent=2, sort_keys=True)
    return path


def bytecode_signature(binary, queries, params_path, timeout=120):
    return {q.key: bytecode(binary, q, params_path, timeout) for q in queries}


def bytecode(binary, query, params_path, timeout=120):
    """Return the bytecode of one query as a short hash.

    Two parameter sets that give the same hash for every query must run at the
    same speed, so the tuner measures each distinct hash only one time.
    """
    try:
        done = _run(binary, query, explain_script(query.sql), params_path, timeout)
    except subprocess.TimeoutExpired:
        return None
    if done.returncode != 0 or not done.stdout.strip():
        return None
    if ERROR_MARK.search(done.stdout) or ERROR_MARK.search(done.stderr):
        return None
    return hashlib.sha1(normalize_bytecode(done.stdout).encode()).hexdigest()[:16]


def normalize_bytecode(text):
    """Remove the schema cookie from every `Transaction` opcode.

    TPC-H query 15 builds a view and removes it again, so the cookie counts
    up as the benchmark runs. It says nothing about the plan, and leaving it
    in would make every hash new on every pass.
    """
    return SCHEMA_COOKIE.sub(r"\1", text)


def explain_script(sql):
    """Build a script that prints the bytecode of every SELECT in `sql`.

    A few benchmark files hold more than one statement: TPC-H query 15 builds
    a view, reads it, then removes it. The statements around the SELECT run
    for real, so the SELECT still compiles against the view.
    """
    lines = [".mode list"]
    for statement in split_statements(sql):
        head = statement.lstrip().lower()
        if head.startswith("select") or head.startswith("with"):
            lines.append(f"EXPLAIN {statement};")
        else:
            lines.append(f"{statement};")
    lines.append(".quit")
    return "\n".join(lines) + "\n"


def split_statements(sql):
    """Split a query file into statements, without its `--` comments.

    The scanner tracks string literals, so a `--` or a `;` inside a quoted
    value stays part of the statement.
    """
    statements = []
    current = []
    index = 0
    in_string = False
    while index < len(sql):
        char = sql[index]
        if in_string:
            current.append(char)
            if char == "'":
                in_string = sql[index + 1 : index + 2] == "'"
                if in_string:
                    current.append("'")
                    index += 1
            index += 1
            continue
        if char == "'":
            in_string = True
            current.append(char)
            index += 1
            continue
        if sql.startswith("--", index):
            end = sql.find("\n", index)
            index = len(sql) if end == -1 else end
            continue
        if char == ";":
            statements.append("".join(current).strip())
            current = []
            index += 1
            continue
        current.append(char)
        index += 1
    statements.append("".join(current).strip())
    return [statement for statement in statements if statement]


def measure(binary, queries, params_path, repeats=3, timeout=120, warmup=True):
    """Time every query and keep the fastest run of each one.

    The fastest run is the measurement the machine's other work disturbed
    least, so it is the most repeatable summary of a plan's cost.
    """
    results = {}
    for query in queries:
        if warmup:
            run_once(binary, query, params_path, timeout)
        samples = []
        for _ in range(repeats):
            sample = run_once(binary, query, params_path, timeout)
            if sample is None:
                samples = []
                break
            samples.append(sample)
        results[query.key] = min(samples) if samples else None
    return results


def run_once(binary, query, params_path, timeout):
    """Return the execution time of one query in milliseconds, or None."""
    script = ".timer on\n" + ";\n".join(split_statements(query.sql)) + ";\n.quit\n"
    started = time.perf_counter()
    try:
        done = _run(binary, query, script, params_path, timeout)
    except subprocess.TimeoutExpired:
        return None
    if done.returncode != 0:
        return None
    if ERROR_MARK.search(done.stdout) or ERROR_MARK.search(done.stderr):
        return None
    reported = EXECUTION_TOTAL.findall(done.stdout)
    if reported:
        return sum(float(value) * UNIT_TO_MS[unit] for value, unit in reported)
    return (time.perf_counter() - started) * 1000.0


def _run(binary, query, script, params_path, timeout):
    """Run one script and fail loudly when another process holds the database.

    `tursodb` takes an exclusive lock on the file, so a second harness on the
    same database gets no plan and no timing. Treating that as a slow plan
    would poison the cache, so stop instead.
    """
    done = subprocess.run(
        [binary, query.db_path, "--quiet", "--output-mode", "list"],
        input=script,
        env=_env(params_path),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if "Locking error" in done.stderr:
        raise RuntimeError(f"{query.db_path} is open in another process")
    return done


def _env(params_path):
    env = dict(os.environ)
    env["RUST_LOG"] = "off"
    if params_path is None:
        env.pop("TURSO_OPTIMIZER_PARAMS", None)
    else:
        env["TURSO_OPTIMIZER_PARAMS"] = params_path
    return env
