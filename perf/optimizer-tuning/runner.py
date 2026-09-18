"""Run one cost vector against the TPC-H workload and keep every measurement.

The engine caches its parameter file once per process, so each measurement
starts a new process with the environment already set. Nothing here reuses a
process across two different vectors.
"""

import hashlib
import json
import os
import subprocess
import time

import cost_vector

# The read-only snapshot cannot run a query file that creates a view, so such a
# file runs against a private writable copy of the same snapshot.
WRITABLE_QUERIES = ("15",)

PROCESS_WATCHDOG_MARGIN_SECONDS = 60


def sha256_of_file(path, limit=None):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        read = 0
        while True:
            block = handle.read(1 << 20)
            if not block:
                break
            digest.update(block)
            read += len(block)
            if limit is not None and read >= limit:
                break
    return digest.hexdigest()


def sha256_of_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class Workload:
    """The frozen list of query files, with the statements each one holds."""

    def __init__(self, query_dir):
        self.query_dir = os.path.abspath(query_dir)
        self.queries = []
        for name in sorted(os.listdir(self.query_dir)):
            if not name.endswith(".sql"):
                continue
            path = os.path.join(self.query_dir, name)
            with open(path, encoding="utf-8") as handle:
                text = handle.read()
            stem = name[: -len(".sql")]
            skips = [
                line.strip()
                for line in text.splitlines()
                if line.strip().startswith("-- LIMBO_SKIP")
                or line.strip().startswith("-- SQLITE_SKIP")
            ]
            self.queries.append(
                {
                    "query": stem,
                    "file": name,
                    "sha256": sha256_of_text(text),
                    "bytes": len(text.encode("utf-8")),
                    "directives": skips,
                    "writable": stem in WRITABLE_QUERIES,
                }
            )
        self.queries.sort(key=lambda row: int(row["query"]))

    @property
    def names(self):
        """Every query Turso must run. No file carries a LIMBO_SKIP directive."""
        return [
            row["query"]
            for row in self.queries
            if not any(d.startswith("-- LIMBO_SKIP") for d in row["directives"])
        ]

    def manifest(self):
        return {
            "query_dir": self.query_dir,
            "queries": self.queries,
            "runnable": self.names,
        }


class Engine:
    """Launches the execution runner, one fresh process per call."""

    def __init__(self, binary, databases, vector_dir, log_dir):
        self.binary = os.path.abspath(binary)
        self.databases = {k: os.path.abspath(v) for k, v in databases.items()}
        self.vector_dir = os.path.abspath(vector_dir)
        self.log_dir = os.path.abspath(log_dir)
        os.makedirs(self.vector_dir, exist_ok=True)
        os.makedirs(self.log_dir, exist_ok=True)
        self.binary_sha256 = sha256_of_file(self.binary)

    def vector_path(self, vector_id, vector):
        """Write a checked vector to an immutable path named after its digest."""
        checked = cost_vector.validate(vector)
        body = json.dumps(checked, indent=2, sort_keys=True) + "\n"
        digest = sha256_of_text(body)[:16]
        path = os.path.join(self.vector_dir, f"{vector_id}-{digest}.json")
        if not os.path.exists(path):
            temporary = path + ".partial"
            with open(temporary, "w", encoding="utf-8") as handle:
                handle.write(body)
            os.replace(temporary, path)
            os.chmod(path, 0o444)
        return path

    def _run(self, arguments, vector_path, timeout_seconds, log_name):
        environment = dict(os.environ)
        environment.pop("RUST_LOG", None)
        if vector_path is None:
            environment.pop("TURSO_OPTIMIZER_PARAMS", None)
        else:
            environment["TURSO_OPTIMIZER_PARAMS"] = vector_path
        stderr_path = os.path.join(self.log_dir, log_name)
        started = time.time()
        with open(stderr_path, "wb") as stderr:
            process = subprocess.Popen(
                [self.binary] + arguments,
                stdout=subprocess.PIPE,
                stderr=stderr,
                env=environment,
            )
            try:
                stdout, _ = process.communicate(timeout=timeout_seconds)
                timed_out = False
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, _ = process.communicate()
                timed_out = True
        return {
            "stdout": (stdout or b"").decode("utf-8", "replace"),
            "returncode": process.returncode,
            "timed_out": timed_out,
            "wall_seconds": time.time() - started,
            "stderr_path": stderr_path,
        }

    def effective_vector(self, vector_path):
        """Read back the vector the engine actually loaded.

        The engine falls back to its compiled defaults when a file is missing or
        invalid, so a run that skipped this check could measure the defaults
        while believing it measured a candidate.
        """
        result = self._run(
            [
                "--print-params",
                "--database",
                next(iter(self.databases.values())),
                "--query-dir",
                ".",
            ],
            vector_path,
            60,
            "print-params.err",
        )
        if result["returncode"] != 0:
            raise RuntimeError(f"--print-params failed: {result['stderr_path']}")
        return json.loads(result["stdout"].strip())

    def plans(self, regime, query_names, vector_path, workload, timeout_seconds=600):
        """Collect the plan and the bytecode of every named query."""
        records = {}
        read_only = [n for n in query_names if n not in WRITABLE_QUERIES]
        writable = [n for n in query_names if n in WRITABLE_QUERIES]
        for names, needs_write in ((read_only, False), (writable, True)):
            if not names:
                continue
            database = self.databases[regime + ("_rw" if needs_write else "")]
            arguments = [
                "--database", database,
                "--query-dir", workload.query_dir,
                "--plans", "--bytecode",
            ]
            for name in names:
                arguments += ["--query", name]
            if needs_write:
                arguments.append("--writable")
            result = self._run(
                arguments, vector_path, timeout_seconds, f"plans-{regime}.err"
            )
            if result["returncode"] != 0 or result["timed_out"]:
                raise RuntimeError(
                    f"plan collection failed for {regime}: {result['stderr_path']}"
                )
            for line in result["stdout"].splitlines():
                if not line.startswith("{"):
                    continue
                record = json.loads(line)
                records.setdefault(record["query"], []).append(record)
        return records

    def measure(self, regime, query_name, vector_path, workload, warmups, repetitions,
                timeout_seconds):
        """Run one query in its own process and return every sample."""
        needs_write = query_name in WRITABLE_QUERIES
        database = self.databases[regime + ("_rw" if needs_write else "")]
        arguments = [
            "--database", database,
            "--query-dir", workload.query_dir,
            "--query", query_name,
            "--warmups", str(warmups),
            "--repetitions", str(repetitions),
            "--timeout-seconds", str(timeout_seconds),
        ]
        if needs_write:
            arguments.append("--writable")
        watchdog = (
            timeout_seconds * (warmups + repetitions) + PROCESS_WATCHDOG_MARGIN_SECONDS
        )
        result = self._run(
            arguments, vector_path, watchdog, f"measure-{regime}-q{query_name}.err"
        )
        samples = [
            json.loads(line) for line in result["stdout"].splitlines()
            if line.startswith("{")
        ]
        status = "ok"
        if result["timed_out"]:
            status = "process_timeout"
        elif result["returncode"] != 0:
            status = "error"
        elif len(samples) != repetitions:
            status = "incomplete"
        return {
            "status": status,
            "samples": samples,
            "returncode": result["returncode"],
            "wall_seconds": result["wall_seconds"],
            "stderr_path": result["stderr_path"],
        }

    def rows(self, regime, query_name, vector_path, workload, timeout_seconds):
        """Collect every result row for an untimed correctness comparison."""
        needs_write = query_name in WRITABLE_QUERIES
        database = self.databases[regime + ("_rw" if needs_write else "")]
        arguments = [
            "--database", database,
            "--query-dir", workload.query_dir,
            "--query", query_name,
            "--print-rows",
            "--timeout-seconds", str(timeout_seconds),
        ]
        if needs_write:
            arguments.append("--writable")
        result = self._run(
            arguments,
            vector_path,
            timeout_seconds + PROCESS_WATCHDOG_MARGIN_SECONDS,
            f"rows-{regime}-q{query_name}.err",
        )
        if result["returncode"] != 0 or result["timed_out"]:
            raise RuntimeError(
                f"row collection failed for {regime} query {query_name}: "
                f"{result['stderr_path']}"
            )
        return [json.loads(line) for line in result["stdout"].splitlines()
                if line.startswith("[")]


def normalized_bytecode(rows):
    """Remove the schema cookie from the bytecode.

    The P3 operand of `Transaction` carries the schema version. A query file
    that creates and drops a view raises that version on every run, so the same
    program would otherwise look different each time it ran.
    """
    normalized = []
    for row in rows:
        if len(row) > 3 and row[1] == "Transaction":
            row = list(row)
            row[4] = "<schema-cookie>"
        normalized.append(list(row))
    return normalized


def plan_fingerprint(records):
    """Identify the executable program a query will run.

    The bytecode is the identity: it holds the join order, the access paths, the
    sorters, the ephemeral indexes and every other executable choice. The
    structured plan is kept beside it for reading, with the estimated rows and
    costs removed, because those change with the parameters without changing
    what runs.
    """
    parts = []
    for record in sorted(records, key=lambda r: r["statement"]):
        plan = json.loads(json.dumps(record["plan"]))
        for node in plan.get("nodes", []):
            node.get("op", {}).pop("estimate", None)
        parts.append(
            {
                "statement": record["statement"],
                "bytecode": normalized_bytecode(record["bytecode"]),
                "plan": plan,
            }
        )
    body = json.dumps(parts, sort_keys=True, separators=(",", ":"))
    return sha256_of_text(body), parts


def structural_plan(records):
    """The plan text of each statement, for a readable before/after comparison."""
    lines = []
    for record in sorted(records, key=lambda r: r["statement"]):
        for node in record["plan"].get("nodes", []):
            lines.append(node["detail"])
    return lines
