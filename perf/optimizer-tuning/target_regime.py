#!/usr/bin/env python3
"""Compare cost vectors under the conditions the bundled TPC-H script uses.

The search proxy runs the execution runner with a warm cache and the syscall
I/O backend. The bundled `perf/tpc-h/run.sh` instead drops the system caches
before every query and runs the `tursodb` shell with the io_uring backend, and
its timing covers process start, parsing, preparation and printing the rows.
This script measures that regime, so a result from the proxy can be confirmed
where the project actually measures TPC-H.

Configurations are interleaved inside every round and their order turns around
on every other round, so a machine that slows down partway cannot favour one
side.
"""

import argparse
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import cost_vector
import runner

REGIMES = ("nostats", "analyzed")


def drop_caches():
    subprocess.run(["sync"], check=True)
    result = subprocess.run(
        ["sudo", "-n", "tee", "/proc/sys/vm/drop_caches"],
        input=b"3\n", stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "cannot drop the system caches: "
            + result.stderr.decode("utf-8", "replace").strip()
        )


def query_sql(query_dir, name):
    """Read a query file and strip the runner directives, as run.sh does."""
    path = os.path.join(query_dir, f"{name}.sql")
    with open(path, encoding="utf-8") as handle:
        lines = handle.read().splitlines()
    kept = [
        line for line in lines
        if not line.strip().startswith("-- LIMBO_SKIP")
        and not line.strip().startswith("-- SQLITE_SKIP")
    ]
    return "\n".join(kept)


def run_one(binary, database, sql, vector_path, vfs, timeout_seconds, output_path):
    environment = dict(os.environ)
    environment["RUST_LOG"] = "off"
    if vector_path is None:
        environment.pop("TURSO_OPTIMIZER_PARAMS", None)
    else:
        environment["TURSO_OPTIMIZER_PARAMS"] = vector_path
    command = [binary, database]
    if vfs:
        command += ["--vfs", vfs]
    command += ["--quiet", "--output-mode", "list", "--", sql]
    drop_caches()
    with open(output_path, "wb") as output:
        started = time.perf_counter()
        process = subprocess.Popen(
            command, stdout=output, stderr=subprocess.PIPE, env=environment
        )
        try:
            _, errors = process.communicate(timeout=timeout_seconds)
            timed_out = False
        except subprocess.TimeoutExpired:
            process.kill()
            _, errors = process.communicate()
            timed_out = True
        elapsed = time.perf_counter() - started
    with open(output_path, "rb") as output:
        rows = sum(1 for _ in output)
    return {
        "elapsed_seconds": elapsed,
        "returncode": process.returncode,
        "timed_out": timed_out,
        "output_rows": rows,
        "stderr": errors.decode("utf-8", "replace")[:2000],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary")
    parser.add_argument("--builds", nargs="*", default=[],
                        help="name=path pairs of builds that carry their "
                             "defaults compiled in; use instead of --binary")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--query-dir", default="perf/tpc-h/queries")
    parser.add_argument("--out", required=True)
    parser.add_argument("--vectors", nargs="*", default=[])
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--vfs", default="io_uring")
    parser.add_argument("--timeout-seconds", type=int, default=600)
    parser.add_argument("--suffix", default="-cli",
                        help="suffix of the database copies this script writes to")
    args = parser.parse_args()

    workload = runner.Workload(args.query_dir)
    names = workload.names
    databases = {
        regime: os.path.join(args.data_dir, f"tpch-{regime}{args.suffix}.db")
        for regime in REGIMES
    }
    for regime, path in databases.items():
        if not os.path.exists(path):
            raise SystemExit(f"missing {path}; copy the snapshot there first")

    if args.builds:
        configurations = []
        for entry in args.builds:
            name, path = entry.split("=", 1)
            configurations.append((name, os.path.abspath(path), None))
    else:
        if not args.binary:
            raise SystemExit("pass --binary or --builds")
        configurations = [("defaults", os.path.abspath(args.binary), None)]
        for path in args.vectors:
            absolute = os.path.abspath(path)
            cost_vector.read(absolute)
            configurations.append((
                os.path.splitext(os.path.basename(path))[0],
                os.path.abspath(args.binary), absolute,
            ))

    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    scratch = os.path.join(os.path.dirname(os.path.abspath(args.out)), "cli-output")
    os.makedirs(scratch, exist_ok=True)

    done = set()
    if os.path.exists(args.out):
        with open(args.out, encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    row = json.loads(line)
                    done.add((row["config"], row["round"], row["regime"],
                              row["query"]))

    for round_index in range(args.rounds):
        order = (configurations if round_index % 2 == 0
                 else list(reversed(configurations)))
        for regime in REGIMES:
            for name in names:
                for config, binary, vector_path in order:
                    if (config, round_index, regime, name) in done:
                        continue
                    result = run_one(
                        binary, databases[regime],
                        query_sql(args.query_dir, name), vector_path, args.vfs,
                        args.timeout_seconds,
                        os.path.join(scratch, f"{config}-{regime}-q{name}.out"),
                    )
                    record = {
                        "config": config, "round": round_index, "regime": regime,
                        "query": name, "vfs": args.vfs, "binary": binary,
                        "measured_at": time.time(), **result,
                    }
                    with open(args.out, "a", encoding="utf-8") as handle:
                        handle.write(json.dumps(record, sort_keys=True) + "\n")
                        handle.flush()
                        os.fsync(handle.fileno())
                    status = "ok" if result["returncode"] == 0 and not result["timed_out"] else "FAILED"
                    print(f"r{round_index} {regime:9} Q{name:<3} {config:16} "
                          f"{result['elapsed_seconds']:7.3f}s rows={result['output_rows']:<6} "
                          f"{status}", flush=True)


if __name__ == "__main__":
    main()
