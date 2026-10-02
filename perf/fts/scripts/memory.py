import argparse
import csv
import json
import os
import platform
import subprocess
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Measure Turso FTS query-phase peak heap with dhat")
    parser.add_argument("output", type=Path)
    parser.add_argument("--documents", type=int, default=10000)
    parser.add_argument("--connections", type=int, nargs="+", default=[1, 2, 4, 8, 16, 32])
    parser.add_argument("--queries", type=int, default=3, help="queries per connection")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--state", choices=["first", "warm"], default="warm")
    parser.add_argument("--profile", choices=["dev", "bench-profile"], default="bench-profile")
    args = parser.parse_args()
    if min(args.documents, args.queries, args.runs, *args.connections) <= 0:
        parser.error("counts must be positive")
    if len(set(args.connections)) != len(args.connections):
        parser.error("connection counts must be distinct")
    if args.state == "first" and args.queries != 1:
        parser.error("first-query measurement requires --queries 1")
    root = Path(__file__).resolve().parents[3]
    args.output.mkdir()
    output = args.output.resolve()
    write_environment(args, root, output)
    csv_paths = []
    for connections in args.connections:
        directory = output / f"c{connections}"
        directory.mkdir()
        csv_path = directory / "results.csv"
        csv_paths.append(str(csv_path))
        with csv_path.open("w", newline="") as stream:
            writer = None
            for run in range(1, args.runs + 1):
                for query in ("rare", "common", "and", "or", "phrase", "ranked"):
                    modes = ("wal", "mvcc") if run % 2 else ("mvcc", "wal")
                    for mode in modes:
                        stem = directory / f"{mode}-{query}-run{run}"
                        print(f"Heap: c{connections} {mode} {query} run {run}", flush=True)
                        command = [
                            "cargo",
                            "run",
                            "--profile",
                            args.profile,
                            "-p",
                            "memory-benchmark",
                            "--features",
                            "fts",
                            "--bin",
                            "fts-memory",
                            "--",
                            "--query",
                            query,
                            "--state",
                            args.state,
                            "--mode",
                            mode,
                            "--documents",
                            str(args.documents),
                            "--connections",
                            str(connections),
                            "--queries",
                            str(args.queries),
                            "--dhat-file",
                            str(stem) + ".dhat.json",
                        ]
                        with (
                            Path(str(stem) + ".json").open("w") as report_file,
                            stem.with_suffix(".log").open("w") as log,
                        ):
                            subprocess.run(command, cwd=root, stdout=report_file, stderr=log, check=True)
                        report = json.loads(Path(str(stem) + ".json").read_text())
                        row = measurement(report, args.queries, run, args.profile)
                        if writer is None:
                            writer = csv.DictWriter(stream, fieldnames=row.keys())
                            writer.writeheader()
                        writer.writerow(row)
                        stream.flush()
    command = ["uv", "run", str(root / "perf/fts/plot/plot-fts.py"), "--sweep", *csv_paths]
    for extension in ("png", "pdf", "svg"):
        command += ["-o", str(output / f"fts-peak-heap.{extension}")]
    subprocess.run(command, cwd=root, check=True)


def write_environment(args, root, output):
    metadata = dict(
        arguments=vars(args) | {"output": str(output)},
        machine=platform.platform(),
        cpus=os.cpu_count(),
        allocator="dhat",
        scope="query-phase Rust allocations",
    )
    for name, command in (
        ("rustc", ["rustc", "-Vv"]),
        ("revision", ["git", "rev-parse", "HEAD"]),
        ("worktree", ["git", "status", "--short"]),
    ):
        metadata[name] = subprocess.check_output(command, cwd=root, text=True).strip()
    (output / "environment.json").write_text(json.dumps(metadata, indent=2) + "\n")


def measurement(report, queries_per_connection, run, profile):
    if report["queries"] != queries_per_connection * report["connections"] or report["transactions"] != 0:
        raise ValueError("memory run did not complete the requested queries")
    documents = report["documents"]
    either = (documents + 1) // 2 + (documents + 2) // 3 - (documents + 5) // 6
    expected = {
        "rare": (documents + 99) // 100,
        "common": documents,
        "and": (documents + 5) // 6,
        "or": either,
        "phrase": (documents + 199) // 200,
        "ranked": min(10, either),
    }
    query = report["query"]
    if report["rows_per_query"] != expected[query]:
        raise ValueError("unexpected query result count")
    peak = report["peak_live_query_bytes"]
    if not 0 <= report["retained_query_bytes"] <= peak <= report["total_allocated_bytes"]:
        raise ValueError("inconsistent heap measurements")
    return dict(
        benchmark="memory",
        engine="turso",
        mode=report["mode"],
        state=report["state"],
        documents=documents,
        connections=report["connections"],
        queries=report["queries"],
        run=run,
        query=query,
        requested_queries=queries_per_connection,
        min_seconds=0,
        debug_assertions=str(profile == "dev").lower(),
        peak_heap_bytes=peak,
        retained_heap_bytes=report["retained_query_bytes"],
        total_allocated_bytes=report["total_allocated_bytes"],
        total_allocations=report["total_allocations"],
    )


if __name__ == "__main__":
    main()
