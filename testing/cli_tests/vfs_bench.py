#!/usr/bin/env python3

# vfs benchmarking/comparison
import argparse
import os
import platform
import statistics
import subprocess
from pathlib import Path
from time import perf_counter, sleep
from typing import Dict

from cli_tests.console import error, info, test
from cli_tests.test_turso_cli import TestTursoShell

LIMBO_BIN = Path("./target/release/tursodb")
DB_FILE = Path("testing/temp.db")
vfs_list = ["syscall"]
if platform.system() == "Linux":
    vfs_list.append("io_uring")


def append_time(times, start, perf_counter):
    times.append(perf_counter() - start)
    return True


def bench_one(vfs: str, sql: str, iterations: int) -> list[float]:
    """
    Launch a single Limbo process with the requested VFS, run `sql`
    `iterations` times, return a list of elapsed wall‑clock times.
    """
    shell = TestTursoShell(
        exec_name=str(LIMBO_BIN),
        flags=f"-m list --vfs {vfs} {DB_FILE}",
        init_commands="",
    )

    times: list[float] = []

    for i in range(1, iterations + 1):
        start = perf_counter()
        _ = shell.run_test_fn(sql, lambda x: x is not None and append_time(times, start, perf_counter))
        test(f"  {vfs} | run {i:>3}: {times[-1]:.6f}s")

    shell.quit()
    return times


def setup_temp_db() -> None:
    cmd = ["sqlite3", "testing/testing.db", ".clone testing/temp.db"]
    proc = subprocess.run(cmd, check=True)
    proc.check_returncode()
    sleep(0.3)  # make sure it's finished


def cleanup_temp_db() -> None:
    if DB_FILE.exists():
        DB_FILE.unlink()
        os.remove("testing/temp.db-wal")


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark a SQL statement against all Limbo VFS back‑ends.")
    parser.add_argument("sql", help="SQL statement to execute (quote it)")
    parser.add_argument("iterations", type=int, help="number of repetitions")
    args = parser.parse_args()
    setup_temp_db()

    sql, iterations = args.sql, args.iterations
    if iterations <= 0:
        error("iterations must be a positive integer")
        parser.error("Invalid Arguments")

    info(f"SQL        : {sql}")
    info(f"Iterations : {iterations}")
    info(f"Database   : {DB_FILE.resolve()}")
    info("-" * 60)
    averages: Dict[str, float] = {}

    for vfs in vfs_list:
        setup_temp_db()
        test(f"\n### VFS: {vfs} ###")
        times = bench_one(vfs, sql, iterations)
        info(f"All times ({vfs}):", " ".join(f"{t:.6f}" for t in times))
        avg = statistics.mean(times)
        averages[vfs] = avg
        cleanup_temp_db()

    info("\n" + "-" * 60)
    info("Average runtime per VFS")
    info("-" * 60)

    for vfs in vfs_list:
        info(f"vfs: {vfs} : {averages[vfs]:.6f} s")
    info("-" * 60)

    baseline = "syscall"
    baseline_avg = averages[baseline]

    name_pad = max(len(v) for v in vfs_list)
    for vfs in vfs_list:
        avg = averages[vfs]
        if vfs == baseline:
            info(f"{vfs:<{name_pad}} : {avg:.6f}  (baseline)")
        else:
            pct = (avg - baseline_avg) / baseline_avg * 100.0
            faster_slower = "slower" if pct > 0 else "faster"
            info(f"{vfs:<{name_pad}} : {avg:.6f}  ({abs(pct):.1f}% {faster_slower} than {baseline})")
        info("-" * 60)


if __name__ == "__main__":
    main()
