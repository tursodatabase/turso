#!/usr/bin/env python3

# vfs vs sqlite3 benchmarking/comparison
import argparse
import os
import platform
import statistics
import subprocess
from pathlib import Path
from time import perf_counter, sleep
from typing import List

from cli_tests.console import error, info, test
from cli_tests.test_turso_cli import TestTursoShell
from faker import Faker

# for now, use debug for the debug assertions
LIMBO_BIN = Path("./target/release/tursodb")
DB_FILE = Path("testing/system/temp.db")

SQLITE_BIN = "sqlite3"

vfs_list = ["syscall", "sqlite"]
if platform.system() == "Linux":
    vfs_list.append("io_uring")

if platform.system() == "Windows":
    vfs_list.append("win_iocp")


def append_time(i, times, start, perf_counter):
    if i > 0:
        times.append(perf_counter() - start)
    return True


fake = Faker()


def bench_one(vfs: str, sql: str, iterations: int, assorted: bool, use_sqlite3=False) -> List[float]:
    """
    Launch a single process (Tursodb with the requested VFS or sqlite3), run `sql`
    `iterations` times, return a list of elapsed wall‑clock times.
    """
    if use_sqlite3:
        shell = TestTursoShell(
            exec_name=SQLITE_BIN,
            flags=str(DB_FILE),
            init_commands="",
        )
        test_name = "sqlite3"
    else:
        shell = TestTursoShell(
            exec_name=str(LIMBO_BIN),
            flags=f"-m list --vfs {vfs} {DB_FILE}",
            init_commands="",
        )
        test_name = f"limbo ({vfs})"

    times: List[float] = []
    queries = [sql]
    if assorted:
        queries.extend(
            [
                "select * from users;",
                "insert into products (name,price) values (randomblob(1024*64), randomblob(1024*64));",
                "select first_name, last_name, age from users limit 1000;",
                "insert into users (first_name, last_name, email, phone_number, address, city, state, zipcode,age)"
                + f"values ('{fake.first_name()}', '{fake.last_name()}', '{fake.email()}', '{fake.phone_number()}',"
                + f"'{fake.street_address()}', '{fake.city()}', '{fake.state_abbr()}', '{fake.zipcode()}', 62);",
            ]
        )
    for i in range(1, iterations + 1):
        for query in queries:
            start = perf_counter()
            _ = shell.run_test_fn(query, lambda x: x is not None and append_time(i, times, start, perf_counter))
            test(f"  {test_name} | run {i:>3}: {times[-1]:.6f}s")

    shell.quit()
    return times


def setup_temp_db() -> None:
    # make sure we start fresh, otherwise we could end up with
    # one having to checkpoint the others from the previous run
    cleanup_temp_db()
    cmd = ["sqlite3", "testing/system/testing.db", ".clone testing/system/temp.db"]
    proc = subprocess.run(cmd, check=True)
    proc.check_returncode()
    sleep(0.3)  # make sure it's finished


def cleanup_temp_db() -> None:
    if DB_FILE.exists():
        DB_FILE.unlink()
    wal_file = DB_FILE.with_suffix(".db-wal")
    if wal_file.exists():
        os.remove(wal_file)
    shm_file = DB_FILE.with_suffix(".db-shm")
    if shm_file.exists():
        os.remove(shm_file)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark a specific Turso VFS against sqlite3.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Available VFS options: {', '.join(vfs_list)}",
    )
    parser.add_argument("vfs", choices=vfs_list, help="VFS backend to benchmark against sqlite3")
    parser.add_argument("sql", help="SQL statement to execute (quote it)")
    parser.add_argument("iterations", type=int, default=100, help="number of repetitions")
    parser.add_argument("--assorted", action="store_true", help="use additional assorted queries")
    args = parser.parse_args()

    vfs, sql, iterations, assorted = args.vfs, args.sql, args.iterations, args.assorted
    if iterations <= 0:
        error("iterations must be a positive integer")
        parser.error("Invalid Arguments")

    info(f"VFS        : {vfs}")
    info(f"SQL        : {sql}")
    info(f"Iterations : {iterations}")
    info(f"Assorted   : {assorted}")
    info(f"Database   : {DB_FILE.resolve()}")
    info("-" * 60)

    # Benchmark sqlite3
    setup_temp_db()
    test("\n### SQLite3 (baseline) ###")
    sqlite_times = bench_one(vfs, sql, iterations, assorted, use_sqlite3=True)
    if len(sqlite_times) < 1000:
        info("All times (sqlite3):", " ".join(f"{t:.6f}" for t in sqlite_times))
    else:
        info("All times truncated...")

    sqlite_avg = statistics.mean(sqlite_times)
    sqlite_median = statistics.median(sqlite_times)
    sqlite_stdev = statistics.stdev(sqlite_times) if len(sqlite_times) > 1 else 0

    # Benchmark Turso with specified IO backend
    setup_temp_db()
    test(f"\n### Turso with I/O: {vfs} ###")
    limbo_times = bench_one(vfs, sql, iterations, assorted, use_sqlite3=False)
    info(f"All times (limbo {vfs}):", " ".join(f"{t:.6f}" for t in limbo_times))
    limbo_avg = statistics.mean(limbo_times)
    limbo_median = statistics.median(limbo_times)
    limbo_stdev = statistics.stdev(limbo_times) if len(limbo_times) > 1 else 0

    cleanup_temp_db()

    # Results summary
    info("\n" + "=" * 60)
    info("BENCHMARK RESULTS")
    info("=" * 60)
    info("\nSQLite3 (baseline):")
    info(f"  Average : {sqlite_avg:.6f} s")
    info(f"  Median  : {sqlite_median:.6f} s")
    info(f"  Std Dev : {sqlite_stdev:.6f} s")
    if len(sqlite_times) > 0:
        info(f"  Min     : {min(sqlite_times):.6f} s")
        info(f"  Max     : {max(sqlite_times):.6f} s")
    info(f"\nTurso ({vfs}):")
    info(f"  Average : {limbo_avg:.6f} s")
    info(f"  Median  : {limbo_median:.6f} s")
    info(f"  Std Dev : {limbo_stdev:.6f} s")
    if len(limbo_times) > 0:
        info(f"  Min     : {min(limbo_times):.6f} s")
        info(f"  Max     : {max(limbo_times):.6f} s")
    info("\n" + "-" * 60)
    info("COMPARISON")
    info("-" * 60)
    # Performance comparison
    pct_diff = (limbo_avg - sqlite_avg) / sqlite_avg * 100.0
    faster_slower = "slower" if pct_diff > 0 else "faster"
    info(f"Turso ({vfs}) is {abs(pct_diff):.1f}% {faster_slower} than SQLite3")
    info(f"  SQLite3 avg: {sqlite_avg:.6f} s")
    info(f"  Turso avg  : {limbo_avg:.6f} s")
    info(f"  Difference : {limbo_avg - sqlite_avg:+.6f} s")
    # Median comparison
    median_pct_diff = (limbo_median - sqlite_median) / sqlite_median * 100.0
    median_faster_slower = "slower" if median_pct_diff > 0 else "faster"
    info(f"\nMedian comparison: Turso is {abs(median_pct_diff):.1f}% {median_faster_slower}")
    info(f"  SQLite3 median: {sqlite_median:.6f} s")
    info(f"  Turso median  : {limbo_median:.6f} s")
    info("=" * 60)


if __name__ == "__main__":
    main()
