#!/usr/bin/env python3
"""
Local reproducer for the nested-savepoint row-loss assertion that
fires under antithesis stress-composer.

Mirrors the antithesis topology:
  - first_setup.py once
  - N concurrent worker processes
  - each worker loops: pick a parallel_driver_*.py, run it
  - workers weighted toward nested_savepoint (the failing driver)
    and wal_checkpoint (the dominant precursor in the failure log)
  - when any worker exits with status 91 (an always() assertion),
    the runner reports it and stops the rest.

Toggle MVCC with: --mvcc
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2].parent
DRIVERS_DIR = REPO_ROOT / "testing" / "antithesis" / "stress-composer"
STUB_DIR = Path(__file__).resolve().parent / "antithesis_stub"


# Weighted bag of drivers. The hot ones are nested_savepoint
# (the failing assertion) and wal_checkpoint (5/6 precursors).
DRIVER_WEIGHTS: dict[str, int] = {
    "parallel_driver_nested_savepoint.py": 8,
    "parallel_driver_wal_checkpoint.py": 6,
    "parallel_driver_create_index.py": 3,
    "parallel_driver_savepoint.py": 2,
    "parallel_driver_insert.py": 2,
    "parallel_driver_delete.py": 2,
    "parallel_driver_update.py": 1,
}


def build_driver_pool() -> list[Path]:
    pool: list[Path] = []
    for name, weight in DRIVER_WEIGHTS.items():
        path = DRIVERS_DIR / name
        if not path.exists():
            sys.stderr.write(f"missing driver: {path}\n")
            continue
        for _ in range(weight):
            pool.append(path)
    return pool


def spawn_setup(workdir: Path, env: dict[str, str]) -> None:
    setup = DRIVERS_DIR / "first_setup.py"
    res = subprocess.run(
        [sys.executable, str(setup)],
        cwd=workdir,
        env=env,
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        sys.stderr.write("first_setup failed:\n")
        sys.stderr.write(res.stdout)
        sys.stderr.write(res.stderr)
        raise SystemExit(res.returncode)


def patch_drivers_for_mvcc(workdir: Path) -> None:
    """
    Copy the parallel drivers into workdir, rewriting turso.connect()
    to enable experimental_features='mvcc' so we exercise the MVCC path.
    """
    target = workdir / "drivers"
    target.mkdir(exist_ok=True)
    for src in DRIVERS_DIR.glob("*.py"):
        text = src.read_text()
        # Only the parallel drivers + first_setup connect to stress_composer.db;
        # init_state.db is the metadata DB (kept on the default backend).
        text = text.replace(
            'turso.connect("stress_composer.db")',
            'turso.connect("stress_composer.db", experimental_features="mvcc")',
        )
        (target / src.name).write_text(text)


def worker_loop(
    worker_id: int,
    driver_pool: list[Path],
    workdir: Path,
    env: dict[str, str],
    use_patched: bool,
    deadline: float,
    fail_event,
) -> None:
    import random as _r

    rng = _r.Random((os.getpid() << 16) ^ worker_id ^ time.time_ns())
    iters = 0
    drivers_dir = (workdir / "drivers") if use_patched else DRIVERS_DIR
    while time.time() < deadline and not fail_event.is_set():
        driver = rng.choice(driver_pool)
        path = drivers_dir / driver.name
        res = subprocess.run(
            [sys.executable, str(path)],
            cwd=workdir,
            env=env,
            capture_output=True,
            text=True,
        )
        iters += 1
        if res.returncode == 91 or res.returncode == 92:
            sys.stderr.write(
                f"[worker {worker_id}] {path.name} FAILED (rc={res.returncode}) iter={iters}\n"
            )
            sys.stderr.write("--- stdout ---\n")
            sys.stderr.write(res.stdout)
            sys.stderr.write("--- stderr ---\n")
            sys.stderr.write(res.stderr)
            sys.stderr.flush()
            fail_event.set()
            return
        # Other non-zero exits (SIGSEGV, panics) are also interesting.
        if res.returncode not in (0, 91, 92):
            sys.stderr.write(
                f"[worker {worker_id}] {path.name} exit={res.returncode} iter={iters}\n"
            )
            sys.stderr.write(res.stderr[-2000:])
            sys.stderr.flush()
    sys.stderr.write(f"[worker {worker_id}] done, iters={iters}\n")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--duration", type=int, default=120, help="seconds")
    p.add_argument("--mvcc", action="store_true")
    p.add_argument("--keep-workdir", action="store_true")
    args = p.parse_args()

    driver_pool = build_driver_pool()
    if not driver_pool:
        return 2

    workdir = Path(tempfile.mkdtemp(prefix="repro_savepoint_"))
    sys.stderr.write(f"workdir: {workdir}\n")

    env = os.environ.copy()
    # Make the local antithesis stub importable.
    pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{STUB_DIR}{os.pathsep}{DRIVERS_DIR}{os.pathsep}{pp}".rstrip(os.pathsep)
    )

    if args.mvcc:
        patch_drivers_for_mvcc(workdir)

    spawn_setup(workdir, env)

    ctx = mp.get_context("spawn")
    fail_event = ctx.Event()
    deadline = time.time() + args.duration

    procs = []
    for i in range(args.workers):
        prc = ctx.Process(
            target=worker_loop,
            args=(i, driver_pool, workdir, env, args.mvcc, deadline, fail_event),
            daemon=False,
        )
        prc.start()
        procs.append(prc)

    rc = 0
    try:
        for prc in procs:
            prc.join()
        if fail_event.is_set():
            rc = 1
    except KeyboardInterrupt:
        fail_event.set()
        for prc in procs:
            prc.terminate()
        rc = 130

    if not args.keep_workdir:
        shutil.rmtree(workdir, ignore_errors=True)
    else:
        sys.stderr.write(f"workdir kept at: {workdir}\n")

    return rc


if __name__ == "__main__":
    sys.exit(main())
