#!/usr/bin/env python3
# Usage e.g.: uv run scripts/corruption_bisecter.py -i corruption.sql -o bisected.sql
# To clean up input data for this script, consider using `scripts/clean_interactions.sh`
import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Sequence


def read_statements(input_path: Path) -> List[str]:
    with input_path.open("r", encoding="utf-8", errors="replace") as f:
        lines = [line.rstrip("\n") for line in f]
    return [line for line in lines if line.strip()]


# Run a set of SQL statements using tursodb and then run integrity_check on the given db file using sqlite3.
# Return whether the integrity check passed or failed.
def run_sql_and_do_integrity_check(
    workspace_root: Path,
    db_path: Path,
    statements: Sequence[str],
) -> bool:
    # Apply statements (if any) and then run integrity_check on the given db file
    if statements:
        sql_input = "\n".join(statements) + "\n"
        run_cmd = ["cargo", "run", "--quiet", "--", str(db_path)]
        run_proc = subprocess.run(
            run_cmd,
            input=sql_input,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(workspace_root),
            check=False,
        )
        if run_proc.returncode != 0:
            raise RuntimeError(f"cargo run failed (code {run_proc.returncode})")

    sqlite_cmd = [
        "sqlite3",
        str(db_path),
        "pragma integrity_check;",
    ]
    sqlite_proc = subprocess.run(
        sqlite_cmd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(workspace_root),
        check=False,
    )

    output = (sqlite_proc.stdout or "").strip()
    if sqlite_proc.returncode != 0:
        raise RuntimeError(f"sqlite3 returned code {sqlite_proc.returncode} with output: {output}")

    return output.lower() == "ok"


# Find a minimal subset of SQL statements that still fails integrity check.
# This is done by binary searching for the minimal prefix.
# We don't care about scenarios where some prefix P fails and then a larger prefix P' does not fail anymore;
# We just want to find the minimal prefix that fails in some manner.
def find_min_failing_prefix( # noqa: C901
    workspace_root: Path,
    statements: Sequence[str],
) -> List[str]:
    # For performance reasons, reuse DB across attempts: keep last passing DB snapshot and apply only deltas.
    with tempfile.TemporaryDirectory(prefix="limbo-bisect-") as tmpdir:
        tmpdir_path = Path(tmpdir)
        db_pass = tmpdir_path / "pass.db"
        db_work = tmpdir_path / "work.db"

        def delete_db(base: Path) -> None:
            for suffix in ("", "-wal", "-shm"):
                p = Path(str(base) + suffix)
                if p.exists():
                    try:
                        p.unlink()
                    except FileNotFoundError:
                        pass

        def copy_db(src: Path, dst: Path) -> None:
            delete_db(dst)
            for suffix in ("", "-wal", "-shm"):
                s = Path(str(src) + suffix)
                d = Path(str(dst) + suffix)
                if s.exists():
                    d.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(s, d)

        last_pass_len = 0

        def check_prefix(k: int) -> bool:
            nonlocal last_pass_len
            # Prepare working DB starting from last passing snapshot when possible
            if last_pass_len > 0 and k > last_pass_len and db_pass.exists():
                copy_db(db_pass, db_work)
                delta = statements[last_pass_len:k]
                integrity_check_ok = run_sql_and_do_integrity_check(workspace_root, db_work, delta)
            else:
                delete_db(db_work)
                initial = statements[:k]
                integrity_check_ok = run_sql_and_do_integrity_check(workspace_root, db_work, initial)

            sys.stderr.write(f"Test prefix {k} -> {integrity_check_ok}\n")
            if integrity_check_ok:
                copy_db(db_work, db_pass)
                last_pass_len = k
            return not integrity_check_ok

        # Binary search minimal k such that prefix of length k FAILS.
        low = 1
        high = len(statements)
        answer_k = None

        # Initialize with empty DB as passing baseline
        delete_db(db_pass)
        delete_db(db_work)

        while low <= high:
            mid = (low + high) // 2
            failed = check_prefix(mid)
            if failed:
                answer_k = mid
                high = mid - 1
            else:
                low = mid + 1

        if answer_k is None:
            raise RuntimeError("Could not find a failing prefix despite full set failing.")

        return list(statements[:answer_k])

def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Find a minimal subset of SQL statements that still FAILS pragma integrity_check."
        )
    )
    parser.add_argument(
        "-i",
        "--input",
        type=Path,
        help="Path to input SQL file (one statement per line)",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("bisected.sql"),
        help="Path to write the minimized failing prefix (default: bisected.sql)",
    )

    args = parser.parse_args(argv)

    # Assume the script is placed under <repo>/scripts/ and use repo root as workspace
    workspace_root = Path(__file__).resolve().parent.parent

    if not args.input.exists():
        raise RuntimeError(f"Input file not found: {args.input}")

    statements = read_statements(args.input)
    if not statements:
        raise RuntimeError("Input file has no statements after filtering empty lines.")

    # Confirm the full input fails integrity check on a fresh DB
    with tempfile.TemporaryDirectory(prefix="limbo-bisect-precheck-") as pretmp:
        pre_db = Path(pretmp) / "check.db"
        integrity_check_ok = run_sql_and_do_integrity_check(workspace_root, pre_db, statements)
        if integrity_check_ok:
            raise RuntimeError("Full input did not FAIL integrity check")

    result_lines = find_min_failing_prefix(workspace_root, statements)
    summary = (
        f"Reduced failing subset to {len(result_lines)} of {len(statements)} statements.\n"
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as f:
        for line in result_lines:
            f.write(line + "\n")
    sys.stderr.write(f"Wrote minimized failing prefix to {args.output}\n")

    sys.stderr.write(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))


