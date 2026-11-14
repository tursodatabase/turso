"""Compare one ORM test suite's results on SQLite and on Turso.

Usage: score.py <sqlite.xml> <turso.xml> [--known-failures FILE [--update]]

Score = tests passing on both / tests passing on SQLite. Tests that pass on
SQLite but not on Turso are grouped by their normalized error message so each
group can be triaged as one bug.

With --known-failures, FILE lists the tests that pass on SQLite and fail on
Turso in CI. The exit status is 1 if a test fails that FILE does not list (a
regression) or if FILE lists a test that now passes on both (it must be
removed, so a later regression of it is caught). --update rewrites FILE with
today's failures.
"""

import argparse
import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("sqlite_xml")
    parser.add_argument("turso_xml")
    parser.add_argument("--known-failures", help="tests that pass on SQLite and are known to fail on Turso")
    parser.add_argument("--update", action="store_true", help="rewrite --known-failures with today's failures")
    parser.add_argument("--top", type=int, default=30, help="number of failure groups to print")
    args = parser.parse_args()

    sqlite, turso = load(args.sqlite_xml), load(args.turso_xml)
    # A test the suite skips on Turso (e.g. it needs a newer SQLite version than
    # Turso reports) doesn't count either way.
    skipped_on_turso = {name for name, (status, _) in turso.items() if status == "skip"}
    passed_on_sqlite = {name for name, (status, _) in sqlite.items() if status == "pass"}
    sqlite_pass = passed_on_sqlite - skipped_on_turso
    failing = {name for name in sqlite_pass if turso.get(name, ("missing", ""))[0] != "pass"}
    score = f"{len(sqlite_pass) - len(failing)}/{len(sqlite_pass)} = {percent(sqlite_pass, failing):.1f}%"

    print(f"sqlite: {len(sqlite)} tests, {len(passed_on_sqlite)} pass")
    print(f"turso:  {len(turso)} tests, {sum(status == 'pass' for status, _ in turso.values())} pass")
    print(f"passed on sqlite, skipped on turso (not counted): {len(passed_on_sqlite & skipped_on_turso)}")
    print(f"score:  {score}")
    print_failure_groups(failing, turso, args.top)

    if not args.known_failures:
        return 0
    known = set()
    if os.path.exists(args.known_failures):
        with open(args.known_failures) as f:
            known = {line.rstrip("\n") for line in f if line.strip()}
    if args.update:
        # Keep entries for tests SQLite doesn't pass here; CI may still need them.
        updated = failing | (known - sqlite_pass)
        with open(args.known_failures, "w") as f:
            f.writelines(f"{name}\n" for name in sorted(updated))
        print(f"\nwrote {len(updated)} known failures to {args.known_failures}")
        return 0

    regressions = sorted(failing - known)
    # A known failure that SQLite doesn't pass here (skipped on this platform
    # or SQLite version) is not judged either way.
    fixed = sorted((known - failing) & sqlite_pass)
    print(f"\nknown failures: {len(known)}; new failures: {len(regressions)}; fixed: {len(fixed)}")
    for name in regressions:
        print(f"  NEW FAILURE {name}: {turso.get(name, ('missing', '<missing in turso run>'))[1]}")
    for name in fixed:
        print(f"  NO LONGER FAILS {name}")
    if fixed:
        # The path is testing/orm-suites/<ecosystem>/<suite>/known-failures.txt.
        suite = "/".join(os.path.abspath(args.known_failures).split(os.sep)[-3:-1])
        print(
            f"\n{len(fixed)} known failures now pass on Turso."
            f"\nRemove them from {args.known_failures}, or run: testing/orm-suites/run.sh {suite} --update"
        )
    write_github_summary(args.turso_xml, score, len(regressions), len(fixed))
    return 1 if regressions or fixed else 0


def load(path):
    results = {}
    for case in ET.parse(path).iter("testcase"):
        name = f"{case.get('classname')}::{case.get('name')}"
        # Some runners put the driver and its version in the test name,
        # e.g. SQLAlchemy's "CompileTest_sqlite+pysqlite_3_47_1".
        name = re.sub(r"sqlite\+\w+?_\d+_\d+_\d+", "sqlite", name)
        name = " ".join(name.split())
        status, message = "pass", ""
        for child in case:
            if child.tag in ("failure", "error"):
                status = "fail"
                message = child.get("message") or first_line(child.text)
            elif child.tag == "skipped":
                status = "skip"
        results[name] = (status, message)
    return results


def first_line(text):
    lines = (text or "").strip().splitlines()
    return lines[-1] if lines else ""


def percent(passing, failing):
    return 100 * (len(passing) - len(failing)) / max(1, len(passing))


def print_failure_groups(failing, turso, top):
    groups = defaultdict(list)
    for name in sorted(failing):
        status, message = turso.get(name, ("missing", "<missing in turso run>"))
        groups[f"[{status}] {normalize(message)}"].append(name)
    print(f"\nfailure groups ({len(groups)}; count, message, example):")
    for key, names in sorted(groups.items(), key=lambda kv: -len(kv[1]))[:top]:
        print(f"{len(names):5d}  {key}\n       e.g. {names[0]}")


def normalize(message):
    message = message.splitlines()[0] if message else "<no message>"
    message = re.sub(r"0x[0-9a-f]+", "0x?", message)
    message = re.sub(r"'[^']*'", "'?'", message)
    message = re.sub(r"\d+", "N", message)
    return message[:160]


def write_github_summary(turso_xml, score, regressions, fixed):
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary:
        return
    suite = os.path.basename(os.path.dirname(os.path.abspath(turso_xml)))
    with open(summary, "a") as f:
        f.write(f"| {suite} | {score} | {regressions} | {fixed} |\n")


if __name__ == "__main__":
    sys.exit(main())
