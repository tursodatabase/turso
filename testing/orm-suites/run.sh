#!/usr/bin/env bash
# run.sh <ecosystem>/<suite> [--update]
#
# Runs one ORM's own test suite on SQLite and on Turso, then checks the Turso
# run against the suite's known-failures.txt. Exits 1 if a test that passes on
# SQLite fails on Turso and is not listed, or if a listed test no longer fails.
# --update rewrites known-failures.txt with today's failures instead.
#
# Example: testing/orm-suites/run.sh go/gorm
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
suite="${1:?usage: run.sh <ecosystem>/<suite> [--update]}"
shift
[ -x "$ROOT/$suite/run.sh" ] || { echo "no such suite: $suite" >&2; exit 2; }

"$ROOT/$suite/run.sh"
results="$ROOT/results/${suite/\//-}"
python3 "$ROOT/score.py" "$results/sqlite.xml" "$results/turso.xml" \
    --known-failures "$ROOT/$suite/known-failures.txt" "$@"
