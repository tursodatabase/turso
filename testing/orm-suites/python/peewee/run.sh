#!/usr/bin/env bash
# Runs Peewee's own test suite against SQLite twice: with the stdlib sqlite3
# module and with turso. Extra arguments name test modules, e.g. `run.sh models`.
source "$(dirname "$0")/../lib.sh"

PEEWEE_TAG=4.5.3
fetch peewee https://github.com/coleifer/peewee.git "$PEEWEE_TAG"
setup_venv unittest-xml-reporting
export PYTHONPATH="$CACHE/peewee:$PYTHONPATH" PEEWEE_TEST_BACKEND=sqlite
out="$RESULTS_ROOT/python-peewee"
mkdir -p "$out"

for driver in sqlite turso; do
    echo "== peewee $PEEWEE_TAG on $driver"
    (cd "$CACHE/peewee" && rm -f peewee_test.db* && \
        python "$PY_DIR/peewee/run_tests.py" "$driver" "$out/$driver.xml" "$@" >"$out/$driver.log" 2>&1) || true
    tail -n 3 "$out/$driver.log"
done
