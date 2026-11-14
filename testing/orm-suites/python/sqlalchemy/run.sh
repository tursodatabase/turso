#!/usr/bin/env bash
# Runs SQLAlchemy's own test suite twice: on the stdlib sqlite3 driver
# (sqlite://) and on the Turso dialect shipped with pyturso (sqlite+turso://).
# Extra arguments are passed to pytest, e.g. `run.sh test/orm/test_query.py`.
source "$(dirname "$0")/../lib.sh"

SQLALCHEMY_TAG=rel_2_0_45
fetch sqlalchemy https://github.com/sqlalchemy/sqlalchemy.git "$SQLALCHEMY_TAG"
setup_venv -e "$CACHE/sqlalchemy" pytest pytest-xdist
export PYTHONPATH="$PY_DIR/sqlalchemy:$PYTHONPATH"
out="$RESULTS_ROOT/python-sqlalchemy"
work="$CACHE/sqlalchemy-work"
mkdir -p "$out" "$work"

for driver in sqlite turso; do
    case "$driver" in
        sqlite) url="sqlite:///$work/sqlite.db" ;;
        # SQLAlchemy's SQLite test setup attaches a second database on every
        # connection, and ATTACH is experimental in Turso.
        turso) url="sqlite+turso:///$work/turso.db?experimental_features=attach" ;;
    esac
    echo "== sqlalchemy $SQLALCHEMY_TAG on $driver"
    rm -f "$work/$driver"*.db*
    # --maxfail=0: SQLAlchemy's setup.cfg stops the run after 250 failures.
    (cd "$CACHE/sqlalchemy" && python -m pytest -q -p no:cacheprovider -p turso_sa_plugin \
        -n "${JOBS:-8}" --maxfail=0 --dburi "$url" --junitxml="$out/$driver.xml" "${@:-test}" \
        >"$out/$driver.log" 2>&1) || true
    tail -n 1 "$out/$driver.log"
done
