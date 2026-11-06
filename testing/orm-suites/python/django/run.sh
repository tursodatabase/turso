#!/usr/bin/env bash
# Runs Django's own test suite against the sqlite backend twice: with the
# stdlib sqlite3 module and with turso. Extra arguments are test apps to run
# instead of all of them, e.g. `run.sh basic queries`.
#
# Each test app runs in its own process (JOBS at a time). Django creates the
# tables of every installed app before running any test, so one CREATE TABLE
# that turso rejects would otherwise fail the whole suite.
source "$(dirname "$0")/../lib.sh"

DJANGO_TAG=6.1.2
fetch django https://github.com/django/django.git "$DJANGO_TAG"
setup_venv -e "$CACHE/django" unittest-xml-reporting
export PYTHONPATH="$PY_DIR/django:$PYTHONPATH"
export ORM_RESULTS_DIR="$RESULTS_ROOT/python-django"
TESTS="$CACHE/django/tests"
WORK="$CACHE/django-work"

if [ $# -gt 0 ]; then
    apps=("$@")
else
    apps=($(cd "$TESTS" && python -c 'import runtests; print(*sorted(runtests.get_test_modules(False)))'))
fi

# run_app <driver> <app>: runs one test app, writing $WORK/<driver>/<app>.{xml,log}.
run_app() {
    local dir="$WORK/$1/$2"
    mkdir -p "$dir"
    (cd "$TESTS" && ORM_DRIVER=$1 ORM_DB_DIR="$dir" ORM_RESULTS_DIR="$WORK/$1" ORM_XML_NAME="$2.xml" \
        python runtests.py --settings=orm_settings --noinput --parallel=1 "$2" >"$WORK/$1/$2.log" 2>&1) || true
    rm -rf "$dir"
}
export -f run_app
export TESTS WORK

mkdir -p "$ORM_RESULTS_DIR"
for driver in sqlite turso; do
    echo "== django $DJANGO_TAG on $driver (${#apps[@]} test apps)"
    rm -rf "$WORK/$driver" && mkdir -p "$WORK/$driver"
    printf '%s\n' "${apps[@]}" | xargs -P "${JOBS:-8}" -I{} bash -c "run_app $driver {}"
    python "$PY_DIR/django/merge_junit.py" "$ORM_RESULTS_DIR/$driver.xml" "$WORK/$driver"/*.xml
    # Apps that wrote no test results, with the last line of their output.
    for app in "${apps[@]}"; do
        [ -s "$WORK/$driver/$app.xml" ] || echo "$app: $(tail -n 1 "$WORK/$driver/$app.log")"
    done >"$ORM_RESULTS_DIR/$driver-setup-errors.txt"
    echo "apps with no results: $(wc -l <"$ORM_RESULTS_DIR/$driver-setup-errors.txt")"
done
