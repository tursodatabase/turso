#!/usr/bin/env bash
# Builds php-src's unmodified ext/sqlite3 and pdo_sqlite against
# libturso_sqlite3 and runs PHP's own .phpt suites against it — the
# compatibility scoreboard for the PHP driver.
#
# Usage: bindings/php/phpt-harness.sh [suite ...]
#   suites default to: ext/sqlite3/tests ext/pdo_sqlite/tests
# Env:
#   PHP_VERSION   php-src release to test against (default 8.5.2)
#   PHPT_WORKDIR  scratch dir (default ~/.cache/turso-phpt)
set -euo pipefail

PHP_VERSION=${PHP_VERSION:-8.5.2}
REPO=$(cd "$(dirname "$0")/../.." && pwd)
WORK=${PHPT_WORKDIR:-"$HOME/.cache/turso-phpt"}
SRC="$WORK/php-$PHP_VERSION"
LIB="$REPO/target/debug"
INC="$REPO/bindings/c/include"

if [ ! -e "$LIB/libturso_sqlite3.dylib" ] && [ ! -e "$LIB/libturso_sqlite3.so" ]; then
    echo "libturso_sqlite3 not built; run: cargo build -p turso_sqlite3 --features capi" >&2
    exit 1
fi

mkdir -p "$WORK"
if [ ! -d "$SRC" ]; then
    echo "== fetching php-src $PHP_VERSION"
    curl -fsSL "https://www.php.net/distributions/php-$PHP_VERSION.tar.gz" -o "$WORK/php-$PHP_VERSION.tar.gz"
    tar xzf "$WORK/php-$PHP_VERSION.tar.gz" -C "$WORK"
fi

cd "$SRC"
if [ ! -f Makefile ]; then
    # -O1: the Apple clang optimizer ICEs on some large php-src files at
    # -O2 (zend_execute.c, the opcache JIT helpers); the JIT is disabled
    # for the same reason and is not needed for conformance runs.
    echo "== configuring php (minimal cli + sqlite3 + pdo_sqlite, against turso)"
    CFLAGS="-O1 -g" \
    SQLITE_CFLAGS="-I$INC" SQLITE_LIBS="-L$LIB -lturso_sqlite3" \
    PDO_SQLITE_CFLAGS="-I$INC" PDO_SQLITE_LIBS="-L$LIB -lturso_sqlite3" \
    ./configure --disable-all --disable-opcache-jit --enable-cli --with-sqlite3 --enable-pdo --with-pdo-sqlite \
        > "$WORK/configure.log" 2>&1 || { tail -30 "$WORK/configure.log"; exit 1; }
fi

echo "== building php cli"
make -j8 sapi/cli/php > "$WORK/make.log" 2>&1 || { tail -40 "$WORK/make.log"; exit 1; }

export DYLD_LIBRARY_PATH="$LIB" LD_LIBRARY_PATH="$LIB"
echo "== engine probe (must be the turso build, 3.42.0)"
sapi/cli/php -r 'var_dump(SQLite3::version());'

SUITES=("${@:-ext/sqlite3/tests}")
[ $# -eq 0 ] && SUITES=(ext/sqlite3/tests ext/pdo_sqlite/tests)
echo "== running: ${SUITES[*]}"
sapi/cli/php run-tests.php -q -P --show-diff "${SUITES[@]}" 2>&1 | tee "$WORK/results.log" | tail -40
echo "== full results: $WORK/results.log"
