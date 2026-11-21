#!/usr/bin/env bash
# Runs Knex's integration tests for the better-sqlite3 client on better-sqlite3 and on Turso.
source "$(dirname "$0")/../lib.sh"
use_node 26
build_turso

SRC="$(fetch_repo knex https://github.com/knex/knex.git 3.3.0)"
OUT="$(results_dir knex)"
cd "$SRC"
if [ ! -d node_modules ]; then
  # Knex pins better-sqlite3 ^10, which does not compile on Node 24+; 13.0.3
  # ships prebuilt binaries. --ignore-scripts skips building the other native
  # drivers (sqlite3, pg-native, oracledb) that these tests do not use.
  npm install --ignore-scripts --no-audit --no-fund
  npm install --ignore-scripts --no-audit --no-fund --no-save better-sqlite3@13.0.3
fi

for driver in sqlite turso; do
  use_driver "$driver" node_modules
  rm -f test/test.sqlite3*
  DB=better-sqlite3 mocha_per_file "$OUT/$driver.xml" \
    test/integration-test-suite.js $(find test/integration2 -name '*.spec.js' | sort) \
    -- --exit -t 10000 --file test/prep-db-mocha.js
done
use_driver sqlite node_modules
