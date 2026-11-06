#!/usr/bin/env bash
# Runs Kysely's own node test suite (sqlite dialect only) on better-sqlite3 and on Turso.
source "$(dirname "$0")/../lib.sh"
use_node 26
build_turso

SRC="$(fetch_repo kysely https://github.com/kysely-org/kysely.git v0.29.8)"
OUT="$(results_dir kysely)"
PNPM="npx -y pnpm@12.11.2"
cd "$SRC"
if [ ! -d test/node/dist ]; then
  $PNPM install --frozen-lockfile
  $PNPM run build
  npx tsc -p test/node
fi

for driver in sqlite turso; do
  use_driver "$driver" node_modules
  DIALECTS=sqlite mocha_per_file "$OUT/$driver.xml" test/node/dist/*.test.js -- --timeout 15000
done
use_driver sqlite node_modules
