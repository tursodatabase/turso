#!/usr/bin/env bash
# Runs Drizzle's better-sqlite3 integration tests on better-sqlite3 and on Turso.
source "$(dirname "$0")/../lib.sh"
use_node 22
build_turso

SRC="$(fetch_repo drizzle https://github.com/drizzle-team/drizzle-orm.git 0.45.4)"
OUT="$(results_dir drizzle)"
PNPM="npx -y pnpm@10.6.3"
if [ ! -d "$SRC/drizzle-orm/dist" ]; then
  (cd "$SRC" && $PNPM install --frozen-lockfile)
  (cd "$SRC/drizzle-orm" && $PNPM run build)
fi

cd "$SRC/integration-tests"
for driver in sqlite turso; do
  use_driver "$driver" node_modules ../drizzle-orm/node_modules
  # forks: an engine panic kills one test file's process, not the whole run.
  npx vitest run --pool=forks --reporter=junit --outputFile="$OUT/$driver.xml" \
    tests/sqlite/better-sqlite.test.ts tests/relational/bettersqlite.test.ts \
    js-tests/driver-init/module/better-sqlite3.test.mjs \
    js-tests/driver-init/commonjs/better-sqlite3.test.cjs || true
done
use_driver sqlite node_modules ../drizzle-orm/node_modules
