#!/usr/bin/env bash
# Runs TypeORM's test suite with only the better-sqlite3 connection enabled,
# on better-sqlite3 and on Turso.
source "$(dirname "$0")/../lib.sh"
use_node 26
build_turso

SRC="$(fetch_repo typeorm https://github.com/typeorm/typeorm.git 1.1.2)"
OUT="$(results_dir typeorm)"
PKG="$SRC/packages/typeorm"
if [ ! -d "$PKG/build/compiled" ]; then
  # npx refuses to start pnpm inside the repo (devEngines check), so use -C.
  (cd "$CACHE" && npx -y pnpm@10.34.5 -C typeorm install --frozen-lockfile)
  (cd "$PKG" && node_modules/.bin/tsc)
fi
cd "$PKG"
echo '[{"skip": false, "type": "better-sqlite3", "database": "temp/better-sqlite3.db", "logging": false}]' \
  > ormconfig.json

for driver in sqlite turso; do
  use_driver "$driver" node_modules
  rm -rf temp
  # --no-config: .mocharc.json lists every spec file, which would be added to each batch.
  MOCHA_BATCH=40 mocha_per_file "$OUT/$driver.xml" \
    $(find build/compiled/test -name '*.test.js' | sort) \
    -- --no-config --file build/compiled/test/utils/test-setup.js --exit --timeout 90000
done
use_driver sqlite node_modules
