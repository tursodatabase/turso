#!/usr/bin/env bash
# Run bun's internal/dbtest suite against SQLite (modernc.org/sqlite via
# sqliteshim, bun's default) and against Turso.
source "$(dirname "$0")/../common.sh"

build_turso
SRC="$(fetch https://github.com/uptrace/bun v1.2.18 bun)"
DBTEST="$SRC/internal/dbtest"

# The only upstream change: open SQLite through sqliteDriverName(), defined below.
sed -i.orig 's|sql.Open(sqliteshim.DriverName(),|sql.Open(sqliteDriverName(sqliteshim.DriverName()),|' "$DBTEST/db_test.go"
grep -q sqliteDriverName "$DBTEST/db_test.go"
cat >"$DBTEST/zz_sqlite_only_test.go" <<'EOF'
package dbtest_test

import "os"

// BUN_SQLITE_DRIVER overrides the driver name that sqliteshim picked.
func sqliteDriverName(shimName string) string {
	if name := os.Getenv("BUN_SQLITE_DRIVER"); name != "" {
		return name
	}
	return shimName
}

// Only test SQLite: no PostgreSQL, MySQL or SQL Server servers are running.
var _ = func() bool {
	for name := range allDBs {
		if name != sqliteName {
			delete(allDBs, name)
		}
	}
	return true
}()
EOF
add_turso "$DBTEST" "$DBTEST" dbtest_test

BUN_SQLITE_DRIVER= run_tests sqlite bun "$DBTEST" .
BUN_SQLITE_DRIVER=turso run_tests turso bun "$DBTEST" .
